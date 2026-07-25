import { describe, expect, test } from "bun:test";
import {
  applyMemberEdge,
  createGroupStateTracker,
  createInitialGroupTrackerState,
} from "./group-state-tracker";

describe("createInitialGroupTrackerState", () => {
  test("starts Empty with no members", () => {
    const state = createInitialGroupTrackerState("g1");

    expect(state).toEqual({ groupId: "g1", members: {}, state: "Empty" });
  });
});

describe("applyMemberEdge", () => {
  test("a first member joining transitions Empty -> PreparingRebalance", () => {
    const state = createInitialGroupTrackerState("g1");

    const result = applyMemberEdge(state, {
      type: "joining",
      clientId: "consumer-a",
      memberId: null,
    });

    expect(result.transition).toEqual({
      from: "Empty",
      to: "PreparingRebalance",
      generationId: undefined,
      edge: "joining",
    });
  });

  test("a member syncing after joining transitions PreparingRebalance -> CompletingRebalance", () => {
    let state = createInitialGroupTrackerState("g1");
    state = applyMemberEdge(state, {
      type: "joining",
      clientId: "consumer-a",
      memberId: null,
    }).state;

    const result = applyMemberEdge(state, { type: "syncing", clientId: "consumer-a" });

    expect(result.transition).toEqual({
      from: "PreparingRebalance",
      to: "CompletingRebalance",
      generationId: undefined,
      edge: "syncing",
    });
  });

  test("a member joined after syncing transitions CompletingRebalance -> Stable, carrying generationId", () => {
    let state = createInitialGroupTrackerState("g1");
    state = applyMemberEdge(state, {
      type: "joining",
      clientId: "consumer-a",
      memberId: null,
    }).state;
    state = applyMemberEdge(state, { type: "syncing", clientId: "consumer-a" }).state;

    const result = applyMemberEdge(state, {
      type: "joined",
      clientId: "consumer-a",
      memberId: "consumer-a-1",
      generationId: 1,
      isLeader: true,
    });

    expect(result.transition).toEqual({
      from: "CompletingRebalance",
      to: "Stable",
      generationId: 1,
      edge: "joined",
    });
  });

  test("member leaving the only-member group transitions Stable -> Empty", () => {
    let state = createInitialGroupTrackerState("g1");
    state = applyMemberEdge(state, {
      type: "joining",
      clientId: "consumer-a",
      memberId: null,
    }).state;
    state = applyMemberEdge(state, { type: "syncing", clientId: "consumer-a" }).state;
    state = applyMemberEdge(state, {
      type: "joined",
      clientId: "consumer-a",
      memberId: "consumer-a-1",
      generationId: 1,
      isLeader: true,
    }).state;

    const result = applyMemberEdge(state, {
      type: "left",
      clientId: "consumer-a",
      memberId: "consumer-a-1",
    });

    expect(result.transition).toEqual({
      from: "Stable",
      to: "Empty",
      generationId: undefined,
      edge: "left",
    });
  });

  test("no transition is emitted when the derived group state does not change", () => {
    let state = createInitialGroupTrackerState("g1");
    state = applyMemberEdge(state, {
      type: "joining",
      clientId: "consumer-a",
      memberId: null,
    }).state;

    // A second member also starts joining while the group is already PreparingRebalance.
    const result = applyMemberEdge(state, {
      type: "joining",
      clientId: "consumer-b",
      memberId: null,
    });

    expect(result.transition).toBeUndefined();
  });

  test("an existing stable member's rebalancing edge (heartbeat error) reopens Stable -> PreparingRebalance", () => {
    let state = createInitialGroupTrackerState("g1");
    state = applyMemberEdge(state, {
      type: "joining",
      clientId: "consumer-a",
      memberId: null,
    }).state;
    state = applyMemberEdge(state, { type: "syncing", clientId: "consumer-a" }).state;
    state = applyMemberEdge(state, {
      type: "joined",
      clientId: "consumer-a",
      memberId: "consumer-a-1",
      generationId: 1,
      isLeader: true,
    }).state;

    const result = applyMemberEdge(state, { type: "rebalancing", clientId: "consumer-a" });

    expect(result.transition).toEqual({
      from: "Stable",
      to: "PreparingRebalance",
      generationId: undefined,
      edge: "rebalancing",
    });
  });

  /**
   * Reproduces the exact interleaved sequence observed in the spike
   * (docs/spikes/0001-platformatic-kafka-diagnostics.md): consumer B joins a group
   * where consumer A is already Stable, and edges from A and B arrive interleaved,
   * not in a clean per-member order. The group as a whole must only reach Stable once
   * *both* members have reached the same generationId.
   */
  test("reproduces the spike's interleaved two-member rebalance sequence", () => {
    let state = createInitialGroupTrackerState("spike-group");
    // consumer A is already Stable at generation 4 before this sequence starts.
    state = applyMemberEdge(state, {
      type: "joining",
      clientId: "spike-a",
      memberId: null,
    }).state;
    state = applyMemberEdge(state, { type: "syncing", clientId: "spike-a" }).state;
    state = applyMemberEdge(state, {
      type: "joined",
      clientId: "spike-a",
      memberId: "spike-a-1",
      generationId: 4,
      isLeader: true,
    }).state;
    expect(state.state).toBe("Stable");

    // t=6171 B starts joining (new member) -> Stable -> PreparingRebalance
    let result = applyMemberEdge(state, {
      type: "joining",
      clientId: "spike-b",
      memberId: null,
    });
    expect(result.transition?.to).toBe("PreparingRebalance");
    state = result.state;

    // t=6204 A notices the rebalance via heartbeat error (still preparing overall, no change)
    result = applyMemberEdge(state, { type: "rebalancing", clientId: "spike-a" });
    expect(result.transition).toBeUndefined();
    state = result.state;

    // t=6204 A starts joinGroup again (still preparing overall, no change)
    result = applyMemberEdge(state, {
      type: "joining",
      clientId: "spike-a",
      memberId: "spike-a-1",
    });
    expect(result.transition).toBeUndefined();
    state = result.state;

    // t=6209 A starts syncGroup, but B hasn't reached syncing yet -> still preparing overall
    result = applyMemberEdge(state, { type: "syncing", clientId: "spike-a" });
    expect(result.transition).toBeUndefined();
    state = result.state;

    // t=6212 B starts syncGroup -> now both members are (at least) syncing -> Completing
    result = applyMemberEdge(state, { type: "syncing", clientId: "spike-b" });
    expect(result.transition?.to).toBe("CompletingRebalance");
    state = result.state;

    // t=6219 A reaches Stable at generation 18, but B hasn't yet -> still Completing overall
    result = applyMemberEdge(state, {
      type: "joined",
      clientId: "spike-a",
      memberId: "spike-a-1",
      generationId: 18,
      isLeader: true,
    });
    expect(result.transition).toBeUndefined();
    state = result.state;

    // t=6221 B reaches Stable at generation 18 -> both members stable -> group Stable
    result = applyMemberEdge(state, {
      type: "joined",
      clientId: "spike-b",
      memberId: "spike-b-1",
      generationId: 18,
      isLeader: false,
    });
    expect(result.transition).toEqual({
      from: "CompletingRebalance",
      to: "Stable",
      generationId: 18,
      edge: "joined",
    });
    state = result.state;

    expect(state.state).toBe("Stable");
    expect(Object.keys(state.members).sort()).toEqual(["spike-a", "spike-b"]);
  });

  test("a syncing edge with no preceding joining edge is handled defensively", () => {
    const state = createInitialGroupTrackerState("g1");

    const result = applyMemberEdge(state, { type: "syncing", clientId: "consumer-a" });

    expect(result.transition?.to).toBe("CompletingRebalance");
  });
});

describe("createGroupStateTracker", () => {
  test("invokes onTransition only when the derived group state actually changes", () => {
    const transitions: Array<{ groupId: string; from: string; to: string }> = [];
    const tracker = createGroupStateTracker((groupId, transition) => {
      transitions.push({ groupId, from: transition.from, to: transition.to });
    });

    tracker.applyEdge("g1", { type: "joining", clientId: "consumer-a", memberId: null });
    tracker.applyEdge("g1", { type: "joining", clientId: "consumer-b", memberId: null });
    tracker.applyEdge("g1", { type: "syncing", clientId: "consumer-a" });
    tracker.applyEdge("g1", { type: "syncing", clientId: "consumer-b" });

    expect(transitions).toEqual([
      { groupId: "g1", from: "Empty", to: "PreparingRebalance" },
      { groupId: "g1", from: "PreparingRebalance", to: "CompletingRebalance" },
    ]);
  });

  test("tracks multiple groups independently", () => {
    const transitions: Array<{ groupId: string; to: string }> = [];
    const tracker = createGroupStateTracker((groupId, transition) => {
      transitions.push({ groupId, to: transition.to });
    });

    tracker.applyEdge("g1", { type: "joining", clientId: "consumer-a", memberId: null });
    tracker.applyEdge("g2", { type: "joining", clientId: "consumer-x", memberId: null });

    expect(transitions).toEqual([
      { groupId: "g1", to: "PreparingRebalance" },
      { groupId: "g2", to: "PreparingRebalance" },
    ]);
  });

  test("evicts a group's entry entirely once it returns to Empty (memory cleanup)", () => {
    const tracker = createGroupStateTracker(() => {});

    tracker.applyEdge("g1", { type: "joining", clientId: "consumer-a", memberId: null });
    tracker.applyEdge("g1", { type: "syncing", clientId: "consumer-a" });
    tracker.applyEdge("g1", {
      type: "joined",
      clientId: "consumer-a",
      memberId: "consumer-a-1",
      generationId: 1,
      isLeader: true,
    });
    expect(tracker.trackedGroupIds()).toEqual(["g1"]);

    tracker.applyEdge("g1", { type: "left", clientId: "consumer-a", memberId: "consumer-a-1" });

    expect(tracker.trackedGroupIds()).toEqual([]);
  });

  test("a fresh join after eviction starts cleanly from Empty again (no stale residue)", () => {
    const transitions: Array<{ from: string; to: string }> = [];
    const tracker = createGroupStateTracker((_groupId, transition) => {
      transitions.push({ from: transition.from, to: transition.to });
    });

    tracker.applyEdge("g1", { type: "joining", clientId: "consumer-a", memberId: null });
    tracker.applyEdge("g1", { type: "left", clientId: "consumer-a", memberId: null });
    transitions.length = 0;

    tracker.applyEdge("g1", { type: "joining", clientId: "consumer-b", memberId: null });

    expect(transitions).toEqual([{ from: "Empty", to: "PreparingRebalance" }]);
  });

  test("keeps unrelated groups tracked when one group is evicted", () => {
    const tracker = createGroupStateTracker(() => {});

    tracker.applyEdge("g1", { type: "joining", clientId: "consumer-a", memberId: null });
    tracker.applyEdge("g2", { type: "joining", clientId: "consumer-x", memberId: null });
    tracker.applyEdge("g1", { type: "left", clientId: "consumer-a", memberId: null });

    expect(tracker.trackedGroupIds()).toEqual(["g2"]);
  });
});
