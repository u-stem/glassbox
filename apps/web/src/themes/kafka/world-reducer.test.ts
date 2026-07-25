import { describe, expect, test } from "bun:test";
import type { GlassboxEvent } from "@glassbox/schema";
import { createInitialWorld, reduceWorld } from "./world-reducer";

function makeEvent(ts: number, type: string, payload: unknown): GlassboxEvent {
  return {
    id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    seq: 0,
    ts,
    theme: "kafka",
    source: { kind: "client" },
    type,
    payload,
  };
}

describe("createInitialWorld", () => {
  test("starts with no topics, no groups and no snapshot yet", () => {
    const world = createInitialWorld();

    expect(world).toEqual({
      topics: [],
      groups: {},
      snapshotFetchedAt: 0,
      groupStateFromTransition: {},
    });
  });
});

describe("reduceWorld: admin.snapshot", () => {
  test("sets topics and groups as the authoritative baseline", () => {
    const world = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [{ name: "orders", partitions: [{ index: 0, earliestOffset: 0, endOffset: 5 }] }],
        groups: [
          {
            groupId: "g1",
            state: "Stable",
            members: [{ memberId: "m1", clientId: "consumer-a", assignments: [] }],
            offsets: [{ topic: "orders", partition: 0, committed: 5, lag: 0 }],
          },
        ],
      }),
    );

    expect(world.topics).toEqual([
      { name: "orders", partitions: [{ index: 0, earliestOffset: 0, endOffset: 5 }] },
    ]);
    expect(world.groups.g1?.state).toBe("Stable");
    expect(world.snapshotFetchedAt).toBe(100);
  });

  test("ignores an admin.snapshot older than the current baseline (out-of-order delivery)", () => {
    const afterFirstSnapshot = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", { topics: [], groups: [] }),
    );

    const result = reduceWorld(
      afterFirstSnapshot,
      makeEvent(50, "admin.snapshot", {
        topics: [{ name: "stale-topic", partitions: [] }],
        groups: [],
      }),
    );

    expect(result).toBe(afterFirstSnapshot);
  });
});

describe("reduceWorld: admin.snapshot lag", () => {
  test("carries through an omitted lag as undefined, not 0", () => {
    const world = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [],
        groups: [
          {
            groupId: "g1",
            state: "Stable",
            members: [],
            offsets: [{ topic: "orders", partition: 0, committed: 5 }],
          },
        ],
      }),
    );

    expect(world.groups.g1?.offsets).toEqual([
      { topic: "orders", partition: 0, committed: 5, lag: undefined },
    ]);
  });
});

describe("reduceWorld: group.state.changed overlay", () => {
  test("updates an existing group's state", () => {
    const baseline = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    const result = reduceWorld(
      baseline,
      makeEvent(200, "group.state.changed", {
        groupId: "g1",
        from: "Stable",
        to: "PreparingRebalance",
        edge: "joining",
      }),
    );

    expect(result.groups.g1?.state).toBe("PreparingRebalance");
  });

  test("creates a group entry when the transition arrives before any snapshot has seen it", () => {
    const result = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "group.state.changed", {
        groupId: "g1",
        from: "Empty",
        to: "PreparingRebalance",
        edge: "joining",
      }),
    );

    expect(result.groups.g1?.state).toBe("PreparingRebalance");
  });

  test("ignores a group.state.changed older than the current snapshot baseline (ADR-0003)", () => {
    const baseline = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    const result = reduceWorld(
      baseline,
      makeEvent(50, "group.state.changed", {
        groupId: "g1",
        from: "Stable",
        to: "PreparingRebalance",
        edge: "joining",
      }),
    );

    expect(result).toBe(baseline);
  });

  test("a group.state.changed is not stomped by a later admin.snapshot's own (stale-polled) state, even though the snapshot's ts is newer", () => {
    const baseline = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    const transitioned = reduceWorld(
      baseline,
      makeEvent(200, "group.state.changed", {
        groupId: "g1",
        from: "Stable",
        to: "PreparingRebalance",
        edge: "joining",
      }),
    );

    // The admin-poller's own ts (250) is newer than the baseline (100), so it clears
    // the snapshotFetchedAt authority check -- but its *polled* state ("Stable") is
    // stale relative to the group.state.changed at ts 200, which this snapshot's tick
    // simply hadn't caught up to yet. Within the reconcile grace window (well under
    // STATE_RECONCILE_GRACE_MS after the transition), the transition's `to` still wins.
    const afterStaleSnapshot = reduceWorld(
      transitioned,
      makeEvent(250, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    expect(afterStaleSnapshot.groups.g1?.state).toBe("PreparingRebalance");
  });

  test("a snapshot still seeds the initial state for a group with no live transition on record yet", () => {
    const result = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "CompletingRebalance", members: [], offsets: [] }],
      }),
    );

    expect(result.groups.g1?.state).toBe("CompletingRebalance");
  });

  test("a snapshot reconciles a stuck transition once the grace window has elapsed (self-heals a missed later transition)", () => {
    const baseline = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    const transitioned = reduceWorld(
      baseline,
      makeEvent(200, "group.state.changed", {
        groupId: "g1",
        from: "Stable",
        to: "PreparingRebalance",
        edge: "joining",
      }),
    );

    // No further group.state.changed ever arrives for g1 (e.g. a missed collector
    // event) -- well past the grace window, the broker-polled snapshot state must win
    // again rather than leaving the UI stuck on "PreparingRebalance" forever.
    const reconciled = reduceWorld(
      transitioned,
      makeEvent(200 + 5000, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    expect(reconciled.groups.g1?.state).toBe("Stable");
  });
});

describe("reduceWorld: consumer.group.joined/left overlay", () => {
  test("optimistically adds a member before the next snapshot arrives", () => {
    const baseline = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    const result = reduceWorld(
      baseline,
      makeEvent(150, "consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "m1",
        generationId: 1,
        isLeader: true,
        assignments: [{ topic: "orders", partitions: [0] }],
      }),
    );

    expect(result.groups.g1?.members).toEqual([
      {
        memberId: "m1",
        clientId: "consumer-a",
        assignments: [{ topic: "orders", partitions: [0] }],
        liveness: "alive",
      },
    ]);
  });

  test("a rejoin with a new memberId (classic protocol assigns a fresh one per generation) replaces the old entry for the same clientId", () => {
    const baseline = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    const firstJoin = reduceWorld(
      baseline,
      makeEvent(150, "consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "m1-gen1",
        generationId: 1,
        isLeader: true,
        assignments: [{ topic: "orders", partitions: [0] }],
      }),
    );

    const rejoined = reduceWorld(
      firstJoin,
      makeEvent(200, "consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "m1-gen2",
        generationId: 2,
        isLeader: true,
        assignments: [{ topic: "orders", partitions: [0, 1] }],
      }),
    );

    expect(rejoined.groups.g1?.members).toEqual([
      {
        memberId: "m1-gen2",
        clientId: "consumer-a",
        assignments: [{ topic: "orders", partitions: [0, 1] }],
        liveness: "alive",
      },
    ]);
  });

  test("a later snapshot reconciles away an optimistic member the broker never confirmed", () => {
    const withOptimisticMember = reduceWorld(
      reduceWorld(
        createInitialWorld(),
        makeEvent(100, "admin.snapshot", {
          topics: [],
          groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
        }),
      ),
      makeEvent(150, "consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "m1",
        generationId: 1,
        isLeader: true,
        assignments: [],
      }),
    );

    const reconciled = reduceWorld(
      withOptimisticMember,
      makeEvent(200, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    expect(reconciled.groups.g1?.members).toEqual([]);
  });

  test("removing a member via consumer.group.left, interleaved with a join for another member", () => {
    const baseline = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    let world = reduceWorld(
      baseline,
      makeEvent(150, "consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "m1",
        generationId: 1,
        isLeader: true,
        assignments: [],
      }),
    );
    world = reduceWorld(
      world,
      makeEvent(160, "consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-b",
        memberId: "m2",
        generationId: 1,
        isLeader: false,
        assignments: [],
      }),
    );
    world = reduceWorld(
      world,
      makeEvent(170, "consumer.group.left", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "m1",
      }),
    );

    expect(world.groups.g1?.members.map((m) => m.clientId)).toEqual(["consumer-b"]);
  });

  test("consumer.group.left is a no-op when the group is not yet known", () => {
    const result = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "consumer.group.left", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "m1",
      }),
    );

    expect(result.groups).toEqual({});
  });
});

describe("reduceWorld: consumer.connection.lost (kill scenario)", () => {
  function joinedWorld() {
    const baseline = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
      }),
    );

    return reduceWorld(
      baseline,
      makeEvent(150, "consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "m1",
        generationId: 1,
        isLeader: true,
        assignments: [{ topic: "orders", partitions: [0] }],
      }),
    );
  }

  test("marks the member as lost instead of removing it", () => {
    const world = joinedWorld();

    const result = reduceWorld(
      world,
      makeEvent(200, "consumer.connection.lost", {
        groupId: "g1",
        clientId: "consumer-a",
        reason: "kill",
      }),
    );

    expect(result.groups.g1?.members).toEqual([
      {
        memberId: "m1",
        clientId: "consumer-a",
        assignments: [{ topic: "orders", partitions: [0] }],
        liveness: "lost",
      },
    ]);
  });

  test("the lost mark is carried forward across an admin.snapshot that still lists the member (broker hasn't timed it out yet)", () => {
    const lost = reduceWorld(
      joinedWorld(),
      makeEvent(200, "consumer.connection.lost", {
        groupId: "g1",
        clientId: "consumer-a",
        reason: "kill",
      }),
    );

    const afterSnapshot = reduceWorld(
      lost,
      makeEvent(250, "admin.snapshot", {
        topics: [],
        groups: [
          {
            groupId: "g1",
            state: "Stable",
            members: [
              {
                memberId: "m1",
                clientId: "consumer-a",
                assignments: [{ topic: "orders", partitions: [0] }],
              },
            ],
            offsets: [],
          },
        ],
      }),
    );

    expect(afterSnapshot.groups.g1?.members).toEqual([
      {
        memberId: "m1",
        clientId: "consumer-a",
        assignments: [{ topic: "orders", partitions: [0] }],
        liveness: "lost",
      },
    ]);
  });

  test("the lost mark (and the member) disappears once the broker's own snapshot no longer lists the member (real session timeout)", () => {
    const lost = reduceWorld(
      joinedWorld(),
      makeEvent(200, "consumer.connection.lost", {
        groupId: "g1",
        clientId: "consumer-a",
        reason: "kill",
      }),
    );

    const afterTimeout = reduceWorld(
      lost,
      makeEvent(9000, "admin.snapshot", {
        topics: [],
        groups: [{ groupId: "g1", state: "Empty", members: [], offsets: [] }],
      }),
    );

    expect(afterTimeout.groups.g1?.members).toEqual([]);
    expect(afterTimeout.groups.g1?.state).toBe("Empty");
  });

  test("is a no-op when the group is not yet known", () => {
    const result = reduceWorld(
      createInitialWorld(),
      makeEvent(100, "consumer.connection.lost", {
        groupId: "g1",
        clientId: "consumer-a",
        reason: "kill",
      }),
    );

    expect(result.groups).toEqual({});
  });

  test("is a no-op when the clientId is not a known member of the group", () => {
    const world = joinedWorld();

    const result = reduceWorld(
      world,
      makeEvent(200, "consumer.connection.lost", {
        groupId: "g1",
        clientId: "consumer-unknown",
        reason: "kill",
      }),
    );

    expect(result).toBe(world);
  });

  test("a lost member rejoining with the same clientId returns to alive", () => {
    const lost = reduceWorld(
      joinedWorld(),
      makeEvent(200, "consumer.connection.lost", {
        groupId: "g1",
        clientId: "consumer-a",
        reason: "kill",
      }),
    );

    const rejoined = reduceWorld(
      lost,
      makeEvent(250, "consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "m2",
        generationId: 2,
        isLeader: true,
        assignments: [{ topic: "orders", partitions: [0] }],
      }),
    );

    expect(rejoined.groups.g1?.members).toEqual([
      {
        memberId: "m2",
        clientId: "consumer-a",
        assignments: [{ topic: "orders", partitions: [0] }],
        liveness: "alive",
      },
    ]);
  });
});

describe("reduceWorld: unrelated events", () => {
  test("leaves the world unchanged for an event type it doesn't handle", () => {
    const world = createInitialWorld();

    const result = reduceWorld(
      world,
      makeEvent(100, "producer.send.start", { topic: "orders", messageCount: 1, keys: [null] }),
    );

    expect(result).toBe(world);
  });

  test("leaves the world unchanged for a payload that fails schema validation", () => {
    const world = createInitialWorld();

    const result = reduceWorld(world, makeEvent(100, "admin.snapshot", { bogus: true }));

    expect(result).toBe(world);
  });
});
