import { describe, expect, test } from "bun:test";
import type { GlassboxEvent } from "@glassbox/schema";
import { createInitialWorld, reduceWorld } from "../world-reducer";
import { buildTimeTravelIndex, seqRange, transitionHistoryAtSeq, worldAtSeq } from "./time-travel";

function snapshotEvent(seq: number, groups: unknown[]): GlassboxEvent {
  return {
    id: `01ARZ3NDEKTSV4RRFFQ69G5FA${seq.toString().padStart(2, "0")}`,
    seq,
    ts: seq * 1000,
    theme: "kafka",
    source: { kind: "admin" },
    type: "admin.snapshot",
    payload: { topics: [], groups },
  };
}

function transitionEvent(seq: number, groupId: string, from: string, to: string): GlassboxEvent {
  return {
    id: `01ARZ3NDEKTSV4RRFFQ69G5FB${seq.toString().padStart(2, "0")}`,
    seq,
    ts: seq * 1000,
    theme: "kafka",
    source: { kind: "client", actorId: groupId },
    type: "group.state.changed",
    payload: { groupId, from, to, edge: "joining" },
  };
}

/** Reference implementation with no checkpointing at all, to check worldAtSeq's
 * checkpoint-based replay agrees with a plain full reduce. */
function naiveWorldAtSeq(events: readonly GlassboxEvent[], seq: number) {
  const ascending = [...events].sort((a, b) => a.seq - b.seq);
  let world = createInitialWorld();
  for (const event of ascending) {
    if (event.seq > seq) {
      break;
    }
    world = reduceWorld(world, event);
  }
  return world;
}

describe("buildTimeTravelIndex / worldAtSeq", () => {
  test("returns the initial world for a seq before any event", () => {
    const events = [
      snapshotEvent(5, [{ groupId: "g1", state: "Stable", members: [], offsets: [] }]),
    ];
    const index = buildTimeTravelIndex(events);

    expect(worldAtSeq(index, 1)).toEqual(createInitialWorld());
  });

  test("returns the initial world when there are no events at all", () => {
    const index = buildTimeTravelIndex([]);

    expect(worldAtSeq(index, 100)).toEqual(createInitialWorld());
  });

  test("matches a plain full reduce at an exact event seq", () => {
    const events = [
      transitionEvent(1, "g1", "Empty", "PreparingRebalance"),
      transitionEvent(2, "g1", "PreparingRebalance", "Stable"),
    ];
    const index = buildTimeTravelIndex(events);

    expect(worldAtSeq(index, 1)).toEqual(naiveWorldAtSeq(events, 1));
    expect(worldAtSeq(index, 2)).toEqual(naiveWorldAtSeq(events, 2));
  });

  test("matches a plain full reduce across many events, including checkpoint boundaries", () => {
    // 450 events forces 2 checkpoints (interval 200) plus a partial tail.
    const events: GlassboxEvent[] = [];
    for (let i = 1; i <= 450; i += 1) {
      events.push(
        i % 2 === 0
          ? transitionEvent(i, "g1", "Stable", "PreparingRebalance")
          : transitionEvent(i, "g1", "PreparingRebalance", "Stable"),
      );
    }
    const index = buildTimeTravelIndex(events);

    for (const seq of [1, 100, 199, 200, 201, 399, 400, 401, 450]) {
      expect(worldAtSeq(index, seq)).toEqual(naiveWorldAtSeq(events, seq));
    }
  });

  test("matches a plain full reduce across a mixed event stream (admin.snapshot + transitions) straddling a checkpoint boundary", () => {
    // Checkpoint interval is 200 (see time-travel.ts); this mixes admin.snapshot
    // (authoritative baseline, per ADR-0003) with group.state.changed transitions
    // right around that boundary, so a checkpoint captured exactly at/near seq 200
    // must reflect the snapshot's reset of membership/state, not just a naive
    // "replay every event the same way" result.
    const events: GlassboxEvent[] = [];
    for (let i = 1; i <= 199; i += 1) {
      events.push(
        i % 2 === 0
          ? transitionEvent(i, "g1", "Stable", "PreparingRebalance")
          : transitionEvent(i, "g1", "PreparingRebalance", "Stable"),
      );
    }
    // Lands exactly on the checkpoint boundary (200 events applied so far once this
    // one is included) and re-establishes the group from an admin snapshot.
    events.push(
      snapshotEvent(200, [
        {
          groupId: "g1",
          state: "Stable",
          members: [{ memberId: "m1", clientId: "consumer-1", assignments: [] }],
          offsets: [],
        },
      ]),
    );
    for (let i = 201; i <= 250; i += 1) {
      events.push(
        i % 2 === 0
          ? transitionEvent(i, "g1", "Stable", "PreparingRebalance")
          : transitionEvent(i, "g1", "PreparingRebalance", "Stable"),
      );
    }
    const index = buildTimeTravelIndex(events);

    for (const seq of [150, 199, 200, 201, 225, 250]) {
      expect(worldAtSeq(index, seq)).toEqual(naiveWorldAtSeq(events, seq));
    }
    // Sanity check that the snapshot actually took effect (membership only comes
    // from admin.snapshot, per ADR-0003), not just that the two replay strategies
    // happen to agree on an empty result.
    expect(worldAtSeq(index, 200).groups.g1?.members).toEqual([
      { memberId: "m1", clientId: "consumer-1", assignments: [], liveness: "alive" },
    ]);
  });

  test("handles events arriving out of ascending order in the input (store is newest-first)", () => {
    const events = [
      transitionEvent(2, "g1", "PreparingRebalance", "Stable"),
      transitionEvent(1, "g1", "Empty", "PreparingRebalance"),
    ];
    const index = buildTimeTravelIndex(events);

    expect(worldAtSeq(index, 2).groups.g1?.state).toBe("Stable");
    expect(worldAtSeq(index, 1).groups.g1?.state).toBe("PreparingRebalance");
  });
});

describe("transitionHistoryAtSeq", () => {
  test("only includes transitions up to and including the given seq, newest-first", () => {
    const events = [
      transitionEvent(1, "g1", "Empty", "PreparingRebalance"),
      transitionEvent(2, "g1", "PreparingRebalance", "CompletingRebalance"),
      transitionEvent(3, "g1", "CompletingRebalance", "Stable"),
    ];

    const atSeq2 = transitionHistoryAtSeq(events, 2, "g1", 10);

    expect(atSeq2.map((e) => `${e.from}->${e.to}`)).toEqual([
      "PreparingRebalance->CompletingRebalance",
      "Empty->PreparingRebalance",
    ]);
  });

  test("excludes other groups' transitions", () => {
    const events = [
      transitionEvent(1, "g1", "Empty", "PreparingRebalance"),
      transitionEvent(2, "g2", "Empty", "PreparingRebalance"),
    ];

    expect(transitionHistoryAtSeq(events, 2, "g1", 10)).toHaveLength(1);
  });

  test("caps the returned history", () => {
    const events = Array.from({ length: 5 }, (_, i) =>
      transitionEvent(i + 1, "g1", "Stable", "PreparingRebalance"),
    );

    expect(transitionHistoryAtSeq(events, 5, "g1", 2)).toHaveLength(2);
  });
});

describe("seqRange", () => {
  test("returns undefined for an empty event list", () => {
    expect(seqRange([])).toBeUndefined();
  });

  test("returns the min/max seq across the (possibly unsorted) event list", () => {
    const events = [transitionEvent(5, "g1", "a", "b"), transitionEvent(1, "g1", "a", "b")];

    expect(seqRange(events)).toEqual({ min: 1, max: 5 });
  });
});
