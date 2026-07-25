import { beforeEach, describe, expect, test } from "bun:test";
import type { GlassboxEvent } from "@glassbox/schema";
import { useKafkaStore } from "./store";

function makeEvent(overrides: Partial<GlassboxEvent> = {}): GlassboxEvent {
  return {
    id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    seq: 0,
    ts: 1_700_000_000_000,
    theme: "kafka",
    source: { kind: "client", actorId: "producer-1" },
    type: "producer.send.start",
    payload: { topic: "orders", messageCount: 1, keys: [null] },
    ...overrides,
  };
}

beforeEach(() => {
  useKafkaStore.getState().reset();
});

describe("useKafkaStore", () => {
  test("starts with an empty event list", () => {
    expect(useKafkaStore.getState().events).toEqual([]);
  });

  test("prepends newly added events (newest first)", () => {
    useKafkaStore.getState().addEvent(makeEvent({ seq: 0 }));
    useKafkaStore.getState().addEvent(makeEvent({ seq: 1 }));

    expect(useKafkaStore.getState().events.map((e) => e.seq)).toEqual([1, 0]);
  });

  test("caps the event list at the configured maximum", () => {
    for (let i = 0; i < 2050; i += 1) {
      useKafkaStore.getState().addEvent(makeEvent({ seq: i }));
    }

    expect(useKafkaStore.getState().events.length).toBe(2000);
  });

  test("has no latest snapshot initially", () => {
    expect(useKafkaStore.getState().latestSnapshot).toBeUndefined();
  });

  test("records the payload of an admin.snapshot event as the latest snapshot", () => {
    const snapshotPayload = {
      topics: [{ name: "orders", partitions: [{ index: 0, earliestOffset: 0, endOffset: 5 }] }],
    };

    useKafkaStore
      .getState()
      .addEvent(makeEvent({ type: "admin.snapshot", payload: snapshotPayload }));

    expect(useKafkaStore.getState().latestSnapshot).toEqual(snapshotPayload);
  });

  test("keeps the previous snapshot when a non-snapshot event arrives", () => {
    const snapshotPayload = {
      topics: [{ name: "orders", partitions: [{ index: 0, earliestOffset: 0, endOffset: 5 }] }],
    };
    useKafkaStore
      .getState()
      .addEvent(makeEvent({ type: "admin.snapshot", payload: snapshotPayload }));

    useKafkaStore.getState().addEvent(makeEvent({ type: "producer.send.start" }));

    expect(useKafkaStore.getState().latestSnapshot).toEqual(snapshotPayload);
  });

  test("reset clears both events and the latest snapshot", () => {
    useKafkaStore.getState().addEvent(
      makeEvent({
        type: "admin.snapshot",
        payload: { topics: [] },
      }),
    );

    useKafkaStore.getState().reset();

    expect(useKafkaStore.getState().events).toEqual([]);
    expect(useKafkaStore.getState().latestSnapshot).toBeUndefined();
  });

  test("starts with an empty world (no groups, no snapshot yet)", () => {
    expect(useKafkaStore.getState().world).toEqual({
      topics: [],
      groups: {},
      snapshotFetchedAt: 0,
      groupStateFromTransition: {},
    });
  });

  test("derives group state into world via the world-reducer (ADR-0003)", () => {
    useKafkaStore.getState().addEvent(
      makeEvent({
        ts: 100,
        type: "admin.snapshot",
        payload: {
          topics: [],
          groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
        },
      }),
    );

    useKafkaStore.getState().addEvent(
      makeEvent({
        ts: 200,
        type: "group.state.changed",
        payload: { groupId: "g1", from: "Stable", to: "PreparingRebalance", edge: "joining" },
      }),
    );

    expect(useKafkaStore.getState().world.groups.g1?.state).toBe("PreparingRebalance");
  });

  test("reset also clears world back to its initial value", () => {
    useKafkaStore.getState().addEvent(
      makeEvent({
        ts: 100,
        type: "admin.snapshot",
        payload: {
          topics: [],
          groups: [{ groupId: "g1", state: "Stable", members: [], offsets: [] }],
        },
      }),
    );

    useKafkaStore.getState().reset();

    expect(useKafkaStore.getState().world).toEqual({
      topics: [],
      groups: {},
      snapshotFetchedAt: 0,
      groupStateFromTransition: {},
    });
  });

  test("starts with no transition history", () => {
    expect(useKafkaStore.getState().transitionHistoryByGroup).toEqual({});
  });

  test("appends a group.state.changed event to its group's transition history", () => {
    useKafkaStore.getState().addEvent(
      makeEvent({
        ts: 200,
        type: "group.state.changed",
        payload: { groupId: "g1", from: "Stable", to: "PreparingRebalance", edge: "joining" },
      }),
    );

    expect(useKafkaStore.getState().transitionHistoryByGroup.g1).toEqual([
      { ts: 200, from: "Stable", to: "PreparingRebalance", edge: "joining" },
    ]);
  });

  test("keeps transition history unchanged (same reference) for a non-group.state.changed event", () => {
    useKafkaStore.getState().addEvent(
      makeEvent({
        ts: 200,
        type: "group.state.changed",
        payload: { groupId: "g1", from: "Stable", to: "PreparingRebalance", edge: "joining" },
      }),
    );
    const before = useKafkaStore.getState().transitionHistoryByGroup;

    useKafkaStore.getState().addEvent(makeEvent({ type: "producer.send.start" }));

    expect(useKafkaStore.getState().transitionHistoryByGroup).toBe(before);
  });

  test("caps each group's transition history at 10 entries, newest first", () => {
    for (let i = 0; i < 12; i += 1) {
      useKafkaStore.getState().addEvent(
        makeEvent({
          ts: i,
          type: "group.state.changed",
          payload: { groupId: "g1", from: "Stable", to: "PreparingRebalance", edge: "joining" },
        }),
      );
    }

    const history = useKafkaStore.getState().transitionHistoryByGroup.g1 ?? [];
    expect(history.length).toBe(10);
    expect(history[0]?.ts).toBe(11);
  });

  test("reset clears transition history", () => {
    useKafkaStore.getState().addEvent(
      makeEvent({
        type: "group.state.changed",
        payload: { groupId: "g1", from: "Stable", to: "PreparingRebalance", edge: "joining" },
      }),
    );

    useKafkaStore.getState().reset();

    expect(useKafkaStore.getState().transitionHistoryByGroup).toEqual({});
  });
});
