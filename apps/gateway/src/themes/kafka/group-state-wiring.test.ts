import { describe, expect, test } from "bun:test";
import type { GlassboxEvent } from "@glassbox/schema";
import { ulid } from "ulid";
import { wireGroupStateTracker } from "./group-state-wiring";

function makeEvent(type: string, payload: unknown): GlassboxEvent {
  return {
    id: ulid(),
    seq: 0,
    ts: Date.now(),
    theme: "kafka",
    source: { kind: "client" },
    type,
    payload,
  };
}

/** Minimal EventBusLike fake: publish() records the event, subscribe() lets the test
 * feed input events through the same listener the wiring registered. */
function createFakeBus() {
  const published: GlassboxEvent[] = [];
  let listener: ((event: GlassboxEvent) => void) | undefined;

  return {
    published,
    feed(event: GlassboxEvent): void {
      listener?.(event);
    },
    publish(event: Omit<GlassboxEvent, "seq">): GlassboxEvent {
      const full = { ...event, seq: published.length } as GlassboxEvent;
      published.push(full);
      return full;
    },
    subscribe(fn: (event: GlassboxEvent) => void): () => void {
      listener = fn;
      return () => {
        listener = undefined;
      };
    },
  };
}

describe("wireGroupStateTracker", () => {
  test("publishes group.state.changed when a joining edge starts a rebalance", () => {
    const bus = createFakeBus();
    wireGroupStateTracker(bus);

    bus.feed(
      makeEvent("consumer.group.joining", {
        groupId: "spike-group",
        clientId: "consumer-a",
        memberId: null,
      }),
    );

    const changed = bus.published.find((e) => e.type === "group.state.changed");
    expect(changed?.payload).toEqual({
      groupId: "spike-group",
      from: "Empty",
      to: "PreparingRebalance",
      generationId: undefined,
      edge: "joining",
    });
  });

  test("drives Preparing -> Completing -> Stable across joining/syncing/joined events", () => {
    const bus = createFakeBus();
    wireGroupStateTracker(bus);

    bus.feed(
      makeEvent("consumer.group.joining", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: null,
      }),
    );
    bus.feed(makeEvent("consumer.group.syncing", { groupId: "g1", clientId: "consumer-a" }));
    bus.feed(
      makeEvent("consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "consumer-a-1",
        generationId: 1,
        isLeader: true,
        assignments: [],
      }),
    );

    const transitions = bus.published
      .filter((e) => e.type === "group.state.changed")
      .map((e) => (e.payload as { to: string }).to);

    expect(transitions).toEqual(["PreparingRebalance", "CompletingRebalance", "Stable"]);
  });

  test("a member leaving publishes a left edge transition", () => {
    const bus = createFakeBus();
    wireGroupStateTracker(bus);

    bus.feed(
      makeEvent("consumer.group.joining", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: null,
      }),
    );
    bus.feed(makeEvent("consumer.group.syncing", { groupId: "g1", clientId: "consumer-a" }));
    bus.feed(
      makeEvent("consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "consumer-a-1",
        generationId: 1,
        isLeader: true,
        assignments: [],
      }),
    );
    bus.feed(
      makeEvent("consumer.group.left", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "consumer-a-1",
      }),
    );

    const last = bus.published.filter((e) => e.type === "group.state.changed").at(-1);
    expect(last?.payload).toEqual({
      groupId: "g1",
      from: "Stable",
      to: "Empty",
      generationId: undefined,
      edge: "left",
    });
  });

  test("consumer.connection.lost for the sole member evicts the tracker's bookkeeping (Stable -> Empty)", () => {
    const bus = createFakeBus();
    wireGroupStateTracker(bus);

    bus.feed(
      makeEvent("consumer.group.joining", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: null,
      }),
    );
    bus.feed(makeEvent("consumer.group.syncing", { groupId: "g1", clientId: "consumer-a" }));
    bus.feed(
      makeEvent("consumer.group.joined", {
        groupId: "g1",
        clientId: "consumer-a",
        memberId: "consumer-a-1",
        generationId: 1,
        isLeader: true,
        assignments: [],
      }),
    );
    bus.feed(
      makeEvent("consumer.connection.lost", {
        groupId: "g1",
        clientId: "consumer-a",
        reason: "kill",
      }),
    );

    const last = bus.published.filter((e) => e.type === "group.state.changed").at(-1);
    expect(last?.payload).toEqual({
      groupId: "g1",
      from: "Stable",
      to: "Empty",
      generationId: undefined,
      edge: "left",
    });
  });

  test("consumer.connection.lost for one of two members does not prematurely change a Stable group's derived state", () => {
    const bus = createFakeBus();
    wireGroupStateTracker(bus);

    for (const clientId of ["consumer-a", "consumer-b"]) {
      bus.feed(makeEvent("consumer.group.joining", { groupId: "g1", clientId, memberId: null }));
      bus.feed(makeEvent("consumer.group.syncing", { groupId: "g1", clientId }));
      bus.feed(
        makeEvent("consumer.group.joined", {
          groupId: "g1",
          clientId,
          memberId: `${clientId}-1`,
          generationId: 1,
          isLeader: clientId === "consumer-a",
          assignments: [],
        }),
      );
    }

    const beforeKill = bus.published.filter((e) => e.type === "group.state.changed").length;

    bus.feed(
      makeEvent("consumer.connection.lost", {
        groupId: "g1",
        clientId: "consumer-b",
        reason: "kill",
      }),
    );

    const afterKill = bus.published.filter((e) => e.type === "group.state.changed").length;
    expect(afterKill).toBe(beforeKill);
  });

  test("ignores unrelated event types", () => {
    const bus = createFakeBus();
    wireGroupStateTracker(bus);

    bus.feed(makeEvent("producer.send.start", { topic: "orders", messageCount: 1, keys: [null] }));

    expect(bus.published.some((e) => e.type === "group.state.changed")).toBe(false);
  });
});
