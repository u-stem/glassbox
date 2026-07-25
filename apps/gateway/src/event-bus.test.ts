import { describe, expect, test } from "bun:test";
import { EventBus } from "./event-bus";

function makeEvent(type: string) {
  return {
    id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    ts: 1_700_000_000_000,
    theme: "kafka",
    source: { kind: "scenario" as const },
    type,
    payload: {},
  };
}

describe("EventBus", () => {
  test("assigns seq 0 to the first published event", () => {
    const bus = new EventBus({ capacity: 10 });

    const published = bus.publish(makeEvent("a"));

    expect(published.seq).toBe(0);
  });

  test("assigns monotonically increasing seq values", () => {
    const bus = new EventBus({ capacity: 10 });

    bus.publish(makeEvent("a"));
    const second = bus.publish(makeEvent("b"));
    const third = bus.publish(makeEvent("c"));

    expect([second.seq, third.seq]).toEqual([1, 2]);
  });

  test("notifies subscribers of published events", () => {
    const bus = new EventBus({ capacity: 10 });
    const received: string[] = [];
    bus.subscribe((event) => received.push(event.type));

    bus.publish(makeEvent("a"));
    bus.publish(makeEvent("b"));

    expect(received).toEqual(["a", "b"]);
  });

  test("stops notifying a subscriber after it unsubscribes", () => {
    const bus = new EventBus({ capacity: 10 });
    const received: string[] = [];
    const unsubscribe = bus.subscribe((event) => received.push(event.type));

    bus.publish(makeEvent("a"));
    unsubscribe();
    bus.publish(makeEvent("b"));

    expect(received).toEqual(["a"]);
  });

  test("evicts the oldest event once capacity is exceeded", () => {
    const bus = new EventBus({ capacity: 2 });

    bus.publish(makeEvent("a"));
    bus.publish(makeEvent("b"));
    bus.publish(makeEvent("c"));

    const result = bus.getSince(-1);

    expect(result.events.map((e) => e.type)).toEqual(["b", "c"]);
  });

  test("getSince returns no gap and an empty list for an empty bus", () => {
    const bus = new EventBus({ capacity: 10 });

    const result = bus.getSince(-1);

    expect(result).toEqual({ gap: false, events: [] });
  });

  test("getSince returns events strictly after the given seq", () => {
    const bus = new EventBus({ capacity: 10 });
    bus.publish(makeEvent("a"));
    bus.publish(makeEvent("b"));
    bus.publish(makeEvent("c"));

    const result = bus.getSince(0);

    expect(result.events.map((e) => e.type)).toEqual(["b", "c"]);
  });

  test("getSince reports no gap when the requested seq is exactly one before the oldest buffered seq", () => {
    const bus = new EventBus({ capacity: 2 });
    bus.publish(makeEvent("a"));
    bus.publish(makeEvent("b"));
    bus.publish(makeEvent("c"));

    // buffer now holds seq 1 (b) and seq 2 (c); oldest seq is 1
    const result = bus.getSince(0);

    expect(result.gap).toBe(false);
  });

  test("getSince detects a gap when the requested seq is older than the buffer retains", () => {
    const bus = new EventBus({ capacity: 2 });
    bus.publish(makeEvent("a"));
    bus.publish(makeEvent("b"));
    bus.publish(makeEvent("c"));

    const result = bus.getSince(-1);

    expect(result.gap).toBe(true);
  });

  test("returns the full buffer contents when a gap is detected", () => {
    const bus = new EventBus({ capacity: 2 });
    bus.publish(makeEvent("a"));
    bus.publish(makeEvent("b"));
    bus.publish(makeEvent("c"));

    const result = bus.getSince(-1);

    expect(result.events.map((e) => e.type)).toEqual(["b", "c"]);
  });

  test("getSince returns an empty list when the caller is already up to date", () => {
    const bus = new EventBus({ capacity: 10 });
    bus.publish(makeEvent("a"));
    const last = bus.publish(makeEvent("b"));

    const result = bus.getSince(last.seq);

    expect(result).toEqual({ gap: false, events: [] });
  });

  describe("getSinceAndSubscribe (SSE reconnect race)", () => {
    test("regression guard: a naive getSince-then-subscribe call sequence can lose an event published in between", () => {
      const bus = new EventBus({ capacity: 10 });
      bus.publish(makeEvent("a"));

      const backlog = bus.getSince(-1);
      // Simulates an event published in the window between reading the backlog and
      // registering a live subscriber (e.g. if that gap ever became async).
      bus.publish(makeEvent("b"));
      const received: string[] = [];
      bus.subscribe((event) => received.push(event.type));

      // "b" is silently lost by the naive pattern: it happened before subscribe.
      expect(backlog.events.map((e) => e.type)).toEqual(["a"]);
      expect(received).toEqual([]);
    });

    test("captures an event published exactly during the getSince snapshot computation", () => {
      const bus = new EventBus({ capacity: 10 });
      bus.publish(makeEvent("a"));

      const originalGetSince = bus.getSince.bind(bus);
      bus.getSince = (lastSeq: number) => {
        // Simulates the race window: something publishes while the snapshot is being read.
        bus.publish(makeEvent("interleaved"));
        return originalGetSince(lastSeq);
      };

      const received: string[] = [];
      const { result } = bus.getSinceAndSubscribe(-1, (event) => received.push(event.type));

      const allSeen = [...result.events.map((e) => e.type), ...received];
      expect(allSeen).toContain("interleaved");
    });

    test("delivers the backlog once and does not duplicate it via the live subscriber", () => {
      const bus = new EventBus({ capacity: 10 });
      bus.publish(makeEvent("a"));
      bus.publish(makeEvent("b"));

      const received: string[] = [];
      const { result } = bus.getSinceAndSubscribe(-1, (event) => received.push(event.type));

      expect(result.events.map((e) => e.type)).toEqual(["a", "b"]);
      expect(received).toEqual([]);
    });

    test("delivers events published after subscribing via the live listener", () => {
      const bus = new EventBus({ capacity: 10 });
      bus.publish(makeEvent("a"));

      const received: string[] = [];
      bus.getSinceAndSubscribe(-1, (event) => received.push(event.type));

      bus.publish(makeEvent("b"));

      expect(received).toEqual(["b"]);
    });

    test("stops delivering events once unsubscribed", () => {
      const bus = new EventBus({ capacity: 10 });

      const received: string[] = [];
      const { unsubscribe } = bus.getSinceAndSubscribe(-1, (event) => received.push(event.type));
      unsubscribe();

      bus.publish(makeEvent("a"));

      expect(received).toEqual([]);
    });

    test("reports a gap and does not duplicate the full-buffer resend via the live subscriber", () => {
      const bus = new EventBus({ capacity: 2 });
      bus.publish(makeEvent("a"));
      bus.publish(makeEvent("b"));
      bus.publish(makeEvent("c"));

      const received: string[] = [];
      const { result } = bus.getSinceAndSubscribe(-1, (event) => received.push(event.type));

      expect(result.gap).toBe(true);
      expect(result.events.map((e) => e.type)).toEqual(["b", "c"]);
      expect(received).toEqual([]);
    });
  });
});
