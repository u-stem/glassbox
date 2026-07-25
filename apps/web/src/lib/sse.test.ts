import { describe, expect, test } from "bun:test";
import { connectKafkaEvents, type EventSourceLike } from "./sse";

function makeFakeEventSource(): EventSourceLike & {
  emit: (data: string) => void;
  closed: boolean;
} {
  let listener: ((data: string) => void) | undefined;
  return {
    onMessage(cb) {
      listener = cb;
    },
    close() {
      this.closed = true;
    },
    closed: false,
    emit(data: string) {
      listener?.(data);
    },
  };
}

const validEvent = {
  id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
  seq: 0,
  ts: 1_700_000_000_000,
  theme: "kafka",
  source: { kind: "client" as const, actorId: "producer-1" },
  type: "producer.send.start",
  payload: { topic: "orders", messageCount: 1, keys: [null] },
};

describe("connectKafkaEvents", () => {
  test("calls onEvent with a parsed GlassboxEvent for a valid message", () => {
    const fakeSource = makeFakeEventSource();
    const received: unknown[] = [];
    connectKafkaEvents(fakeSource, { onEvent: (event) => received.push(event), onReset: () => {} });

    fakeSource.emit(JSON.stringify(validEvent));

    expect(received).toEqual([validEvent]);
  });

  test("calls onReset instead of onEvent for a control.reset message", () => {
    const fakeSource = makeFakeEventSource();
    let resetCalled = false;
    const received: unknown[] = [];
    connectKafkaEvents(fakeSource, {
      onEvent: (event) => received.push(event),
      onReset: () => {
        resetCalled = true;
      },
    });

    fakeSource.emit(
      JSON.stringify({
        ...validEvent,
        type: "control.reset",
        payload: { reason: "gap" },
      }),
    );

    expect(resetCalled).toBe(true);
    expect(received).toEqual([]);
  });

  test("silently ignores malformed JSON", () => {
    const fakeSource = makeFakeEventSource();
    const received: unknown[] = [];
    connectKafkaEvents(fakeSource, { onEvent: (event) => received.push(event), onReset: () => {} });

    fakeSource.emit("not json");

    expect(received).toEqual([]);
  });

  test("silently ignores JSON that fails envelope validation", () => {
    const fakeSource = makeFakeEventSource();
    const received: unknown[] = [];
    connectKafkaEvents(fakeSource, { onEvent: (event) => received.push(event), onReset: () => {} });

    fakeSource.emit(JSON.stringify({ ...validEvent, seq: -1 }));

    expect(received).toEqual([]);
  });

  test("closes the underlying event source when the returned cleanup is called", () => {
    const fakeSource = makeFakeEventSource();
    const cleanup = connectKafkaEvents(fakeSource, { onEvent: () => {}, onReset: () => {} });

    cleanup();

    expect(fakeSource.closed).toBe(true);
  });
});
