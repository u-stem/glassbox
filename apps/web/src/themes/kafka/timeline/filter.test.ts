import { describe, expect, test } from "bun:test";
import type { GlassboxEvent } from "@glassbox/schema";
import { EMPTY_TIMELINE_FILTER, filterEvents } from "./filter";

function event(type: string, actorId: string | undefined): GlassboxEvent {
  return {
    id: `${type}-${actorId ?? "none"}`,
    seq: 0,
    ts: 0,
    theme: "kafka",
    source: actorId === undefined ? { kind: "admin" } : { kind: "client", actorId },
    type,
    payload: {},
  };
}

describe("filterEvents", () => {
  test("returns every event unchanged when the filter is empty", () => {
    const events = [event("a", "x"), event("b", "y")];

    expect(filterEvents(events, EMPTY_TIMELINE_FILTER)).toEqual(events);
  });

  test("keeps only events matching the type filter", () => {
    const matching = event("producer.send.end", "x");
    const events = [matching, event("consumer.message", "x")];

    const result = filterEvents(events, { type: "producer.send.end", actorId: "" });

    expect(result).toEqual([matching]);
  });

  test("keeps only events matching the actorId filter", () => {
    const matching = event("a", "producer-1");
    const events = [matching, event("a", "producer-2")];

    const result = filterEvents(events, { type: "", actorId: "producer-1" });

    expect(result).toEqual([matching]);
  });

  test("combines type and actorId filters", () => {
    const matching = event("consumer.message", "consumer-1");
    const events = [
      matching,
      event("consumer.message", "consumer-2"),
      event("producer.send.end", "consumer-1"),
    ];

    const result = filterEvents(events, { type: "consumer.message", actorId: "consumer-1" });

    expect(result).toEqual([matching]);
  });
});
