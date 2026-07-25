import { describe, expect, test } from "bun:test";
import type { GlassboxEvent } from "@glassbox/schema";
import { parseTransition } from "./history";

function stateChangedEvent(ts: number, groupId: string, from: string, to: string): GlassboxEvent {
  return {
    id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    seq: 0,
    ts,
    theme: "kafka",
    source: { kind: "admin" },
    type: "group.state.changed",
    payload: { groupId, from, to, edge: "joining" },
  };
}

describe("parseTransition", () => {
  test("parses a group.state.changed event into its groupId and history entry", () => {
    const result = parseTransition(stateChangedEvent(200, "g1", "Stable", "PreparingRebalance"));

    expect(result).toEqual({
      groupId: "g1",
      entry: { ts: 200, from: "Stable", to: "PreparingRebalance", edge: "joining" },
    });
  });

  test("returns undefined for an event of a different type", () => {
    const event: GlassboxEvent = {
      id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      seq: 0,
      ts: 100,
      theme: "kafka",
      source: { kind: "client" },
      type: "consumer.message",
      payload: { groupId: "g1", clientId: "c", topic: "t", partition: 0, offset: 0, key: null },
    };

    expect(parseTransition(event)).toBeUndefined();
  });

  test("returns undefined when the payload fails schema validation", () => {
    const event: GlassboxEvent = {
      id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      seq: 0,
      ts: 100,
      theme: "kafka",
      source: { kind: "admin" },
      type: "group.state.changed",
      payload: { bogus: true },
    };

    expect(parseTransition(event)).toBeUndefined();
  });
});
