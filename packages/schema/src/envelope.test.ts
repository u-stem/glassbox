import { describe, expect, test } from "bun:test";
import { glassboxEventSchema } from "./envelope";

const validEvent = {
  id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
  seq: 0,
  ts: 1_700_000_000_000,
  theme: "kafka",
  source: { kind: "client" as const, actorId: "producer-1" },
  type: "producer.send.start",
  payload: { topic: "orders" },
};

describe("glassboxEventSchema", () => {
  test("accepts a valid event envelope", () => {
    const result = glassboxEventSchema.safeParse(validEvent);

    expect(result.success).toBe(true);
  });

  test("accepts source without actorId", () => {
    const result = glassboxEventSchema.safeParse({
      ...validEvent,
      source: { kind: "admin" as const },
    });

    expect(result.success).toBe(true);
  });

  test("rejects a non-ulid id", () => {
    const result = glassboxEventSchema.safeParse({
      ...validEvent,
      id: "not-a-ulid",
    });

    expect(result.success).toBe(false);
  });

  test("rejects a negative seq", () => {
    const result = glassboxEventSchema.safeParse({
      ...validEvent,
      seq: -1,
    });

    expect(result.success).toBe(false);
  });

  test("rejects a non-integer seq", () => {
    const result = glassboxEventSchema.safeParse({
      ...validEvent,
      seq: 1.5,
    });

    expect(result.success).toBe(false);
  });

  test("rejects an unknown source kind", () => {
    const result = glassboxEventSchema.safeParse({
      ...validEvent,
      source: { kind: "unknown" },
    });

    expect(result.success).toBe(false);
  });

  test("rejects a missing type", () => {
    const { type: _type, ...withoutType } = validEvent;
    const result = glassboxEventSchema.safeParse(withoutType);

    expect(result.success).toBe(false);
  });

  test("accepts arbitrary payload shapes", () => {
    const result = glassboxEventSchema.safeParse({
      ...validEvent,
      payload: [1, 2, 3],
    });

    expect(result.success).toBe(true);
  });
});
