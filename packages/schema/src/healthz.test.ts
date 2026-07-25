import { describe, expect, test } from "bun:test";
import { healthzResponseSchema } from "./healthz";

describe("healthzResponseSchema", () => {
  test("accepts a valid healthz response", () => {
    const result = healthzResponseSchema.safeParse({ ok: true, kafka: "connected" });

    expect(result.success).toBe(true);
  });

  test("rejects a response missing kafka", () => {
    const result = healthzResponseSchema.safeParse({ ok: true });

    expect(result.success).toBe(false);
  });

  test("rejects an unrecognized kafka value", () => {
    const result = healthzResponseSchema.safeParse({ ok: true, kafka: "degraded" });

    expect(result.success).toBe(false);
  });
});
