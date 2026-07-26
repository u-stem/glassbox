import { describe, expect, test } from "bun:test";
import {
  brokerActionRequestSchema,
  brokerStatusResponseSchema,
  brokerStatusSchema,
} from "./broker";

describe("brokerStatusSchema", () => {
  test("accepts a running broker", () => {
    const result = brokerStatusSchema.safeParse({ kind: "running" });

    expect(result.success).toBe(true);
  });

  test("accepts a starting broker within its healthcheck grace period", () => {
    const result = brokerStatusSchema.safeParse({ kind: "starting", health: "starting" });

    expect(result.success).toBe(true);
  });

  test("accepts a starting broker whose healthcheck is failing", () => {
    const result = brokerStatusSchema.safeParse({ kind: "starting", health: "unhealthy" });

    expect(result.success).toBe(true);
  });

  test("rejects a starting broker without its health detail", () => {
    const result = brokerStatusSchema.safeParse({ kind: "starting" });

    expect(result.success).toBe(false);
  });

  test("accepts a stopped broker", () => {
    const result = brokerStatusSchema.safeParse({ kind: "stopped" });

    expect(result.success).toBe(true);
  });

  test("accepts a broker whose container was never created", () => {
    const result = brokerStatusSchema.safeParse({ kind: "absent" });

    expect(result.success).toBe(true);
  });

  test("accepts an unavailable broker with a known reason", () => {
    const result = brokerStatusSchema.safeParse({ kind: "unavailable", reason: "docker-missing" });

    expect(result.success).toBe(true);
  });

  test("rejects an unrecognized unavailable reason", () => {
    const result = brokerStatusSchema.safeParse({ kind: "unavailable", reason: "docker-tired" });

    expect(result.success).toBe(false);
  });

  test("rejects an unrecognized kind", () => {
    const result = brokerStatusSchema.safeParse({ kind: "rebooting" });

    expect(result.success).toBe(false);
  });
});

describe("brokerActionRequestSchema", () => {
  test("accepts the start action", () => {
    const result = brokerActionRequestSchema.safeParse({ action: "start" });

    expect(result.success).toBe(true);
  });

  test("accepts the stop action", () => {
    const result = brokerActionRequestSchema.safeParse({ action: "stop" });

    expect(result.success).toBe(true);
  });

  /** Guards ADR-0005's decision that this API never tears containers down: `down`
   * would delete the broker's anonymous volumes along with the container. */
  test("rejects the down action", () => {
    const result = brokerActionRequestSchema.safeParse({ action: "down" });

    expect(result.success).toBe(false);
  });

  test("rejects a request without an action", () => {
    const result = brokerActionRequestSchema.safeParse({});

    expect(result.success).toBe(false);
  });
});

describe("brokerStatusResponseSchema", () => {
  test("accepts a response carrying only the broker status", () => {
    const result = brokerStatusResponseSchema.safeParse({ broker: { kind: "running" } });

    expect(result.success).toBe(true);
  });

  test("accepts a response carrying a detail excerpt", () => {
    const result = brokerStatusResponseSchema.safeParse({
      broker: { kind: "unavailable", reason: "docker-failed" },
      detail: "Cannot connect to the Docker daemon",
    });

    expect(result.success).toBe(true);
  });
});
