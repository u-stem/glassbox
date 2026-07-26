import { describe, expect, test } from "bun:test";
import { parseEnv } from "./env";

describe("parseEnv", () => {
  test("defaults PORT to 4000 when unset", () => {
    const env = parseEnv({});

    expect(env.PORT).toBe(4000);
  });

  test("defaults KAFKA_BROKERS to localhost:9092 when unset", () => {
    const env = parseEnv({});

    expect(env.KAFKA_BROKERS).toBe("localhost:9092");
  });

  test("parses PORT from a numeric string", () => {
    const env = parseEnv({ PORT: "5000" });

    expect(env.PORT).toBe(5000);
  });

  test("uses provided KAFKA_BROKERS value", () => {
    const env = parseEnv({ KAFKA_BROKERS: "broker-1:9092,broker-2:9092" });

    expect(env.KAFKA_BROKERS).toBe("broker-1:9092,broker-2:9092");
  });

  test("rejects a non-numeric PORT", () => {
    expect(() => parseEnv({ PORT: "not-a-number" })).toThrow();
  });

  test("defaults EVENT_BUFFER_CAPACITY to 1000 when unset", () => {
    const env = parseEnv({});

    expect(env.EVENT_BUFFER_CAPACITY).toBe(1000);
  });

  test("defaults ADMIN_POLL_INTERVAL_MS to 1000 when unset", () => {
    const env = parseEnv({});

    expect(env.ADMIN_POLL_INTERVAL_MS).toBe(1000);
  });

  test("defaults WEB_ORIGIN to http://localhost:3000 when unset", () => {
    const env = parseEnv({});

    expect(env.WEB_ORIGIN).toBe("http://localhost:3000");
  });

  test("rejects an EVENT_BUFFER_CAPACITY of 0", () => {
    expect(() => parseEnv({ EVENT_BUFFER_CAPACITY: "0" })).toThrow();
  });

  test("accepts an EVENT_BUFFER_CAPACITY of 1", () => {
    const env = parseEnv({ EVENT_BUFFER_CAPACITY: "1" });

    expect(env.EVENT_BUFFER_CAPACITY).toBe(1);
  });

  test("leaves GLASSBOX_COMPOSE_FILE unset when not provided", () => {
    const env = parseEnv({});

    expect(env.GLASSBOX_COMPOSE_FILE).toBeUndefined();
  });

  test("uses provided GLASSBOX_COMPOSE_FILE value", () => {
    const env = parseEnv({ GLASSBOX_COMPOSE_FILE: "/elsewhere/compose.yaml" });

    expect(env.GLASSBOX_COMPOSE_FILE).toBe("/elsewhere/compose.yaml");
  });
});
