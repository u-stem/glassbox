import { describe, expect, test } from "bun:test";
import { fetchHealthOutcome, startHealthPolling, toEnvironmentStatus } from "./health";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("toEnvironmentStatus", () => {
  test("maps a failed fetch outcome to gateway-down", () => {
    expect(toEnvironmentStatus({ ok: false })).toEqual({ kind: "gateway-down" });
  });

  test("maps a body that fails schema validation to gateway-down", () => {
    expect(
      toEnvironmentStatus({ ok: true, body: { ok: true, kafka: "not-a-real-value" } }),
    ).toEqual({ kind: "gateway-down" });
  });

  test("maps kafka: unreachable to kafka-down", () => {
    expect(toEnvironmentStatus({ ok: true, body: { ok: true, kafka: "unreachable" } })).toEqual({
      kind: "kafka-down",
    });
  });

  test("maps kafka: unknown to checking", () => {
    expect(toEnvironmentStatus({ ok: true, body: { ok: true, kafka: "unknown" } })).toEqual({
      kind: "checking",
    });
  });

  test("maps kafka: connected to ready", () => {
    expect(toEnvironmentStatus({ ok: true, body: { ok: true, kafka: "connected" } })).toEqual({
      kind: "ready",
    });
  });
});

function makeFetchImpl(results: ReadonlyArray<{ ok: boolean; body?: unknown }>): {
  fetchImpl: (
    url: string,
    init: { signal: AbortSignal },
  ) => Promise<{
    ok: boolean;
    json: () => Promise<unknown>;
  }>;
  calls: number[];
} {
  const calls: number[] = [];
  let index = 0;
  const fetchImpl = async () => {
    const result = results[Math.min(index, results.length - 1)];
    calls.push(index);
    index += 1;
    if (result === undefined || !result.ok) {
      return { ok: false, json: async () => ({}) };
    }
    return { ok: true, json: async () => result.body };
  };
  return { fetchImpl, calls };
}

const READY_BODY = { ok: true, kafka: "connected" };

describe("fetchHealthOutcome", () => {
  test("returns the body for a successful response", async () => {
    const { fetchImpl } = makeFetchImpl([{ ok: true, body: READY_BODY }]);

    const outcome = await fetchHealthOutcome("http://gateway/healthz", {
      timeoutMs: 1000,
      fetchImpl,
    });

    expect(outcome).toEqual({ ok: true, body: READY_BODY });
  });

  test("folds a thrown fetch error into ok: false", async () => {
    const fetchImpl = async (): Promise<{ ok: boolean; json: () => Promise<unknown> }> => {
      throw new Error("connection refused");
    };

    const outcome = await fetchHealthOutcome("http://gateway/healthz", {
      timeoutMs: 1000,
      fetchImpl,
    });

    expect(outcome).toEqual({ ok: false });
  });
});

describe("startHealthPolling", () => {
  test("reports ready after a successful first tick", async () => {
    const reported: Array<{ kind: string }> = [];
    const { fetchImpl } = makeFetchImpl([{ ok: true, body: READY_BODY }]);

    const stop = startHealthPolling({
      url: "http://gateway.test/healthz",
      intervalMs: 10,
      timeoutMs: 10,
      fetchImpl,
      onStatus: (status) => reported.push(status),
    });
    await sleep(20);
    stop();

    expect(reported[0]).toEqual({ kind: "ready" });
  });

  test("does not report down after a single failure sandwiched between successes", async () => {
    const reported: Array<{ kind: string }> = [];
    const { fetchImpl } = makeFetchImpl([
      { ok: true, body: READY_BODY },
      { ok: false },
      { ok: true, body: READY_BODY },
    ]);

    const stop = startHealthPolling({
      url: "http://gateway.test/healthz",
      intervalMs: 10,
      timeoutMs: 10,
      fetchImpl,
      onStatus: (status) => reported.push(status),
    });
    await sleep(50);
    stop();

    expect(reported.some((status) => status.kind === "gateway-down")).toBe(false);
  });

  test("reports gateway-down after two consecutive failures", async () => {
    const reported: Array<{ kind: string }> = [];
    const { fetchImpl } = makeFetchImpl([{ ok: false }, { ok: false }]);

    const stop = startHealthPolling({
      url: "http://gateway.test/healthz",
      intervalMs: 10,
      timeoutMs: 10,
      fetchImpl,
      onStatus: (status) => reported.push(status),
    });
    await sleep(30);
    stop();

    expect(reported.some((status) => status.kind === "gateway-down")).toBe(true);
  });

  test("stops calling onStatus after the returned cleanup function runs", async () => {
    const reported: Array<{ kind: string }> = [];
    const { fetchImpl } = makeFetchImpl([{ ok: true, body: READY_BODY }]);

    const stop = startHealthPolling({
      url: "http://gateway.test/healthz",
      intervalMs: 10,
      timeoutMs: 10,
      fetchImpl,
      onStatus: (status) => reported.push(status),
    });
    stop();
    await sleep(30);

    expect(reported.length).toBe(0);
  });
});
