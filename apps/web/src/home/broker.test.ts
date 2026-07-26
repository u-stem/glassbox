import { describe, expect, test } from "bun:test";
import {
  type BrokerFetchResponse,
  type BrokerViewStatus,
  nextPollDelayMs,
  requestBrokerAction,
  startBrokerPolling,
  toBrokerButtonModel,
  toBrokerStatusLabel,
  toBrokerViewStatus,
} from "./broker";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("toBrokerViewStatus", () => {
  test("maps a failed request to unreachable", () => {
    expect(toBrokerViewStatus({ ok: false })).toEqual({ kind: "unreachable" });
  });

  test("maps a body that fails schema validation to unreachable", () => {
    expect(toBrokerViewStatus({ ok: true, body: { broker: { kind: "levitating" } } })).toEqual({
      kind: "unreachable",
    });
  });

  test("passes a running broker through", () => {
    expect(toBrokerViewStatus({ ok: true, body: { broker: { kind: "running" } } })).toEqual({
      kind: "running",
    });
  });

  /** Reported with a 503, but it is still the gateway's authoritative observation --
   * "docker is not usable" is exactly what the UI needs to say. */
  test("passes an unavailable broker through", () => {
    const outcome = {
      ok: true as const,
      body: { broker: { kind: "unavailable", reason: "docker-missing" } },
    };

    expect(toBrokerViewStatus(outcome)).toEqual({ kind: "unavailable", reason: "docker-missing" });
  });
});

describe("nextPollDelayMs", () => {
  /** The only state that changes on its own: everything else changes because the
   * user pressed a button, and that path refreshes explicitly. */
  test("polls a starting broker often", () => {
    expect(nextPollDelayMs({ kind: "starting", health: "starting" })).toBe(2000);
  });

  test("polls a running broker sparingly", () => {
    expect(nextPollDelayMs({ kind: "running" })).toBe(10_000);
  });

  test("polls a stopped broker sparingly", () => {
    expect(nextPollDelayMs({ kind: "stopped" })).toBe(10_000);
  });

  test("polls an unreachable gateway sparingly", () => {
    expect(nextPollDelayMs({ kind: "unreachable" })).toBe(10_000);
  });
});

describe("toBrokerStatusLabel", () => {
  test("labels a running broker", () => {
    expect(toBrokerStatusLabel({ kind: "running" })).toBe("稼働中");
  });

  test("labels a broker still starting up", () => {
    expect(toBrokerStatusLabel({ kind: "starting", health: "starting" })).toBe("起動中…");
  });

  /** A healthcheck that keeps failing is not the same as a slow boot, and saying so
   * is the difference between "wait" and "something is wrong". */
  test("distinguishes a failing healthcheck from a slow boot", () => {
    expect(toBrokerStatusLabel({ kind: "starting", health: "unhealthy" })).toBe(
      "起動中…(ヘルスチェック失敗)",
    );
  });

  test("labels a stopped broker", () => {
    expect(toBrokerStatusLabel({ kind: "stopped" })).toBe("停止中");
  });

  test("labels a broker whose container does not exist yet", () => {
    expect(toBrokerStatusLabel({ kind: "absent" })).toBe("未作成");
  });

  test("labels an unusable docker", () => {
    expect(toBrokerStatusLabel({ kind: "unavailable", reason: "docker-missing" })).toBe(
      "Docker を実行できません",
    );
  });

  test("labels the pre-first-tick state", () => {
    expect(toBrokerStatusLabel({ kind: "checking" })).toBe("確認中…");
  });

  test("labels an unreachable gateway", () => {
    expect(toBrokerStatusLabel({ kind: "unreachable" })).toBe("状態を取得できません");
  });
});

describe("toBrokerButtonModel", () => {
  test("offers to stop a running broker", () => {
    expect(toBrokerButtonModel({ kind: "running" }, undefined)).toEqual({
      action: "stop",
      label: "停止",
      disabled: false,
    });
  });

  /** Stopping mid-boot is a legitimate thing to want, especially when the
   * healthcheck is failing. */
  test("offers to stop a starting broker", () => {
    expect(toBrokerButtonModel({ kind: "starting", health: "starting" }, undefined).action).toBe(
      "stop",
    );
  });

  test("offers to start a stopped broker", () => {
    expect(toBrokerButtonModel({ kind: "stopped" }, undefined)).toEqual({
      action: "start",
      label: "起動",
      disabled: false,
    });
  });

  test("offers to start a broker whose container does not exist yet", () => {
    expect(toBrokerButtonModel({ kind: "absent" }, undefined).action).toBe("start");
  });

  test("disables the button when docker itself is unusable", () => {
    expect(
      toBrokerButtonModel({ kind: "unavailable", reason: "docker-missing" }, undefined).disabled,
    ).toBe(true);
  });

  test("disables the button before the first observation", () => {
    expect(toBrokerButtonModel({ kind: "checking" }, undefined).disabled).toBe(true);
  });

  test("disables the button while the gateway is unreachable", () => {
    expect(toBrokerButtonModel({ kind: "unreachable" }, undefined).disabled).toBe(true);
  });

  test("disables the button while an action is running", () => {
    expect(toBrokerButtonModel({ kind: "stopped" }, "start").disabled).toBe(true);
  });

  test("says what the running action is doing", () => {
    expect(toBrokerButtonModel({ kind: "stopped" }, "start").label).toBe("起動中…");
  });

  test("says when a stop is running", () => {
    expect(toBrokerButtonModel({ kind: "running" }, "stop").label).toBe("停止中…");
  });
});

interface FetchCall {
  url: string;
  method: string;
  body: string | undefined;
}

/** Replays the given responses in order, repeating the last once exhausted. */
function makeFetchImpl(responses: ReadonlyArray<{ body?: unknown; fails?: boolean }>): {
  fetchImpl: (
    url: string,
    init: { method: string; body?: string | undefined; signal: AbortSignal },
  ) => Promise<BrokerFetchResponse>;
  calls: FetchCall[];
} {
  const calls: FetchCall[] = [];
  let index = 0;
  const fetchImpl = async (
    url: string,
    init: { method: string; body?: string | undefined; signal: AbortSignal },
  ): Promise<BrokerFetchResponse> => {
    calls.push({ url, method: init.method, body: init.body });
    const response = responses[Math.min(index, responses.length - 1)];
    index += 1;
    if (response === undefined || response.fails === true) {
      throw new Error("network down");
    }
    return { ok: true, json: async () => response.body };
  };
  return { fetchImpl, calls };
}

const RUNNING_BODY = { broker: { kind: "running" } };

describe("startBrokerPolling", () => {
  test("reports the broker after the first tick", async () => {
    const reported: BrokerViewStatus[] = [];
    const { fetchImpl } = makeFetchImpl([{ body: RUNNING_BODY }]);

    const polling = startBrokerPolling({
      url: "http://gateway.test/api/themes/kafka/broker",
      intervalMs: 10,
      timeoutMs: 10,
      fetchImpl,
      onStatus: (status) => reported.push(status),
    });
    await sleep(20);
    polling.stop();

    expect(reported[0]).toEqual({ kind: "running" });
  });

  /** The gateway restarting under `tsx watch` drops requests for a moment; a single
   * miss must not flash "cannot read the state" at the user. */
  test("does not report unreachable after a single failure between successes", async () => {
    const reported: BrokerViewStatus[] = [];
    const { fetchImpl } = makeFetchImpl([
      { body: RUNNING_BODY },
      { fails: true },
      { body: RUNNING_BODY },
    ]);

    const polling = startBrokerPolling({
      url: "http://gateway.test/api/themes/kafka/broker",
      intervalMs: 10,
      timeoutMs: 10,
      fetchImpl,
      onStatus: (status) => reported.push(status),
    });
    await sleep(60);
    polling.stop();

    expect(reported.some((status) => status.kind === "unreachable")).toBe(false);
  });

  test("reports unreachable after two consecutive failures", async () => {
    const reported: BrokerViewStatus[] = [];
    const { fetchImpl } = makeFetchImpl([{ fails: true }]);

    const polling = startBrokerPolling({
      url: "http://gateway.test/api/themes/kafka/broker",
      intervalMs: 10,
      timeoutMs: 10,
      fetchImpl,
      onStatus: (status) => reported.push(status),
    });
    await sleep(50);
    polling.stop();

    expect(reported.some((status) => status.kind === "unreachable")).toBe(true);
  });

  test("stops reporting after stop()", async () => {
    const reported: BrokerViewStatus[] = [];
    const { fetchImpl } = makeFetchImpl([{ body: RUNNING_BODY }]);

    const polling = startBrokerPolling({
      url: "http://gateway.test/api/themes/kafka/broker",
      intervalMs: 10,
      timeoutMs: 10,
      fetchImpl,
      onStatus: (status) => reported.push(status),
    });
    polling.stop();
    await sleep(30);

    expect(reported).toEqual([]);
  });

  test("refreshNow() issues a request without waiting for the interval", async () => {
    const { fetchImpl, calls } = makeFetchImpl([{ body: RUNNING_BODY }]);

    const polling = startBrokerPolling({
      url: "http://gateway.test/api/themes/kafka/broker",
      intervalMs: 60_000,
      timeoutMs: 10,
      fetchImpl,
      onStatus: () => {},
    });
    await sleep(10);
    polling.refreshNow();
    await sleep(10);
    polling.stop();

    expect(calls.length).toBeGreaterThanOrEqual(2);
  });

  /** A response issued before a start/stop must not land afterwards and roll the UI
   * back to the pre-action state. */
  test("discards the result of a tick that refreshNow() superseded", async () => {
    const reported: BrokerViewStatus[] = [];
    let resolveFirst = (): void => {};
    const firstGate = new Promise<void>((resolve) => {
      resolveFirst = resolve;
    });
    let callCount = 0;
    const fetchImpl = async (): Promise<BrokerFetchResponse> => {
      callCount += 1;
      if (callCount === 1) {
        await firstGate;
        return { ok: true, json: async () => ({ broker: { kind: "stopped" } }) };
      }
      return { ok: true, json: async () => RUNNING_BODY };
    };

    const polling = startBrokerPolling({
      url: "http://gateway.test/api/themes/kafka/broker",
      intervalMs: 60_000,
      timeoutMs: 10_000,
      fetchImpl,
      onStatus: (status) => reported.push(status),
    });
    polling.refreshNow();
    await sleep(20);
    resolveFirst();
    await sleep(20);
    polling.stop();

    expect(reported.some((status) => status.kind === "stopped")).toBe(false);
  });
});

describe("requestBrokerAction", () => {
  test("posts the requested action", async () => {
    const { fetchImpl, calls } = makeFetchImpl([{ body: RUNNING_BODY }]);

    await requestBrokerAction("http://gateway.test/api/themes/kafka/broker", "start", {
      timeoutMs: 100,
      fetchImpl,
    });

    expect(calls[0]?.body).toBe(JSON.stringify({ action: "start" }));
  });

  test("uses POST", async () => {
    const { fetchImpl, calls } = makeFetchImpl([{ body: RUNNING_BODY }]);

    await requestBrokerAction("http://gateway.test/api/themes/kafka/broker", "stop", {
      timeoutMs: 100,
      fetchImpl,
    });

    expect(calls[0]?.method).toBe("POST");
  });

  /** The gateway answers 503 with a perfectly good body when docker is unusable. */
  test("reads the status out of a non-2xx response", async () => {
    const fetchImpl = async (): Promise<BrokerFetchResponse> => ({
      ok: false,
      json: async () => ({ broker: { kind: "unavailable", reason: "docker-missing" } }),
    });

    const status = await requestBrokerAction(
      "http://gateway.test/api/themes/kafka/broker",
      "start",
      { timeoutMs: 100, fetchImpl },
    );

    expect(status).toEqual({ kind: "unavailable", reason: "docker-missing" });
  });

  test("reports a request that never got through as unreachable", async () => {
    const fetchImpl = async (): Promise<BrokerFetchResponse> => {
      throw new Error("network down");
    };

    const status = await requestBrokerAction(
      "http://gateway.test/api/themes/kafka/broker",
      "start",
      { timeoutMs: 100, fetchImpl },
    );

    expect(status).toEqual({ kind: "unreachable" });
  });
});
