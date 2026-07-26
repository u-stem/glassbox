import { type BrokerAction, type BrokerStatus, brokerStatusResponseSchema } from "@glassbox/schema";

/**
 * The broker status as this screen knows it: the gateway's own union plus the two
 * states only a client can be in. Kept as one union (rather than a separate
 * `undefined`) so every consumer switches over one exhaustive type, mirroring how
 * EnvironmentStatus folds "checking" into its own union.
 */
export type BrokerViewStatus =
  | BrokerStatus
  // Before the first tick has answered.
  | { kind: "checking" }
  // The gateway itself did not answer, so the container's state is simply unknown --
  // distinct from "docker is unusable", which is the gateway telling us something.
  | { kind: "unreachable" };

/** One request's outcome, already reduced to "did we get a body we can read". */
export type BrokerFetchOutcome = { ok: false } | { ok: true; body: unknown };

/** Minimal shape needed from a fetch response, so the polling can be exercised with a
 * fake instead of a real DOM Response (same adapter pattern as health.ts / sse.ts). */
export interface BrokerFetchResponse {
  ok: boolean;
  json: () => Promise<unknown>;
}

export type BrokerFetchImpl = (
  url: string,
  init: {
    method: string;
    headers?: Record<string, string>;
    body?: string;
    signal: AbortSignal;
  },
) => Promise<BrokerFetchResponse>;

/**
 * Pure mapping from a request outcome to the status to display. Note what is *not*
 * here: a non-2xx response is not treated as failure. The gateway answers 503 with a
 * perfectly good body when docker is unusable, and that body is exactly the
 * observation the UI needs -- only an unreadable/unparseable answer is "unreachable".
 */
export function toBrokerViewStatus(outcome: BrokerFetchOutcome): BrokerViewStatus {
  if (!outcome.ok) {
    return { kind: "unreachable" };
  }
  const parsed = brokerStatusResponseSchema.safeParse(outcome.body);
  return parsed.success ? parsed.data.broker : { kind: "unreachable" };
}

const IDLE_POLL_MS = 10_000;
/** Only `starting` moves on its own (the healthcheck's start_period is 20s), so it is
 * the only state worth watching closely; every other state changes because someone
 * pressed a button, and that path calls refreshNow() directly. */
const STARTING_POLL_MS = 2000;

export function nextPollDelayMs(status: BrokerViewStatus): number {
  return status.kind === "starting" ? STARTING_POLL_MS : IDLE_POLL_MS;
}

export function toBrokerStatusLabel(status: BrokerViewStatus): string {
  switch (status.kind) {
    case "running":
      return "稼働中";
    case "starting":
      return status.health === "unhealthy" ? "起動中…(ヘルスチェック失敗)" : "起動中…";
    case "stopped":
      return "停止中";
    case "absent":
      return "未作成";
    case "unavailable":
      return "Docker を実行できません";
    case "checking":
      return "確認中…";
    case "unreachable":
      return "状態を取得できません";
  }
}

export interface BrokerButtonModel {
  action: BrokerAction;
  label: string;
  disabled: boolean;
}

/** Which single button to offer, and whether it can be pressed. `pending` wins over
 * the observed status: while an action is in flight the label reports that action,
 * not the state it is about to leave. */
export function toBrokerButtonModel(
  status: BrokerViewStatus,
  pending: BrokerAction | undefined,
): BrokerButtonModel {
  if (pending !== undefined) {
    return {
      action: pending,
      label: pending === "start" ? "起動中…" : "停止中…",
      disabled: true,
    };
  }

  switch (status.kind) {
    case "running":
    case "starting":
      // Stopping mid-boot is legitimate, especially when the healthcheck is failing.
      return { action: "stop", label: "停止", disabled: false };
    case "stopped":
    case "absent":
      return { action: "start", label: "起動", disabled: false };
    case "checking":
      return { action: "start", label: "確認中…", disabled: true };
    case "unavailable":
    case "unreachable":
      // Nothing this button can do would work; the surrounding UI explains why.
      return { action: "start", label: "起動", disabled: true };
  }
}

const DEFAULT_TIMEOUT_MS = 3000;

async function fetchBrokerOutcome(
  url: string,
  options: {
    timeoutMs: number;
    fetchImpl?: BrokerFetchImpl;
    signal?: AbortSignal;
    method?: string;
    body?: string;
  },
): Promise<BrokerFetchOutcome> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const timeoutSignal = AbortSignal.timeout(options.timeoutMs);
  const signal =
    options.signal === undefined ? timeoutSignal : AbortSignal.any([options.signal, timeoutSignal]);
  try {
    const response = await fetchImpl(url, {
      method: options.method ?? "GET",
      headers: { "Content-Type": "application/json" },
      // Spread conditionally: exactOptionalPropertyTypes rejects an explicit
      // `body: undefined` for an optional property.
      ...(options.body === undefined ? {} : { body: options.body }),
      signal,
    });
    // Deliberately not gated on response.ok -- see toBrokerViewStatus.
    return { ok: true, body: await response.json() };
  } catch {
    return { ok: false };
  }
}

/** Sends a start/stop and returns the state the gateway observed afterwards, so the
 * UI can update before the next poll tick. */
export async function requestBrokerAction(
  url: string,
  action: BrokerAction,
  options: { timeoutMs: number; fetchImpl?: BrokerFetchImpl },
): Promise<BrokerViewStatus> {
  const outcome = await fetchBrokerOutcome(url, {
    timeoutMs: options.timeoutMs,
    ...(options.fetchImpl === undefined ? {} : { fetchImpl: options.fetchImpl }),
    method: "POST",
    body: JSON.stringify({ action }),
  });
  return toBrokerViewStatus(outcome);
}

export interface StartBrokerPollingOptions {
  url: string;
  intervalMs?: number;
  timeoutMs?: number;
  fetchImpl?: BrokerFetchImpl;
  onStatus: (status: BrokerViewStatus) => void;
}

export interface BrokerPolling {
  stop: () => void;
  /** Aborts the in-flight tick and polls immediately. Used right after a start/stop
   * so the UI converges without waiting out the interval. */
  refreshNow: () => void;
}

/** Same reasoning as health.ts's hysteresis: the gateway restarting under `tsx watch`
 * drops a request now and then, and a single miss must not flash an error. Applied
 * only to failed requests -- an `unavailable` the gateway actually reported is an
 * authoritative observation and is shown at once. */
const CONSECUTIVE_FAILURES_BEFORE_UNREACHABLE = 2;

/**
 * Polls the gateway's broker endpoint, fetch-then-schedule-next so a slow response
 * never overlaps the next tick (same shape as startHealthPolling).
 *
 * Ticks carry the epoch they were started in, and both stop() and refreshNow() bump
 * it. That is what makes a superseded response harmless: a status read issued *before*
 * a start/stop would otherwise land afterwards and roll the UI back to the state the
 * user just changed. It also keeps an aborted request from being counted as a failure.
 */
export function startBrokerPolling(options: StartBrokerPollingOptions): BrokerPolling {
  const intervalMs = options.intervalMs ?? IDLE_POLL_MS;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;

  let stopped = false;
  let epoch = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let inFlight: AbortController | undefined;
  let consecutiveFailures = 0;

  function isCurrent(tickEpoch: number): boolean {
    return !stopped && tickEpoch === epoch;
  }

  async function tick(tickEpoch: number): Promise<void> {
    if (!isCurrent(tickEpoch)) {
      return;
    }

    inFlight = new AbortController();
    const outcome = await fetchBrokerOutcome(options.url, {
      timeoutMs,
      ...(options.fetchImpl === undefined ? {} : { fetchImpl: options.fetchImpl }),
      signal: inFlight.signal,
    });
    inFlight = undefined;

    if (!isCurrent(tickEpoch)) {
      return;
    }

    const status = toBrokerViewStatus(outcome);
    if (status.kind === "unreachable") {
      consecutiveFailures += 1;
      if (consecutiveFailures >= CONSECUTIVE_FAILURES_BEFORE_UNREACHABLE) {
        options.onStatus(status);
      }
    } else {
      consecutiveFailures = 0;
      options.onStatus(status);
    }

    if (isCurrent(tickEpoch)) {
      // intervalMs is the idle cadence and therefore an upper bound: a broker that is
      // still starting is polled faster, never slower than the caller asked for.
      timer = setTimeout(
        () => {
          void tick(tickEpoch);
        },
        Math.min(intervalMs, nextPollDelayMs(status)),
      );
    }
  }

  void tick(epoch);

  return {
    stop() {
      stopped = true;
      epoch += 1;
      inFlight?.abort();
      if (timer !== undefined) {
        clearTimeout(timer);
        timer = undefined;
      }
    },
    refreshNow() {
      if (stopped) {
        return;
      }
      epoch += 1;
      inFlight?.abort();
      if (timer !== undefined) {
        clearTimeout(timer);
        timer = undefined;
      }
      consecutiveFailures = 0;
      void tick(epoch);
    },
  };
}
