import { healthzResponseSchema } from "@glassbox/schema";

/** The home screen's read of the gateway/broker's reachability, distinct from
 * gateway's own kafkaStatus (which only has 3 states) -- "checking" also covers the
 * home screen's own pre-first-tick state, kept as a variant here (not a separate
 * `undefined`) so every consumer switches over one exhaustive union. */
export type EnvironmentStatus =
  | { kind: "checking" }
  | { kind: "gateway-down" }
  | { kind: "kafka-down" }
  | { kind: "ready" };

const DOWN_KINDS: ReadonlyArray<EnvironmentStatus["kind"]> = ["gateway-down", "kafka-down"];

function isDown(status: EnvironmentStatus): boolean {
  return DOWN_KINDS.includes(status.kind);
}

/** The outcome of one fetch attempt against /healthz, already reduced to "did we get
 * an ok HTTP response back" -- network errors and non-2xx responses are both folded
 * into `{ ok: false }` by the caller (startHealthPolling) before reaching here, since
 * both mean the same thing to this UI (the gateway isn't reachable). */
export type HealthFetchOutcome = { ok: false } | { ok: true; body: unknown };

/** Pure mapping from a /healthz fetch outcome to the home screen's status. A
 * response that isn't ok, or whose body doesn't parse as HealthzResponse, is treated
 * the same as the gateway being unreachable -- there's no meaningful distinction for
 * this UI between "gateway down" and "gateway up but returning garbage". */
export function toEnvironmentStatus(outcome: HealthFetchOutcome): EnvironmentStatus {
  if (!outcome.ok) {
    return { kind: "gateway-down" };
  }
  const parsed = healthzResponseSchema.safeParse(outcome.body);
  if (!parsed.success) {
    return { kind: "gateway-down" };
  }
  switch (parsed.data.kafka) {
    case "unreachable":
      return { kind: "kafka-down" };
    case "unknown":
      return { kind: "checking" };
    case "connected":
      return { kind: "ready" };
  }
}

/** Minimal shape startHealthPolling needs from a fetch response, so it can be
 * exercised with a fake in tests without constructing a real DOM Response (mirrors
 * sse.ts's EventSourceLike adapter pattern). The real global `fetch` satisfies this
 * structurally. */
export interface HealthFetchResponse {
  ok: boolean;
  json: () => Promise<unknown>;
}
export type HealthFetchImpl = (
  url: string,
  init: { signal: AbortSignal },
) => Promise<HealthFetchResponse>;

export interface StartHealthPollingOptions {
  url: string;
  intervalMs?: number;
  timeoutMs?: number;
  fetchImpl?: HealthFetchImpl;
  onStatus: (status: EnvironmentStatus) => void;
}

const DEFAULT_INTERVAL_MS = 5000;
const DEFAULT_TIMEOUT_MS = 2000;
/** A single failed tick must not flip the UI to "down" -- a lone slow/dropped
 * request is common noise, not a real outage. Only a *second consecutive* failure
 * is reported, so a transient blip never makes the status dot flicker. */
const CONSECUTIVE_FAILURES_BEFORE_DOWN = 2;

/** One /healthz fetch reduced to a HealthFetchOutcome: network errors, timeouts and
 * non-2xx responses all fold into `{ ok: false }`. Shared by startHealthPolling's
 * ticks and the home page's server-side initial fetch (which exists so the first
 * paint already shows the real status instead of flashing "checking"). */
export async function fetchHealthOutcome(
  url: string,
  options: { timeoutMs: number; fetchImpl?: HealthFetchImpl; signal?: AbortSignal },
): Promise<HealthFetchOutcome> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const timeoutSignal = AbortSignal.timeout(options.timeoutMs);
  const signal =
    options.signal === undefined ? timeoutSignal : AbortSignal.any([options.signal, timeoutSignal]);
  try {
    const response = await fetchImpl(url, { signal });
    return response.ok ? { ok: true, body: await response.json() } : { ok: false };
  } catch {
    return { ok: false };
  }
}

/**
 * Polls `/healthz` at a fixed interval (fetch-then-schedule-next, like the gateway's
 * own admin-poller, so a slow response never overlaps with the next tick) and reports
 * EnvironmentStatus via `onStatus`. Down statuses (gateway-down/kafka-down) are only
 * reported after `CONSECUTIVE_FAILURES_BEFORE_DOWN` failures in a row; every other
 * status is reported immediately. Returns a cleanup function that stops polling and
 * aborts any in-flight request.
 */
export function startHealthPolling(options: StartHealthPollingOptions): () => void {
  const intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const fetchImpl = options.fetchImpl ?? fetch;

  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let consecutiveDownCount = 0;
  let inFlight: AbortController | undefined;

  async function tick(): Promise<void> {
    if (stopped) {
      return;
    }

    inFlight = new AbortController();
    const outcome = await fetchHealthOutcome(options.url, {
      timeoutMs,
      fetchImpl,
      signal: inFlight.signal,
    });
    inFlight = undefined;

    if (stopped) {
      return;
    }

    const status = toEnvironmentStatus(outcome);
    if (isDown(status)) {
      consecutiveDownCount += 1;
      if (consecutiveDownCount >= CONSECUTIVE_FAILURES_BEFORE_DOWN) {
        options.onStatus(status);
      }
    } else {
      consecutiveDownCount = 0;
      options.onStatus(status);
    }

    if (!stopped) {
      timer = setTimeout(() => {
        void tick();
      }, intervalMs);
    }
  }

  void tick();

  return () => {
    stopped = true;
    inFlight?.abort();
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
  };
}
