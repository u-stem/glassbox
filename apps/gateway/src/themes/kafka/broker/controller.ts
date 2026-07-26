import type { BrokerAction, BrokerStatus } from "@glassbox/schema";
import { buildActionArgs, buildStatusArgs } from "./args";
import type { ComposePathResult } from "./compose-path";
import type { DockerCommandOutcome, DockerRunner } from "./docker-cli";
import { parseComposePs, toBrokerStatus } from "./parse-ps";

export interface BrokerStatusOutcome {
  broker: BrokerStatus;
  /** docker's own stderr, truncated. Present only when something went wrong. */
  detail?: string | undefined;
}

export type BrokerApplyResult =
  | { status: "ok"; outcome: BrokerStatusOutcome }
  | { status: "conflict" };

export interface BrokerController {
  status: () => Promise<BrokerStatusOutcome>;
  apply: (action: BrokerAction) => Promise<BrokerApplyResult>;
}

export interface BrokerControllerTimeouts {
  statusMs?: number;
  startMs?: number;
  stopMs?: number;
}

export interface BrokerControllerDeps {
  run: DockerRunner;
  composePath: ComposePathResult;
  timeouts?: BrokerControllerTimeouts;
}

const DEFAULT_STATUS_TIMEOUT_MS = 10_000;
const DEFAULT_STOP_TIMEOUT_MS = 60_000;
/** Longer than the others because a first `up` may pull the image, which is minutes
 * on a cold cache. The UI does not block on this: it polls until the state settles. */
const DEFAULT_START_TIMEOUT_MS = 180_000;

/** Long enough to carry docker's actual complaint, short enough that a runaway stack
 * trace never becomes the UI. */
const MAX_DETAIL_LENGTH = 500;

function truncateDetail(text: string): string | undefined {
  const trimmed = text.trim();
  if (trimmed === "") {
    return undefined;
  }
  return trimmed.length > MAX_DETAIL_LENGTH ? trimmed.slice(0, MAX_DETAIL_LENGTH) : trimmed;
}

function withDetail(broker: BrokerStatus, detail: string | undefined): BrokerStatusOutcome {
  // Built conditionally rather than passing `detail: undefined`, which
  // exactOptionalPropertyTypes rejects for an optional property.
  return detail === undefined ? { broker } : { broker, detail };
}

/** The error text an unsuccessful invocation left behind, if any. `not-found` and
 * `timeout` carry none: the status read that follows an action already reports those
 * as docker-missing / docker-timeout, and a synthetic message would add nothing. */
function toFailureDetail(outcome: DockerCommandOutcome): string | undefined {
  if (outcome.kind === "spawn-error") {
    return truncateDetail(outcome.message);
  }
  if (outcome.kind === "completed" && outcome.result.exitCode !== 0) {
    return truncateDetail(outcome.result.stderr);
  }
  return undefined;
}

/**
 * Owns every `docker compose` invocation the broker routes make. Two properties
 * matter beyond the mapping itself:
 *
 * - An action is always followed by a fresh status read, and the *observed* state is
 *   what gets returned -- including when the action failed. A failed `up` often means
 *   the container was already running, and showing what is actually there is more
 *   useful than reporting only the error.
 * - Actions are single-flight. Mashing the button must not spawn several
 *   `docker compose up` concurrently (same reasoning as produce-burst's conflict
 *   guard in server.ts).
 */
export function createBrokerController(deps: BrokerControllerDeps): BrokerController {
  const statusMs = deps.timeouts?.statusMs ?? DEFAULT_STATUS_TIMEOUT_MS;
  const startMs = deps.timeouts?.startMs ?? DEFAULT_START_TIMEOUT_MS;
  const stopMs = deps.timeouts?.stopMs ?? DEFAULT_STOP_TIMEOUT_MS;

  let actionInFlight = false;

  const composeMissing: BrokerStatusOutcome = {
    broker: { kind: "unavailable", reason: "compose-missing" },
  };

  async function status(): Promise<BrokerStatusOutcome> {
    if (!deps.composePath.found) {
      return composeMissing;
    }

    const outcome = await deps.run(buildStatusArgs(deps.composePath.path), {
      timeoutMs: statusMs,
    });

    if (outcome.kind === "not-found") {
      return { broker: { kind: "unavailable", reason: "docker-missing" } };
    }
    if (outcome.kind === "timeout") {
      return { broker: { kind: "unavailable", reason: "docker-timeout" } };
    }
    if (outcome.kind === "spawn-error" || outcome.result.exitCode !== 0) {
      return withDetail({ kind: "unavailable", reason: "docker-failed" }, toFailureDetail(outcome));
    }

    return { broker: toBrokerStatus(parseComposePs(outcome.result.stdout)) };
  }

  async function apply(action: BrokerAction): Promise<BrokerApplyResult> {
    if (!deps.composePath.found) {
      return { status: "ok", outcome: composeMissing };
    }
    if (actionInFlight) {
      return { status: "conflict" };
    }

    actionInFlight = true;
    try {
      const outcome = await deps.run(buildActionArgs(deps.composePath.path, action), {
        timeoutMs: action === "start" ? startMs : stopMs,
      });
      const failureDetail = toFailureDetail(outcome);
      const observed = await status();

      // The action's own error text wins over the status read's: it explains why the
      // broker is not in the state the user asked for, which the status read cannot.
      return {
        status: "ok",
        outcome:
          failureDetail === undefined ? observed : withDetail(observed.broker, failureDetail),
      };
    } finally {
      actionInFlight = false;
    }
  }

  return { status, apply };
}
