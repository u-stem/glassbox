import type { BrokerStatus } from "@glassbox/schema";
import { BROKER_SERVICE_NAME } from "./args";

/** The three fields of a `docker compose ps` row this gateway reads. The real output
 * carries ~20 more (image, ports, labels, ...) that nothing here depends on. */
export interface ComposePsEntry {
  service: string;
  state: string;
  health: string;
}

/** Hand-written rather than a Zod schema: this shape is docker's, not ours, and the
 * only contract worth enforcing is "the two fields we read are strings". */
function readEntry(value: unknown): ComposePsEntry | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  if (!("Service" in value) || !("State" in value)) {
    return undefined;
  }
  const { Service: service, State: state } = value;
  if (typeof service !== "string" || typeof state !== "string") {
    return undefined;
  }
  // A compose file without a healthcheck reports no Health at all, which is a valid
  // observation rather than a parse failure.
  const health = "Health" in value && typeof value.Health === "string" ? value.Health : "";
  return { service, state, health };
}

function parseJson(source: string): unknown {
  try {
    return JSON.parse(source);
  } catch {
    return undefined;
  }
}

function readRows(rows: unknown[]): ComposePsEntry[] | undefined {
  const entries: ComposePsEntry[] = [];
  for (const row of rows) {
    const entry = readEntry(row);
    if (entry === undefined) {
      return undefined;
    }
    entries.push(entry);
  }
  return entries;
}

/**
 * Parses `docker compose ps -a --format json`. compose >= v2.21 prints JSON Lines
 * (one object per line); older versions print a JSON array, and both are accepted so
 * the same code works against whichever compose the developer has.
 *
 * Returns `undefined` when the output could not be understood at all, which is
 * deliberately distinct from `[]` ("understood, and there is no container"): reading
 * a docker error message as "no container" would report a perfectly healthy broker as
 * absent, and the start button would then act on a false premise.
 */
export function parseComposePs(stdout: string): ComposePsEntry[] | undefined {
  const trimmed = stdout.trim();
  if (trimmed === "") {
    return [];
  }

  if (trimmed.startsWith("[")) {
    const parsed = parseJson(trimmed);
    return Array.isArray(parsed) ? readRows(parsed) : undefined;
  }

  const lines = trimmed.split(/\r?\n/).filter((line) => line.trim() !== "");
  return readRows(lines.map((line) => parseJson(line)));
}

/** States meaning "the container exists but is not running". `paused` is an
 * approximation -- `up -d` does not unpause it -- but pausing is not something this
 * UI can cause, so it is reported as stopped rather than as an unknown state. */
const STOPPED_STATES: ReadonlyArray<string> = ["created", "exited", "dead", "paused", "removing"];

/**
 * Maps the parsed rows onto the broker status the UI switches over. Health is read,
 * not just State: between `up -d` returning and the healthcheck passing there is a
 * window (start_period is 20s) where the container is `running` but the broker is not
 * necessarily answering yet, and calling that "running" would contradict the
 * gateway's own reachability line.
 */
export function toBrokerStatus(entries: ComposePsEntry[] | undefined): BrokerStatus {
  if (entries === undefined) {
    return { kind: "unavailable", reason: "unrecognized" };
  }

  const broker = entries.find((entry) => entry.service === BROKER_SERVICE_NAME);
  if (broker === undefined) {
    return { kind: "absent" };
  }

  if (broker.state === "running") {
    if (broker.health === "starting") {
      return { kind: "starting", health: "starting" };
    }
    if (broker.health === "unhealthy") {
      return { kind: "starting", health: "unhealthy" };
    }
    return { kind: "running" };
  }

  if (broker.state === "restarting") {
    return { kind: "starting", health: "starting" };
  }

  if (STOPPED_STATES.includes(broker.state)) {
    return { kind: "stopped" };
  }

  return { kind: "unavailable", reason: "unrecognized" };
}
