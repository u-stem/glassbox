import { z } from "zod";

/**
 * Why the broker's state could not be determined. An enum rather than a free-form
 * string: the wire carries a code and the UI owns the wording, so a new failure mode
 * has to be added here (and handled everywhere) instead of leaking an unhandled
 * English message into a Japanese UI.
 */
export const brokerUnavailableReasonSchema = z.enum([
  // The docker CLI is not on PATH (execFile rejected with ENOENT).
  "docker-missing",
  // docker ran but exited non-zero: daemon down, bad compose file, ...
  "docker-failed",
  "docker-timeout",
  // docker/compose.yaml could not be located, so no command was run at all.
  "compose-missing",
  // `ps` succeeded but its output (or the container's State) was not understood.
  "unrecognized",
]);

/**
 * The broker container's state as observed through `docker compose ps`, which is a
 * different question from the gateway's own reachability (`healthzResponseSchema`'s
 * `kafka` field): the container can be up while the broker is not yet answering.
 * Keeping both is deliberate -- see ADR-0005 on why they are never merged into one
 * label.
 *
 * `checking` and `unreachable` are absent on purpose: the server never observes them
 * (they describe the *client's* view of this endpoint) and they live in the web app's
 * own view union instead, mirroring how EnvironmentStatus owns "checking".
 */
export const brokerStatusSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("running") }),
  // Container is up but not answering yet. "starting" = the healthcheck is still in
  // its start_period grace window, "unhealthy" = it has been failing. Both mean
  // "wait" to the UI, but only this distinction separates a slow boot from a stuck
  // broker.
  z.object({ kind: z.literal("starting"), health: z.enum(["starting", "unhealthy"]) }),
  // The container exists but is not running (exited/created/paused/dead).
  z.object({ kind: z.literal("stopped") }),
  // No container for the kafka service exists yet.
  z.object({ kind: z.literal("absent") }),
  z.object({ kind: z.literal("unavailable"), reason: brokerUnavailableReasonSchema }),
]);

/** Deliberately only start/stop: ADR-0005 rules out `down`, which would delete the
 * container together with the anonymous volumes holding the broker's log dirs. */
export const brokerActionSchema = z.enum(["start", "stop"]);

export const brokerActionRequestSchema = z.object({ action: brokerActionSchema });

export const brokerStatusResponseSchema = z.object({
  broker: brokerStatusSchema,
  /** Truncated stderr excerpt from a failed docker invocation, shown verbatim so the
   * cause (daemon not running, port already bound, ...) is visible without a terminal. */
  detail: z.string().optional(),
});

export type BrokerUnavailableReason = z.infer<typeof brokerUnavailableReasonSchema>;
export type BrokerStatus = z.infer<typeof brokerStatusSchema>;
export type BrokerAction = z.infer<typeof brokerActionSchema>;
export type BrokerStatusResponse = z.infer<typeof brokerStatusResponseSchema>;
