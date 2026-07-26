import type { BrokerAction } from "@glassbox/schema";

/** The service name in docker/compose.yaml. Every command is scoped to it so a
 * compose file that grows a second service is never started or stopped by accident. */
export const BROKER_SERVICE_NAME = "kafka";

/**
 * Argument vectors for `docker`, always fixed arrays passed to execFile without a
 * shell -- nothing user-supplied ever reaches them (ADR-0005).
 *
 * `-p` is deliberately never passed: compose derives the project name from the
 * compose file's parent directory, so `-f <absolute path>` alone addresses the same
 * project the README's CLI steps create, regardless of the gateway's cwd.
 */
export function buildStatusArgs(composePath: string): string[] {
  // -a is required: a stopped container is omitted from `ps` entirely, which would be
  // indistinguishable from a container that was never created.
  return ["compose", "-f", composePath, "ps", "-a", "--format", "json", BROKER_SERVICE_NAME];
}

export function buildActionArgs(composePath: string, action: BrokerAction): string[] {
  if (action === "start") {
    // `up` rather than `start` because `start` fails outright when the container does
    // not exist yet, and --no-recreate because recreating the container would discard
    // the anonymous volumes holding the broker's log dirs.
    return ["compose", "-f", composePath, "up", "-d", "--no-recreate", BROKER_SERVICE_NAME];
  }
  // `stop`, never `down`: the container (and with it the broker's data) survives.
  return ["compose", "-f", composePath, "stop", BROKER_SERVICE_NAME];
}
