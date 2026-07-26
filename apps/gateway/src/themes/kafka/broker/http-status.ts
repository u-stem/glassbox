import type { BrokerStatus } from "@glassbox/schema";

/**
 * Maps an observed broker status onto an HTTP status, the same way the scenario route
 * maps its domain results (404/409/422/202). "The broker is stopped" is a successful
 * observation, not an error -- only a failure to observe at all is.
 *
 * The response body carries the status either way, and the web client reads it
 * regardless of the HTTP code: even a 503 says *why* the broker is unavailable, which
 * is exactly what the UI needs to show.
 */
export function toBrokerHttpStatus(broker: BrokerStatus): number {
  if (broker.kind !== "unavailable") {
    return 200;
  }
  switch (broker.reason) {
    case "docker-timeout":
      return 504;
    case "unrecognized":
      // docker answered and we failed to understand it: our bug, not the daemon's.
      return 500;
    case "docker-missing":
    case "docker-failed":
    case "compose-missing":
      return 503;
  }
}
