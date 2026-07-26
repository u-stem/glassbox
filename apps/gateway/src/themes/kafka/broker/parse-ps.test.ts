import { describe, expect, test } from "bun:test";
import { type ComposePsEntry, parseComposePs, toBrokerStatus } from "./parse-ps";

/** Shaped after a real `docker compose ps -a --format json` line (compose v5.1.2),
 * trimmed to the fields this parser reads. The real output carries ~20 more. */
function psLine(fields: { Service?: string; State?: string; Health?: string }): string {
  return JSON.stringify({
    Name: "glassbox-kafka",
    Image: "apache/kafka:4.1.0",
    Service: "kafka",
    ...fields,
  });
}

describe("parseComposePs", () => {
  test("reads a single JSON Lines row", () => {
    const entries = parseComposePs(psLine({ State: "running", Health: "healthy" }));

    expect(entries).toEqual([{ service: "kafka", state: "running", health: "healthy" }]);
  });

  test("reads every row of a multi-row JSON Lines output", () => {
    const stdout = [
      psLine({ Service: "kafka", State: "running", Health: "healthy" }),
      psLine({ Service: "zookeeper", State: "exited", Health: "" }),
    ].join("\n");

    const entries = parseComposePs(stdout);

    expect(entries).toHaveLength(2);
  });

  test("treats empty output as no containers rather than a parse failure", () => {
    expect(parseComposePs("")).toEqual([]);
  });

  test("treats whitespace-only output as no containers", () => {
    expect(parseComposePs("  \n \n")).toEqual([]);
  });

  test("tolerates a trailing newline", () => {
    const entries = parseComposePs(`${psLine({ State: "running", Health: "healthy" })}\n`);

    expect(entries).toHaveLength(1);
  });

  /** compose < v2.21 prints a JSON array instead of JSON Lines. */
  test("reads the legacy JSON array format", () => {
    const stdout = `[${psLine({ State: "exited", Health: "" })}]`;

    const entries = parseComposePs(stdout);

    expect(entries).toEqual([{ service: "kafka", state: "exited", health: "" }]);
  });

  test("reports unparseable output rather than silently reading it as no containers", () => {
    expect(parseComposePs("Cannot connect to the Docker daemon")).toBeUndefined();
  });

  test("reports a row missing State as unparseable", () => {
    expect(parseComposePs(psLine({ Health: "healthy" }))).toBeUndefined();
  });

  test("defaults a missing Health to empty rather than failing", () => {
    const entries = parseComposePs(psLine({ State: "running" }));

    expect(entries).toEqual([{ service: "kafka", state: "running", health: "" }]);
  });
});

function entry(state: string, health = ""): ComposePsEntry[] {
  return [{ service: "kafka", state, health }];
}

describe("toBrokerStatus", () => {
  test("maps a healthy running container to running", () => {
    expect(toBrokerStatus(entry("running", "healthy"))).toEqual({ kind: "running" });
  });

  test("maps a container still inside its healthcheck grace period to starting", () => {
    expect(toBrokerStatus(entry("running", "starting"))).toEqual({
      kind: "starting",
      health: "starting",
    });
  });

  test("maps a running container whose healthcheck keeps failing to starting/unhealthy", () => {
    expect(toBrokerStatus(entry("running", "unhealthy"))).toEqual({
      kind: "starting",
      health: "unhealthy",
    });
  });

  /** A compose file without a healthcheck reports an empty Health; running is then
   * all the evidence there is, so it must not be read as "still starting" forever. */
  test("maps a running container with no healthcheck to running", () => {
    expect(toBrokerStatus(entry("running", ""))).toEqual({ kind: "running" });
  });

  test("maps a restarting container to starting", () => {
    expect(toBrokerStatus(entry("restarting"))).toEqual({ kind: "starting", health: "starting" });
  });

  test("maps an exited container to stopped", () => {
    expect(toBrokerStatus(entry("exited"))).toEqual({ kind: "stopped" });
  });

  test("maps a created container to stopped", () => {
    expect(toBrokerStatus(entry("created"))).toEqual({ kind: "stopped" });
  });

  test("maps a dead container to stopped", () => {
    expect(toBrokerStatus(entry("dead"))).toEqual({ kind: "stopped" });
  });

  test("maps a paused container to stopped", () => {
    expect(toBrokerStatus(entry("paused"))).toEqual({ kind: "stopped" });
  });

  test("maps a removing container to stopped", () => {
    expect(toBrokerStatus(entry("removing"))).toEqual({ kind: "stopped" });
  });

  test("maps no containers to absent", () => {
    expect(toBrokerStatus([])).toEqual({ kind: "absent" });
  });

  test("maps output listing only other services to absent", () => {
    const entries = [{ service: "zookeeper", state: "running", health: "healthy" }];

    expect(toBrokerStatus(entries)).toEqual({ kind: "absent" });
  });

  test("maps an unrecognized state to unavailable", () => {
    expect(toBrokerStatus(entry("levitating"))).toEqual({
      kind: "unavailable",
      reason: "unrecognized",
    });
  });

  test("maps unparseable output to unavailable", () => {
    expect(toBrokerStatus(undefined)).toEqual({ kind: "unavailable", reason: "unrecognized" });
  });
});
