import { describe, expect, test } from "bun:test";
import { createBrokerController } from "./controller";
import type { DockerCommandOutcome, DockerRunner } from "./docker-cli";

const COMPOSE_PATH = "/repo/docker/compose.yaml";

function completed(stdout: string, exitCode = 0, stderr = ""): DockerCommandOutcome {
  return { kind: "completed", result: { exitCode, stdout, stderr } };
}

const RUNNING_PS = JSON.stringify({ Service: "kafka", State: "running", Health: "healthy" });

interface RecordedCall {
  args: readonly string[];
  timeoutMs: number;
}

/** Replays the given outcomes in order, repeating the last one once exhausted, and
 * records every invocation so ordering and timeouts can be asserted. */
function makeRunner(outcomes: DockerCommandOutcome[]): {
  run: DockerRunner;
  calls: RecordedCall[];
} {
  const calls: RecordedCall[] = [];
  let index = 0;
  const run: DockerRunner = async (args, options) => {
    calls.push({ args, timeoutMs: options.timeoutMs });
    const outcome = outcomes[Math.min(index, outcomes.length - 1)];
    index += 1;
    return outcome ?? completed("");
  };
  return { run, calls };
}

function foundPath(): { found: true; path: string } {
  return { found: true, path: COMPOSE_PATH };
}

describe("createBrokerController status", () => {
  test("reports a running broker", async () => {
    const { run } = makeRunner([completed(RUNNING_PS)]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    const outcome = await controller.status();

    expect(outcome.broker).toEqual({ kind: "running" });
  });

  test("reports an absent broker when compose lists no container", async () => {
    const { run } = makeRunner([completed("")]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    const outcome = await controller.status();

    expect(outcome.broker).toEqual({ kind: "absent" });
  });

  test("reports a missing docker CLI", async () => {
    const { run } = makeRunner([{ kind: "not-found" }]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    const outcome = await controller.status();

    expect(outcome.broker).toEqual({ kind: "unavailable", reason: "docker-missing" });
  });

  test("reports a timed-out docker invocation", async () => {
    const { run } = makeRunner([{ kind: "timeout" }]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    const outcome = await controller.status();

    expect(outcome.broker).toEqual({ kind: "unavailable", reason: "docker-timeout" });
  });

  test("reports a non-zero docker exit", async () => {
    const { run } = makeRunner([completed("", 1, "Cannot connect to the Docker daemon")]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    const outcome = await controller.status();

    expect(outcome.broker).toEqual({ kind: "unavailable", reason: "docker-failed" });
  });

  test("carries docker's own error text as the detail", async () => {
    const { run } = makeRunner([completed("", 1, "Cannot connect to the Docker daemon")]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    const outcome = await controller.status();

    expect(outcome.detail).toBe("Cannot connect to the Docker daemon");
  });

  test("truncates an overlong detail", async () => {
    const { run } = makeRunner([completed("", 1, "x".repeat(2000))]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    const outcome = await controller.status();

    expect(outcome.detail?.length).toBeLessThanOrEqual(500);
  });

  test("reports a missing compose file", async () => {
    const { run } = makeRunner([completed(RUNNING_PS)]);
    const controller = createBrokerController({ run, composePath: { found: false } });

    const outcome = await controller.status();

    expect(outcome.broker).toEqual({ kind: "unavailable", reason: "compose-missing" });
  });

  test("does not invoke docker at all when the compose file is missing", async () => {
    const { run, calls } = makeRunner([completed(RUNNING_PS)]);
    const controller = createBrokerController({ run, composePath: { found: false } });

    await controller.status();

    expect(calls).toEqual([]);
  });

  test("uses the status timeout", async () => {
    const { run, calls } = makeRunner([completed(RUNNING_PS)]);
    const controller = createBrokerController({
      run,
      composePath: foundPath(),
      timeouts: { statusMs: 1234 },
    });

    await controller.status();

    expect(calls[0]?.timeoutMs).toBe(1234);
  });
});

describe("createBrokerController apply", () => {
  test("starts the broker before observing its state", async () => {
    const { run, calls } = makeRunner([completed(""), completed(RUNNING_PS)]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    await controller.apply("start");

    expect(calls.map((call) => call.args[3])).toEqual(["up", "ps"]);
  });

  test("stops the broker before observing its state", async () => {
    const { run, calls } = makeRunner([completed(""), completed("")]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    await controller.apply("stop");

    expect(calls.map((call) => call.args[3])).toEqual(["stop", "ps"]);
  });

  test("returns the state observed after the action", async () => {
    const { run } = makeRunner([completed(""), completed(RUNNING_PS)]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    const result = await controller.apply("start");

    expect(result).toEqual({ status: "ok", outcome: { broker: { kind: "running" } } });
  });

  /** A failed `up` is not the end of the story: the container may well be running
   * already, and showing what is actually there beats showing only the error. */
  test("still observes the state when the action itself failed", async () => {
    const { run } = makeRunner([
      completed("", 1, "port is already allocated"),
      completed(RUNNING_PS),
    ]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    const result = await controller.apply("start");

    expect(result.status === "ok" && result.outcome.broker).toEqual({ kind: "running" });
  });

  test("surfaces the failed action's error text", async () => {
    const { run } = makeRunner([
      completed("", 1, "port is already allocated"),
      completed(RUNNING_PS),
    ]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    const result = await controller.apply("start");

    expect(result.status === "ok" && result.outcome.detail).toBe("port is already allocated");
  });

  test("uses the start timeout for starting", async () => {
    const { run, calls } = makeRunner([completed(""), completed(RUNNING_PS)]);
    const controller = createBrokerController({
      run,
      composePath: foundPath(),
      timeouts: { startMs: 4321 },
    });

    await controller.apply("start");

    expect(calls[0]?.timeoutMs).toBe(4321);
  });

  test("uses the stop timeout for stopping", async () => {
    const { run, calls } = makeRunner([completed(""), completed("")]);
    const controller = createBrokerController({
      run,
      composePath: foundPath(),
      timeouts: { stopMs: 5678 },
    });

    await controller.apply("stop");

    expect(calls[0]?.timeoutMs).toBe(5678);
  });

  /** Button mashing must not launch several `docker compose up` at once. */
  test("rejects a second action while one is still running", async () => {
    let release = (): void => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const run: DockerRunner = async () => {
      await gate;
      return completed(RUNNING_PS);
    };
    const controller = createBrokerController({ run, composePath: foundPath() });

    const first = controller.apply("start");
    const second = await controller.apply("start");
    release();
    await first;

    expect(second).toEqual({ status: "conflict" });
  });

  test("accepts a new action once the previous one finished", async () => {
    const { run } = makeRunner([completed(""), completed(RUNNING_PS)]);
    const controller = createBrokerController({ run, composePath: foundPath() });

    await controller.apply("start");
    const second = await controller.apply("stop");

    expect(second.status).toBe("ok");
  });
});
