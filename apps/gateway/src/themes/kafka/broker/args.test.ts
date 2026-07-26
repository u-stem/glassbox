import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { buildActionArgs, buildStatusArgs } from "./args";

const COMPOSE_PATH = "/repo/docker/compose.yaml";

describe("buildStatusArgs", () => {
  test("scopes the query to the compose file", () => {
    expect(buildStatusArgs(COMPOSE_PATH).slice(0, 3)).toEqual(["compose", "-f", COMPOSE_PATH]);
  });

  /** Without -a a stopped container is simply absent from the output, which would be
   * indistinguishable from "never created". */
  test("includes stopped containers", () => {
    expect(buildStatusArgs(COMPOSE_PATH)).toContain("-a");
  });

  test("asks for machine-readable output", () => {
    expect(buildStatusArgs(COMPOSE_PATH).join(" ")).toContain("--format json");
  });

  test("scopes the query to the kafka service", () => {
    expect(buildStatusArgs(COMPOSE_PATH).at(-1)).toBe("kafka");
  });
});

describe("buildActionArgs", () => {
  /** `docker compose start` fails when the container was never created, so `up` is
   * the only single command covering both absent and stopped. */
  test("starts through up so a never-created container is also covered", () => {
    expect(buildActionArgs(COMPOSE_PATH, "start")).toEqual([
      "compose",
      "-f",
      COMPOSE_PATH,
      "up",
      "-d",
      "--no-recreate",
      "kafka",
    ]);
  });

  test("stops without removing the container", () => {
    expect(buildActionArgs(COMPOSE_PATH, "stop")).toEqual([
      "compose",
      "-f",
      COMPOSE_PATH,
      "stop",
      "kafka",
    ]);
  });

  /** ADR-0005: this API never removes containers -- the broker's log dirs live in
   * anonymous volumes that `down` would take with it. */
  test("never tears the container down", () => {
    const everyArg = [
      ...buildStatusArgs(COMPOSE_PATH),
      ...buildActionArgs(COMPOSE_PATH, "start"),
      ...buildActionArgs(COMPOSE_PATH, "stop"),
    ];

    expect(everyArg).not.toContain("down");
  });

  test("never removes volumes", () => {
    const everyArg = [
      ...buildStatusArgs(COMPOSE_PATH),
      ...buildActionArgs(COMPOSE_PATH, "start"),
      ...buildActionArgs(COMPOSE_PATH, "stop"),
    ];

    expect(everyArg.filter((arg) => arg === "-v" || arg === "--volumes")).toEqual([]);
  });

  /** compose derives the project name from the compose file's parent directory.
   * Passing -p would address a *different* project than the README's CLI steps do,
   * so the UI and the terminal would operate on two separate brokers. */
  test("never overrides the compose project name", () => {
    const everyArg = [
      ...buildStatusArgs(COMPOSE_PATH),
      ...buildActionArgs(COMPOSE_PATH, "start"),
      ...buildActionArgs(COMPOSE_PATH, "stop"),
    ];

    expect(everyArg).not.toContain("-p");
  });
});

const repoRoot = fileURLToPath(new URL("../../../../../../", import.meta.url));
const readme = readFileSync(`${repoRoot}README.md`, "utf-8");

/** Same drift guard as setup-commands.test.ts, from the other side: the button and
 * the documented command must keep addressing the same containers. */
describe("parity with the README's documented command", () => {
  test("the start command extends the one documented in README.md", () => {
    const command = `docker ${buildActionArgs("docker/compose.yaml", "start").join(" ")}`;

    expect(command.startsWith("docker compose -f docker/compose.yaml up -d")).toBe(true);
  });

  test("README.md still documents that command", () => {
    expect(readme.includes("docker compose -f docker/compose.yaml up -d")).toBe(true);
  });
});
