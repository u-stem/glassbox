import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { BROKER_SETUP_COMMAND, GATEWAY_SETUP_COMMAND } from "./setup-commands";

const repoRoot = fileURLToPath(new URL("../../../../", import.meta.url));
const readme = readFileSync(`${repoRoot}README.md`, "utf-8");

describe("setup command constants", () => {
  test("BROKER_SETUP_COMMAND matches the command in README.md", () => {
    expect(readme.includes(BROKER_SETUP_COMMAND)).toBe(true);
  });

  test("GATEWAY_SETUP_COMMAND matches the command in README.md", () => {
    expect(readme.includes(GATEWAY_SETUP_COMMAND)).toBe(true);
  });
});
