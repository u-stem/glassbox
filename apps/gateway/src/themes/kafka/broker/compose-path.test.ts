import { describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { DEFAULT_COMPOSE_PATH, resolveComposePath } from "./compose-path";

const alwaysExists = () => true;
const neverExists = () => false;

describe("DEFAULT_COMPOSE_PATH", () => {
  /** Resolved from this module's own location rather than process.cwd(), which
   * differs between `tsx watch` (apps/gateway) and `bun test` (repo root). */
  test("points at the compose file that actually ships with the repo", () => {
    expect(existsSync(DEFAULT_COMPOSE_PATH)).toBe(true);
  });
});

describe("resolveComposePath", () => {
  test("falls back to the repo's compose file when nothing is configured", () => {
    const result = resolveComposePath({ exists: alwaysExists });

    expect(result).toEqual({ found: true, path: DEFAULT_COMPOSE_PATH });
  });

  test("prefers an explicitly configured compose file", () => {
    const result = resolveComposePath({
      configuredPath: "/elsewhere/compose.yaml",
      exists: alwaysExists,
    });

    expect(result).toEqual({ found: true, path: "/elsewhere/compose.yaml" });
  });

  test("reports a configured compose file that is not there", () => {
    const result = resolveComposePath({
      configuredPath: "/elsewhere/compose.yaml",
      exists: neverExists,
    });

    expect(result).toEqual({ found: false });
  });

  test("reports a missing default compose file rather than running docker against it", () => {
    const result = resolveComposePath({ exists: neverExists });

    expect(result).toEqual({ found: false });
  });

  test("ignores an empty configured value", () => {
    const result = resolveComposePath({ configuredPath: "", exists: alwaysExists });

    expect(result).toEqual({ found: true, path: DEFAULT_COMPOSE_PATH });
  });
});
