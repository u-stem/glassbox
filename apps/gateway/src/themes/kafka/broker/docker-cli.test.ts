import { describe, expect, test } from "bun:test";
import { classifyExecFileError } from "./docker-cli";

describe("classifyExecFileError", () => {
  test("recognizes a missing docker CLI", () => {
    expect(classifyExecFileError({ code: "ENOENT" })).toBe("not-found");
  });

  /** execFile reports a timeout by killing the child, not through an error code. */
  test("recognizes a killed child as a timeout", () => {
    expect(classifyExecFileError({ killed: true, signal: "SIGTERM" })).toBe("timeout");
  });

  test("recognizes a non-zero exit as a completed run", () => {
    expect(classifyExecFileError({ code: 1 })).toBe("exited");
  });

  test("treats an unrecognized failure as a spawn error", () => {
    expect(classifyExecFileError({ code: "EACCES" })).toBe("spawn-error");
  });

  test("treats a non-object error as a spawn error", () => {
    expect(classifyExecFileError("boom")).toBe("spawn-error");
  });

  test("treats null as a spawn error", () => {
    expect(classifyExecFileError(null)).toBe("spawn-error");
  });

  /** A killed child takes precedence: docker also reports an exit code when it is
   * terminated, and reading that as a normal exit would hide the timeout. */
  test("prefers timeout over exit code when the child was killed", () => {
    expect(classifyExecFileError({ code: 143, killed: true })).toBe("timeout");
  });
});
