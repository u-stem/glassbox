import { execFile } from "node:child_process";

export interface DockerCommandResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

/** Distinguishes "docker ran and said something" from the ways it can fail to run at
 * all, because those map onto different things for the UI to show: a non-zero exit
 * carries docker's own message, a missing CLI needs the setup instructions instead. */
export type DockerCommandOutcome =
  | { kind: "completed"; result: DockerCommandResult }
  | { kind: "not-found" }
  | { kind: "timeout" }
  | { kind: "spawn-error"; message: string };

export type DockerRunner = (
  args: readonly string[],
  options: { timeoutMs: number },
) => Promise<DockerCommandOutcome>;

export type ExecFileErrorKind = "not-found" | "timeout" | "exited" | "spawn-error";

/**
 * Classifies the error object execFile hands back. Written as a type guard chain
 * rather than a cast: the value is `unknown` at this boundary, and its `code` is a
 * string for spawn failures but a number for a non-zero exit.
 */
export function classifyExecFileError(error: unknown): ExecFileErrorKind {
  if (typeof error !== "object" || error === null) {
    return "spawn-error";
  }
  // Checked before the exit code: a child killed by the timeout also reports an exit
  // code, and reading that as a normal exit would hide the timeout entirely.
  if ("killed" in error && error.killed === true) {
    return "timeout";
  }
  if (!("code" in error)) {
    return "spawn-error";
  }
  if (error.code === "ENOENT") {
    return "not-found";
  }
  if (typeof error.code === "number") {
    return "exited";
  }
  return "spawn-error";
}

function readExitCode(error: unknown): number {
  if (typeof error === "object" && error !== null && "code" in error) {
    if (typeof error.code === "number") {
      return error.code;
    }
  }
  return 1;
}

function readMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** 1 MiB: `compose ps --format json` for a single service is a few hundred bytes, so
 * this is only a guard against a pathological output, never a real limit. */
const MAX_OUTPUT_BYTES = 1_048_576;

/**
 * Runs `docker` with a fixed argument vector and no shell (ADR-0005). `shell: false`
 * is execFile's default but is stated explicitly: it is the property that keeps this
 * from ever becoming arbitrary command execution.
 */
export function createExecFileDockerRunner(): DockerRunner {
  return (args, options) =>
    new Promise((resolve) => {
      execFile(
        "docker",
        [...args],
        {
          timeout: options.timeoutMs,
          maxBuffer: MAX_OUTPUT_BYTES,
          shell: false,
          windowsHide: true,
        },
        (error, stdout, stderr) => {
          if (error === null) {
            resolve({ kind: "completed", result: { exitCode: 0, stdout, stderr } });
            return;
          }
          switch (classifyExecFileError(error)) {
            case "not-found":
              resolve({ kind: "not-found" });
              return;
            case "timeout":
              resolve({ kind: "timeout" });
              return;
            case "exited":
              resolve({
                kind: "completed",
                result: { exitCode: readExitCode(error), stdout, stderr },
              });
              return;
            case "spawn-error":
              resolve({ kind: "spawn-error", message: readMessage(error) });
              return;
          }
        },
      );
    });
}
