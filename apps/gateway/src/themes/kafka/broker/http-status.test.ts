import { describe, expect, test } from "bun:test";
import { toBrokerHttpStatus } from "./http-status";

describe("toBrokerHttpStatus", () => {
  test("reports a running broker as a successful observation", () => {
    expect(toBrokerHttpStatus({ kind: "running" })).toBe(200);
  });

  test("reports a starting broker as a successful observation", () => {
    expect(toBrokerHttpStatus({ kind: "starting", health: "starting" })).toBe(200);
  });

  /** A stopped broker is a perfectly good answer to "what is the broker doing", not
   * an error -- the UI turns it into a start button. */
  test("reports a stopped broker as a successful observation", () => {
    expect(toBrokerHttpStatus({ kind: "stopped" })).toBe(200);
  });

  test("reports an absent broker as a successful observation", () => {
    expect(toBrokerHttpStatus({ kind: "absent" })).toBe(200);
  });

  test("reports a missing docker CLI as unavailable", () => {
    expect(toBrokerHttpStatus({ kind: "unavailable", reason: "docker-missing" })).toBe(503);
  });

  test("reports a failing docker as unavailable", () => {
    expect(toBrokerHttpStatus({ kind: "unavailable", reason: "docker-failed" })).toBe(503);
  });

  test("reports a missing compose file as unavailable", () => {
    expect(toBrokerHttpStatus({ kind: "unavailable", reason: "compose-missing" })).toBe(503);
  });

  test("reports a docker timeout as a gateway timeout", () => {
    expect(toBrokerHttpStatus({ kind: "unavailable", reason: "docker-timeout" })).toBe(504);
  });

  /** Output we could not parse is our problem, not docker's. */
  test("reports unparseable output as an internal error", () => {
    expect(toBrokerHttpStatus({ kind: "unavailable", reason: "unrecognized" })).toBe(500);
  });
});
