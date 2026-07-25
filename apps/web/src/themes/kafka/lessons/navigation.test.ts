import { describe, expect, test } from "bun:test";
import { clampStepIndex } from "./navigation";

describe("clampStepIndex", () => {
  test("passes through an in-range index", () => {
    expect(clampStepIndex(2, 5)).toBe(2);
  });

  test("clamps a negative index up to 0", () => {
    expect(clampStepIndex(-1, 5)).toBe(0);
  });

  test("clamps an index past the end down to the last step", () => {
    expect(clampStepIndex(10, 5)).toBe(4);
  });

  test("returns 0 for a lesson with no steps", () => {
    expect(clampStepIndex(3, 0)).toBe(0);
  });
});
