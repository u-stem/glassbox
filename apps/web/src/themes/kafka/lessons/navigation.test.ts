import { describe, expect, test } from "bun:test";
import { clampStepIndex, findLessonByParam, lessonHref } from "./navigation";
import type { Lesson } from "./types";

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

const lessonA: Lesson = { id: "lesson-a", title: "Lesson A", summary: "", steps: [] };
const lessonB: Lesson = { id: "lesson-b", title: "Lesson B", summary: "", steps: [] };
const testLessons: readonly Lesson[] = [lessonA, lessonB];

describe("findLessonByParam", () => {
  test("finds a lesson matching a known id", () => {
    expect(findLessonByParam(testLessons, "lesson-b")).toBe(lessonB);
  });

  test("returns undefined for an unknown id", () => {
    expect(findLessonByParam(testLessons, "no-such-lesson")).toBeUndefined();
  });

  test("returns undefined when the param is undefined", () => {
    expect(findLessonByParam(testLessons, undefined)).toBeUndefined();
  });

  test("uses the first element when the param is a string array", () => {
    expect(findLessonByParam(testLessons, ["lesson-a", "lesson-b"])).toBe(lessonA);
  });
});

describe("lessonHref", () => {
  test("builds a URL-encoded query string for the lesson id", () => {
    expect(lessonHref("consumer-group-rebalance")).toBe(
      "/themes/kafka?lesson=consumer-group-rebalance",
    );
  });

  test("form-urlencodes characters that need encoding in the lesson id", () => {
    expect(lessonHref("id with space")).toBe("/themes/kafka?lesson=id+with+space");
  });
});
