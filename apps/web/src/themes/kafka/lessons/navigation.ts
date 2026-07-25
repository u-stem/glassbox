import type { Lesson } from "./types";

/** Clamps a lesson step index into [0, totalSteps - 1] (or 0 for an empty lesson),
 * so LessonPanel's Next/Prev buttons can't walk off either end of the step list. */
export function clampStepIndex(index: number, totalSteps: number): number {
  if (totalSteps <= 0) {
    return 0;
  }
  return Math.min(Math.max(index, 0), totalSteps - 1);
}

/** Resolves a `?lesson=` query param (Next.js's searchParams shape) to a known
 * lesson, for deep-linking straight into a lesson from an external URL. A string[]
 * (a repeated `?lesson=a&lesson=b` query) takes its first element; an unknown id or
 * a missing param both fall through to undefined so the caller can fall back to the
 * theme page's normal (no lesson open) display. */
export function findLessonByParam(
  lessons: readonly Lesson[],
  param: string | string[] | undefined,
): Lesson | undefined {
  const id = Array.isArray(param) ? param[0] : param;
  if (id === undefined) {
    return undefined;
  }
  return lessons.find((lesson) => lesson.id === id);
}

/** Builds a deep-link URL into a lesson, for use from outside the theme page (e.g.
 * the home screen). URLSearchParams form-urlencodes the lesson id (spaces become
 * `+`, not `%20`). */
export function lessonHref(lessonId: string): string {
  const params = new URLSearchParams({ lesson: lessonId });
  return `/themes/kafka?${params.toString()}`;
}
