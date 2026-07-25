/** Clamps a lesson step index into [0, totalSteps - 1] (or 0 for an empty lesson),
 * so LessonPanel's Next/Prev buttons can't walk off either end of the step list. */
export function clampStepIndex(index: number, totalSteps: number): number {
  if (totalSteps <= 0) {
    return 0;
  }
  return Math.min(Math.max(index, 0), totalSteps - 1);
}
