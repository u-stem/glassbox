import type { GlassboxEvent } from "@glassbox/schema";

export interface TimelineFilter {
  /** Empty string means "no type filter" (matches everything). */
  type: string;
  /** Empty string means "no actor filter" (matches everything). */
  actorId: string;
}

export const EMPTY_TIMELINE_FILTER: TimelineFilter = { type: "", actorId: "" };

/**
 * Pure filter predicate for the event timeline, extracted so the filter row's
 * logic can be unit-tested without rendering the (virtualized) list.
 */
export function filterEvents(
  events: readonly GlassboxEvent[],
  filter: TimelineFilter,
): GlassboxEvent[] {
  return events.filter((event) => {
    if (filter.type !== "" && event.type !== filter.type) {
      return false;
    }
    if (filter.actorId !== "" && event.source.actorId !== filter.actorId) {
      return false;
    }
    return true;
  });
}
