import { type GlassboxEvent, type GroupState, kafkaEventSchema } from "@glassbox/schema";

export interface TransitionHistoryEntry {
  ts: number;
  from: GroupState;
  to: GroupState;
  /**
   * The member-level edge that drove this transition (e.g. "joining", "joined",
   * "left"), per group-state-tracker.ts. Kept as the "what triggered this" label
   * rather than a per-member clientId: group.state.changed is a group-wide,
   * admin-sourced event and does not carry a clientId (see
   * packages/schema/src/kafka-events.ts groupStateChangedEventSchema) -- the edge
   * type is the closest available substitute.
   */
  edge: string;
}

export interface ParsedTransition {
  groupId: string;
  entry: TransitionHistoryEntry;
}

/**
 * Parses a single event into a (groupId, TransitionHistoryEntry) pair if it's a
 * group.state.changed event, or undefined otherwise. O(1) per event -- the store
 * calls this from addEvent to maintain each group's history incrementally rather
 * than re-scanning the entire event timeline on every render (see lib/store.ts's
 * transitionHistoryByGroup; the previous full-scan extractTransitionHistory was
 * O(events.length) on every event, flagged in the Phase 3 review).
 */
export function parseTransition(event: GlassboxEvent): ParsedTransition | undefined {
  if (event.type !== "group.state.changed") {
    return undefined;
  }
  const parsed = kafkaEventSchema.safeParse({ type: event.type, payload: event.payload });
  if (!parsed.success || parsed.data.type !== "group.state.changed") {
    return undefined;
  }
  const { groupId, from, to, edge } = parsed.data.payload;
  return { groupId, entry: { ts: event.ts, from, to, edge } };
}
