import { type AdminSnapshotPayload, type GlassboxEvent, kafkaEventSchema } from "@glassbox/schema";
import { create } from "zustand";
import {
  parseTransition,
  type TransitionHistoryEntry,
} from "../themes/kafka/rebalance-diagram/history";
import { createInitialWorld, type KafkaWorld, reduceWorld } from "../themes/kafka/world-reducer";

const MAX_EVENTS = 2000;
/** Per-group cap for transitionHistoryByGroup, matching the rebalance diagram's
 * display limit (RebalanceDiagram shows the most recent entries only). */
const MAX_TRANSITION_HISTORY_PER_GROUP = 10;

function extractAdminSnapshot(event: GlassboxEvent): AdminSnapshotPayload | undefined {
  if (event.type !== "admin.snapshot") {
    return undefined;
  }
  const result = kafkaEventSchema.safeParse({ type: event.type, payload: event.payload });
  if (!result.success || result.data.type !== "admin.snapshot") {
    return undefined;
  }
  return result.data.payload;
}

/**
 * Prepends a new transition entry for its group, capped, without touching the
 * `Record` entries for every other group -- O(1) (bounded by the per-group cap) per
 * event, not O(events.length). Returns the same `byGroup` reference when the event
 * isn't a group.state.changed, so components selecting this slice don't re-render for
 * unrelated events (see the Phase 3 review: the previous approach re-scanned the
 * entire event timeline in a page-level useMemo on every single event).
 */
function withTransition(
  byGroup: Record<string, TransitionHistoryEntry[]>,
  event: GlassboxEvent,
): Record<string, TransitionHistoryEntry[]> {
  const parsed = parseTransition(event);
  if (parsed === undefined) {
    return byGroup;
  }
  const existing = byGroup[parsed.groupId] ?? [];
  return {
    ...byGroup,
    [parsed.groupId]: [parsed.entry, ...existing].slice(0, MAX_TRANSITION_HISTORY_PER_GROUP),
  };
}

export interface KafkaStoreState {
  events: GlassboxEvent[];
  latestSnapshot: AdminSnapshotPayload | undefined;
  world: KafkaWorld;
  /** Newest-first, capped per group. Maintained incrementally in addEvent (see
   * withTransition) rather than derived from `events` on read, so RebalanceDiagram
   * can select just its own group's history without depending on the full event
   * timeline. */
  transitionHistoryByGroup: Record<string, TransitionHistoryEntry[]>;
  addEvent: (event: GlassboxEvent) => void;
  reset: () => void;
}

/**
 * Holds the raw event timeline (newest first, capped) plus the most recent
 * admin.snapshot payload, which is the authoritative baseline per ADR-0003.
 *
 * reset() is expected to run to completion before any backlog events resulting from
 * that same reset are re-added via addEvent(). This holds because sse.ts drives both
 * calls from a single SSE connection's message events, which arrive strictly in
 * order (control.reset first, then the resent backlog) and are handled synchronously
 * one at a time; there is no concurrent writer that could interleave them.
 */
export const useKafkaStore = create<KafkaStoreState>((set) => ({
  events: [],
  latestSnapshot: undefined,
  world: createInitialWorld(),
  transitionHistoryByGroup: {},
  addEvent: (event) =>
    set((state) => ({
      events: [event, ...state.events].slice(0, MAX_EVENTS),
      latestSnapshot: extractAdminSnapshot(event) ?? state.latestSnapshot,
      world: reduceWorld(state.world, event),
      transitionHistoryByGroup: withTransition(state.transitionHistoryByGroup, event),
    })),
  reset: () =>
    set({
      events: [],
      latestSnapshot: undefined,
      world: createInitialWorld(),
      transitionHistoryByGroup: {},
    }),
}));
