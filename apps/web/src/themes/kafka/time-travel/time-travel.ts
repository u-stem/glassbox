import type { GlassboxEvent } from "@glassbox/schema";
import { parseTransition, type TransitionHistoryEntry } from "../rebalance-diagram/history";
import { createInitialWorld, type KafkaWorld, reduceWorld } from "../world-reducer";

/** Recompute a full checkpoint every this many events, so worldAtSeq only ever
 * replays at most CHECKPOINT_INTERVAL events from the nearest earlier checkpoint
 * instead of reducing the whole buffer (up to MAX_EVENTS, see lib/store.ts) on every
 * scrub frame. */
const CHECKPOINT_INTERVAL = 200;

export interface TimeTravelIndex {
  /** Ascending by seq (oldest first) -- lib/store.ts's `events` is newest-first;
   * this is the order reduceWorld expects to replay in. */
  ascendingEvents: readonly GlassboxEvent[];
  /** checkpoints[k] is the world after applying ascendingEvents[0 .. k *
   * CHECKPOINT_INTERVAL - 1]; checkpoints[0] is always the initial (empty) world. */
  checkpoints: readonly KafkaWorld[];
}

/**
 * Builds a scrub index from the store's event buffer. Pure and memoization-friendly:
 * the caller (TimeTravelBar) builds this once per time-travel session (see its doc)
 * rather than on every scrub frame.
 */
export function buildTimeTravelIndex(events: readonly GlassboxEvent[]): TimeTravelIndex {
  const ascendingEvents = [...events].sort((a, b) => a.seq - b.seq);
  const checkpoints: KafkaWorld[] = [createInitialWorld()];
  let world = createInitialWorld();
  for (let i = 0; i < ascendingEvents.length; i += 1) {
    const event = ascendingEvents[i];
    if (event === undefined) {
      continue;
    }
    world = reduceWorld(world, event);
    if ((i + 1) % CHECKPOINT_INTERVAL === 0) {
      checkpoints.push(world);
    }
  }
  return { ascendingEvents, checkpoints };
}

/** Index of the last event with `seq <= seq` (binary search, ascendingEvents is
 * sorted by seq), or -1 if every event is newer than `seq`. */
function findEndIndex(ascendingEvents: readonly GlassboxEvent[], seq: number): number {
  let lo = 0;
  let hi = ascendingEvents.length - 1;
  let result = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const event = ascendingEvents[mid];
    if (event !== undefined && event.seq <= seq) {
      result = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return result;
}

/**
 * Reconstructs the world as of the given seq (inclusive), replaying only from the
 * nearest earlier checkpoint rather than from the very start.
 */
export function worldAtSeq(index: TimeTravelIndex, seq: number): KafkaWorld {
  const endIndex = findEndIndex(index.ascendingEvents, seq);
  if (endIndex === -1) {
    return createInitialWorld();
  }
  const eventsIncluded = endIndex + 1;
  const checkpointIndex = Math.floor(eventsIncluded / CHECKPOINT_INTERVAL);
  const replayStart = checkpointIndex * CHECKPOINT_INTERVAL;
  let world = index.checkpoints[checkpointIndex] ?? createInitialWorld();
  for (let i = replayStart; i <= endIndex; i += 1) {
    const event = index.ascendingEvents[i];
    if (event !== undefined) {
      world = reduceWorld(world, event);
    }
  }
  return world;
}

/**
 * Rebalance-diagram history (see rebalance-diagram/history.ts) as of the given seq,
 * newest-first and capped -- mirrors lib/store.ts's withTransition, but derived for
 * an arbitrary past point instead of maintained incrementally for "now".
 */
export function transitionHistoryAtSeq(
  ascendingEvents: readonly GlassboxEvent[],
  seq: number,
  groupId: string,
  cap: number,
): TransitionHistoryEntry[] {
  const entries: TransitionHistoryEntry[] = [];
  for (const event of ascendingEvents) {
    if (event.seq > seq) {
      break;
    }
    const parsed = parseTransition(event);
    if (parsed !== undefined && parsed.groupId === groupId) {
      entries.unshift(parsed.entry);
      if (entries.length > cap) {
        entries.pop();
      }
    }
  }
  return entries;
}

/** `undefined` when there are no events yet (nothing to scrub). Does not assume its
 * input is sorted, unlike the other functions here (TimeTravelBar calls this with
 * either the raw store buffer or an index's ascendingEvents). */
export function seqRange(
  events: readonly GlassboxEvent[],
): { min: number; max: number } | undefined {
  if (events.length === 0) {
    return undefined;
  }
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  for (const event of events) {
    min = Math.min(min, event.seq);
    max = Math.max(max, event.seq);
  }
  return { min, max };
}
