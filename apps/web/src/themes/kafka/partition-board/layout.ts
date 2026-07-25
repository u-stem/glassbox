import type { AdminSnapshotPayload } from "@glassbox/schema";
import type { WorldGroup } from "../world-reducer";

export interface OffsetWindow {
  start: number;
  end: number;
}

/**
 * Auto-following trailing window: follows the highest log-end-offset across a
 * topic's partitions so the board always shows the tail of the log, but never
 * shrinks below `windowSize` wide (so early, low-traffic state still reads as a
 * window rather than a single collapsed point).
 */
export function computeWindow(endOffsets: readonly number[], windowSize: number): OffsetWindow {
  const maxEnd = endOffsets.length === 0 ? 0 : Math.max(...endOffsets);
  const end = Math.max(maxEnd, windowSize);
  const start = Math.max(0, end - windowSize);
  return { start, end };
}

export interface CommittedMarker {
  groupId: string;
  committed: number;
  /** undefined when the group's lag for this partition is unknown this tick --
   * rendered as "--" rather than assumed to be 0 (see world-reducer.ts). */
  lag: number | undefined;
}

export interface PartitionRow {
  partition: number;
  end: number;
  committedByGroup: CommittedMarker[];
}

/**
 * Projects one topic's partitions plus every known group's offsets for that topic
 * into per-partition rows for the board. Pure so the SVG layout can be tested
 * without rendering.
 */
export function buildPartitionRows(
  topic: AdminSnapshotPayload["topics"][number],
  groups: readonly WorldGroup[],
): PartitionRow[] {
  return topic.partitions.map((partition) => ({
    partition: partition.index,
    end: partition.endOffset,
    committedByGroup: groups
      .map((group) => {
        const offset = group.offsets.find(
          (o) => o.topic === topic.name && o.partition === partition.index,
        );
        return offset === undefined
          ? undefined
          : { groupId: group.groupId, committed: offset.committed, lag: offset.lag };
      })
      .filter((marker): marker is CommittedMarker => marker !== undefined),
  }));
}

export interface LagBand {
  from: number;
  to: number;
}

/**
 * The visible gap between a committed offset and the log end offset -- clamped to
 * the current window's start so a committed offset far behind the window doesn't
 * draw a band running off the left edge.
 */
export function computeLagBand(
  committed: number,
  end: number,
  windowStart: number,
): LagBand | undefined {
  const from = Math.max(committed, windowStart);
  if (from >= end) {
    return undefined;
  }
  return { from, to: end };
}
