import {
  type AdminGroup,
  type AdminSnapshotPayload,
  bigintOffsetToNumber,
  type GroupAssignment,
  type GroupState,
} from "@glassbox/schema";
import type { Admin } from "@platformatic/kafka";

export interface RawTopicPartitionOffsets {
  partitionIndex: number;
  earliestOffset: bigint;
  endOffset: bigint;
}

export interface RawTopicOffsets {
  name: string;
  partitions: RawTopicPartitionOffsets[];
}

export type AdminSnapshot = AdminSnapshotPayload;

/**
 * Pure mapping from raw admin API results (bigint offsets, arbitrary order) to a
 * deterministic AdminSnapshot payload. Sorting makes the output stable so that
 * snapshotsEqual can compare snapshots structurally. `groups` (already mapped via
 * buildAdminGroup) is sorted by groupId for the same reason.
 */
export function buildAdminSnapshot(
  topics: RawTopicOffsets[],
  groups: AdminGroup[] = [],
): AdminSnapshot {
  return {
    topics: [...topics]
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((topic) => ({
        name: topic.name,
        partitions: [...topic.partitions]
          .sort((a, b) => a.partitionIndex - b.partitionIndex)
          .map((partition) => ({
            index: partition.partitionIndex,
            earliestOffset: bigintOffsetToNumber(partition.earliestOffset),
            endOffset: bigintOffsetToNumber(partition.endOffset),
          })),
      })),
    groups: [...groups].sort((a, b) => a.groupId.localeCompare(b.groupId)),
  };
}

/**
 * Structural equality check used to decide whether a new admin.snapshot event is
 * worth publishing. Snapshots produced by buildAdminSnapshot are deterministically
 * ordered, so a JSON comparison is sufficient and avoids writing a bespoke deep-equal.
 */
export function snapshotsEqual(
  previous: AdminSnapshot | undefined,
  current: AdminSnapshot,
): boolean {
  if (previous === undefined) {
    return false;
  }
  return JSON.stringify(previous) === JSON.stringify(current);
}

export type AdminPollerDeps = Pick<
  Admin,
  | "listTopics"
  | "metadata"
  | "listOffsets"
  | "listGroups"
  | "describeGroups"
  | "listConsumerGroupOffsets"
>;

async function buildTopicPartitionIndexes(
  admin: AdminPollerDeps,
  topicNames: string[],
): Promise<Array<{ name: string; partitionIndexes: number[] }>> {
  const metadata = await admin.metadata({ topics: topicNames });
  return topicNames.map((name) => {
    const info = metadata.topics.get(name);
    const partitionsCount = info?.partitionsCount ?? 0;
    return { name, partitionIndexes: Array.from({ length: partitionsCount }, (_, i) => i) };
  });
}

async function fetchTopicOffsets(
  admin: AdminPollerDeps,
  topicPartitionIndexes: Array<{ name: string; partitionIndexes: number[] }>,
): Promise<RawTopicOffsets[]> {
  if (topicPartitionIndexes.length === 0) {
    return [];
  }

  const [earliestResult, endResult] = await Promise.all([
    admin.listOffsets({
      topics: topicPartitionIndexes.map(({ name, partitionIndexes }) => ({
        name,
        partitions: partitionIndexes.map((partitionIndex) => ({ partitionIndex, timestamp: -2n })),
      })),
    }),
    admin.listOffsets({
      topics: topicPartitionIndexes.map(({ name, partitionIndexes }) => ({
        name,
        partitions: partitionIndexes.map((partitionIndex) => ({ partitionIndex, timestamp: -1n })),
      })),
    }),
  ]);

  return topicPartitionIndexes.map(({ name, partitionIndexes }) => {
    const earliestTopic = earliestResult.find((t) => t.name === name);
    const endTopic = endResult.find((t) => t.name === name);

    return {
      name,
      partitions: partitionIndexes.map((partitionIndex) => {
        const earliestPartition = earliestTopic?.partitions.find(
          (p) => p.partitionIndex === partitionIndex,
        );
        const endPartition = endTopic?.partitions.find((p) => p.partitionIndex === partitionIndex);

        return {
          partitionIndex,
          earliestOffset: earliestPartition?.offset ?? 0n,
          endOffset: endPartition?.offset ?? 0n,
        };
      }),
    };
  });
}

function buildTopicEndOffsets(rawOffsets: RawTopicOffsets[]): Map<string, Map<number, bigint>> {
  const map = new Map<string, Map<number, bigint>>();
  for (const topic of rawOffsets) {
    const partitionMap = new Map<number, bigint>();
    for (const partition of topic.partitions) {
      partitionMap.set(partition.partitionIndex, partition.endOffset);
    }
    map.set(topic.name, partitionMap);
  }
  return map;
}

/**
 * Fetches groupId -> members -> committed offsets for every classic consumer group,
 * mapping the result to AdminGroup entries (DEAD groups filtered out). Per the spike
 * (docs/spikes/0001-platformatic-kafka-diagnostics.md), listConsumerGroupOffsets
 * silently returns an empty topics array unless `topics` is passed explicitly with
 * partitionIndexes; this always passes the full topic/partition list polled this tick.
 *
 * describeGroups and listConsumerGroupOffsets are independent RPCs fetched via
 * allSettled (not Promise.all): if one fails (e.g. a transient broker hiccup), the
 * other's data is still used rather than discarding both. Failures are reported via
 * `onError` but never thrown, so a group-RPC failure never blocks this tick's topic
 * offset snapshot (see createAdminPoller's tick(), which calls this in its own
 * try/catch as a second line of defense for a listGroups() failure, which isn't
 * settled individually since it determines which groupIds to even query).
 *
 * `previousGroupsById` (this poller's last successfully-built AdminGroup per groupId)
 * is the fallback when an RPC fails: without it, a transient describeGroups failure
 * would make every group vanish from the snapshot for that tick, then reappear on the
 * next successful poll -- a confusing flicker unrelated to any real broker event (see
 * the Phase 2 review, "Problem B"). Carrying over the previous entry instead means a
 * single failed tick is invisible to the UI; only a *group that's truly gone* (a fresh,
 * successful describeGroups response that no longer lists it) is ever dropped.
 */
async function fetchGroupSnapshots(
  admin: AdminPollerDeps,
  topicPartitionIndexes: Array<{ name: string; partitionIndexes: number[] }>,
  topicEndOffsets: Map<string, Map<number, bigint>>,
  previousGroupsById: Map<string, AdminGroup>,
  onError: (error: unknown) => void,
): Promise<AdminGroup[]> {
  const allGroups = await admin.listGroups({});
  const relevantGroups = [...allGroups.values()].filter(
    (group) => group.protocolType === "consumer",
  );
  const groupIds = relevantGroups.map((group) => group.id);

  if (groupIds.length === 0) {
    return [];
  }

  const [describedResult, offsetsResult] = await Promise.allSettled([
    admin.describeGroups({ groups: groupIds }),
    admin.listConsumerGroupOffsets({
      groups: groupIds.map((groupId) => ({ groupId, topics: topicPartitionIndexes })),
    }),
  ]);

  if (describedResult.status === "rejected") {
    onError(describedResult.reason);
  }
  if (offsetsResult.status === "rejected") {
    onError(offsetsResult.reason);
  }

  return groupIds
    .map((groupId): AdminGroup | undefined => {
      const previous = previousGroupsById.get(groupId);

      if (describedResult.status === "rejected") {
        // Membership/state come exclusively from describeGroups; with no fresh data
        // and no prior data either (a brand-new group whose very first poll failed),
        // there's nothing honest to show yet.
        return previous;
      }

      const described = describedResult.value.get(groupId);
      const listGroupsEntry = relevantGroups.find((group) => group.id === groupId);
      const members: RawGroupMember[] = described
        ? [...described.members.values()].map((member) => ({
            memberId: member.id,
            clientId: member.clientId,
            assignments: [...(member.assignments?.values() ?? [])],
          }))
        : [];

      const offsetsForGroup =
        offsetsResult.status === "fulfilled"
          ? offsetsResult.value.find((g) => g.groupId === groupId)
          : undefined;
      const offsetsByTopic: RawGroupOffsetsTopic[] = (offsetsForGroup?.topics ?? []).map(
        (topic) => ({
          name: topic.name,
          partitions: topic.partitions.map((partition) => ({
            partitionIndex: partition.partitionIndex,
            committedOffset: partition.committedOffset,
          })),
        }),
      );

      const raw: RawGroup = {
        groupId,
        // listGroups() also carries a (redundant) state field; used as a fallback
        // only when describeGroups itself doesn't have this groupId (rare).
        state: described?.state ?? listGroupsEntry?.state ?? "DEAD",
        members,
        offsetsByTopic,
      };

      const built = buildAdminGroup(raw, topicEndOffsets);
      if (built === undefined) {
        // A *fresh, successful* describeGroups response says this group is gone
        // (DEAD/unrecognized state): that's authoritative, not a failure, so it is
        // genuinely dropped rather than falling back to `previous`.
        return undefined;
      }

      if (offsetsResult.status === "rejected") {
        // Freeze the last known committed/lag values rather than showing them as
        // empty: recomputing lag from stale committed offsets against this tick's
        // fresh end offsets would mix data from two different points in time.
        return { ...built, offsets: previous?.offsets ?? built.offsets };
      }

      return built;
    })
    .filter((group): group is AdminGroup => group !== undefined);
}

/**
 * Maps the broker's raw ConsumerGroupStateValue (`describeGroups`/`listGroups`, e.g.
 * "STABLE") onto the UI's 4-state model (ADR-0003). DEAD groups (the group no longer
 * exists) and any unrecognized future state are mapped to undefined, signaling the
 * caller to omit the group from the snapshot entirely.
 */
export function mapBrokerGroupState(rawState: string): GroupState | undefined {
  switch (rawState) {
    case "PREPARING_REBALANCE":
      return "PreparingRebalance";
    case "COMPLETING_REBALANCE":
      return "CompletingRebalance";
    case "STABLE":
      return "Stable";
    case "EMPTY":
      return "Empty";
    default:
      return undefined;
  }
}

export interface RawGroupMember {
  memberId: string;
  clientId: string;
  assignments: GroupAssignment[];
}

export interface RawGroupOffsetsPartition {
  partitionIndex: number;
  committedOffset: bigint;
}

export interface RawGroupOffsetsTopic {
  name: string;
  partitions: RawGroupOffsetsPartition[];
}

export interface RawGroup {
  groupId: string;
  state: string;
  members: RawGroupMember[];
  offsetsByTopic: RawGroupOffsetsTopic[];
}

function buildOffsetEntry(
  topicName: string,
  partitionIndex: number,
  committed: number,
  endOffset: bigint | undefined,
): AdminGroup["offsets"][number] {
  if (endOffset === undefined) {
    // Unknown, not "no lag": omit the field entirely rather than defaulting to 0,
    // which would falsely claim the consumer is fully caught up (see ADR-0003 and the
    // groupOffsetSchema doc in packages/schema/src/kafka-events.ts).
    return { topic: topicName, partition: partitionIndex, committed };
  }
  const end = bigintOffsetToNumber(endOffset);
  return {
    topic: topicName,
    partition: partitionIndex,
    committed,
    // Clamped to >= 0: describeGroups and listConsumerGroupOffsets are separate,
    // non-atomic RPCs, so the raw subtraction can theoretically go negative (ADR-0003).
    lag: Math.max(0, end - committed),
  };
}

/**
 * Pure mapping from raw describeGroups/listConsumerGroupOffsets results to an
 * AdminGroup snapshot entry. `topicEndOffsets` (topic -> partitionIndex -> end offset)
 * comes from the same tick's topic offset poll, so lag can be computed without an
 * extra RPC.
 */
export function buildAdminGroup(
  raw: RawGroup,
  topicEndOffsets: Map<string, Map<number, bigint>>,
): AdminGroup | undefined {
  const state = mapBrokerGroupState(raw.state);
  if (state === undefined) {
    return undefined;
  }

  const offsets = raw.offsetsByTopic
    .flatMap((topic) =>
      topic.partitions.map((partition) => {
        const committed = bigintOffsetToNumber(partition.committedOffset);
        const endOffset = topicEndOffsets.get(topic.name)?.get(partition.partitionIndex);
        return buildOffsetEntry(topic.name, partition.partitionIndex, committed, endOffset);
      }),
    )
    .sort((a, b) =>
      a.topic === b.topic ? a.partition - b.partition : a.topic.localeCompare(b.topic),
    );

  return {
    groupId: raw.groupId,
    state,
    members: [...raw.members].sort((a, b) => a.memberId.localeCompare(b.memberId)),
    offsets,
  };
}

export interface AdminPollerOptions {
  intervalMs: number;
  onSnapshot: (snapshot: AdminSnapshot) => void;
  onError?: (error: unknown) => void;
  /**
   * Called when the broker becomes reachable -- on the *transition* into
   * "connected", not on every successful tick. This is what lets the gateway start
   * without a broker (ADR-0005): work that used to be a blocking boot step (creating
   * the demo topic) hangs off this hook instead, so it also runs again after the
   * broker is stopped and restarted, or after a container recreation wiped the topic.
   * Must therefore be idempotent.
   */
  onReachable?: () => void | Promise<void>;
}

export interface AdminPoller {
  start: () => void;
  stop: () => void;
  kafkaStatus: () => "connected" | "unreachable" | "unknown";
}

/**
 * Polls topic/offset state at a fixed interval (reconciliation only, per ADR-0003;
 * rebalance state itself is client-event driven and added in Phase 2). Publishes a
 * new admin.snapshot only when the snapshot actually changed since the previous tick.
 */
export function createAdminPoller(
  admin: AdminPollerDeps,
  options: AdminPollerOptions,
): AdminPoller {
  let previous: AdminSnapshot | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let stopped = false;
  // Tracks broker reachability, not overall tick success: set only from the
  // topic-RPC path below, deliberately independent of onError/snapshotsEqual and of
  // fetchGroupSnapshots' own group RPCs (a lone describeGroups/listConsumerGroupOffsets
  // failure doesn't mean the broker itself is unreachable -- see this function's doc).
  let lastOutcome: "unknown" | "connected" | "unreachable" = "unknown";

  // onError itself must not be allowed to throw: if it did, tick()'s reschedule would
  // never run and the poller would permanently stop ticking.
  function reportError(error: unknown): void {
    try {
      options.onError?.(error);
    } catch {
      // intentionally swallowed; polling must continue regardless.
    }
  }

  // Same contract as reportError: a throwing onReachable (e.g. createTopics racing
  // another gateway instance) must not break the reschedule below, or the poller
  // would stop ticking and the broker would look permanently unreachable.
  async function reportReachable(): Promise<void> {
    try {
      await options.onReachable?.();
    } catch (error) {
      reportError(error);
    }
  }

  async function tick(): Promise<void> {
    if (stopped) {
      return;
    }

    try {
      const topicNames = await admin.listTopics({ includeInternals: false });
      const topicPartitionIndexes = await buildTopicPartitionIndexes(admin, topicNames);
      const rawOffsets = await fetchTopicOffsets(admin, topicPartitionIndexes);
      const topicEndOffsets = buildTopicEndOffsets(rawOffsets);

      // The topic RPCs above are the reachability signal: reaching this point means
      // the broker responded, regardless of what fetchGroupSnapshots does next.
      const wasReachable = lastOutcome === "connected";
      lastOutcome = "connected";
      if (!wasReachable) {
        await reportReachable();
      }

      // Deliberately isolated from the topic-offset fetch above: a failure fetching
      // consumer group info (e.g. listGroups() itself rejecting) must not prevent
      // this tick's topic offset snapshot from being published. fetchGroupSnapshots
      // itself further isolates its two independent RPCs via allSettled.
      const previousGroupsById = new Map((previous?.groups ?? []).map((g) => [g.groupId, g]));
      // Falls back to the previous tick's groups (not []) so a full listGroups()
      // failure doesn't make every group vanish either (same "Problem B" flicker
      // fetchGroupSnapshots itself guards against for its own two RPCs).
      let groups: AdminGroup[] = previous?.groups ?? [];
      try {
        groups = await fetchGroupSnapshots(
          admin,
          topicPartitionIndexes,
          topicEndOffsets,
          previousGroupsById,
          reportError,
        );
      } catch (error) {
        reportError(error);
      }

      const snapshot = buildAdminSnapshot(rawOffsets, groups);

      if (!snapshotsEqual(previous, snapshot)) {
        previous = snapshot;
        options.onSnapshot(snapshot);
      }
    } catch (error) {
      lastOutcome = "unreachable";
      reportError(error);
    }

    if (!stopped) {
      timer = setTimeout(() => {
        void tick();
      }, options.intervalMs);
    }
  }

  return {
    start() {
      stopped = false;
      void tick();
    },
    stop() {
      stopped = true;
      if (timer) {
        clearTimeout(timer);
        timer = undefined;
      }
    },
    kafkaStatus() {
      return lastOutcome;
    },
  };
}
