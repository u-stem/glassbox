import {
  type AdminSnapshotPayload,
  type GlassboxEvent,
  type GroupAssignment,
  type GroupState,
  kafkaEventSchema,
} from "@glassbox/schema";

export interface WorldGroupMember {
  memberId: string;
  clientId: string;
  assignments: GroupAssignment[];
  /**
   * "lost": this gateway has severed the connection (kill scenario), but the broker
   * itself still believes the member is alive until its session times out (see
   * ADR-0003 / consumer.connection.lost's doc). Kept in the member list rather than
   * removed so the UI can show this transient, honest state instead of either hiding
   * it or flickering the member away-and-back across admin.snapshot polls.
   */
  liveness: "alive" | "lost";
}

export interface WorldGroupOffset {
  topic: string;
  partition: number;
  committed: number;
  /** Undefined when the end offset was unknown that tick -- distinct from "no lag"
   * (0); the UI renders this as "--" rather than assuming a value. */
  lag: number | undefined;
}

export interface WorldGroup {
  groupId: string;
  state: GroupState;
  members: WorldGroupMember[];
  offsets: WorldGroupOffset[];
}

export interface KafkaWorld {
  topics: AdminSnapshotPayload["topics"];
  groups: Record<string, WorldGroup>;
  /**
   * ts of the most recently-applied admin.snapshot, or 0 if none has arrived yet.
   * Per ADR-0003, this is the authority boundary: any event whose own ts is older
   * than this must not be allowed to regress state the snapshot already established.
   */
  snapshotFetchedAt: number;
  /**
   * Per-group { state, ts } as last established by a group.state.changed event, kept
   * separately from `groups[groupId].state` so admin.snapshot can tell "have we ever
   * seen a live transition for this group, and how recently" apart from "what does the
   * snapshot itself say". Per ADR-0003, group.state.changed is edge-triggered off
   * client-observed protocol events and is the intended source of truth for `state`
   * specifically; the admin-poller's own state field reflects a ~1s poll tick of the
   * broker's DescribeGroups result, which can lag behind a transition that fired only
   * moments earlier -- comparing raw event timestamps doesn't catch this (the poll's
   * own ts can be newer than the transition while its polled value is still stale, see
   * the Phase 3 review). So: within STATE_RECONCILE_GRACE_MS of the last transition,
   * the transition's `to` wins over the snapshot's state unconditionally; once that
   * grace window has elapsed, the snapshot's state is trusted again (self-healing if a
   * transition was ever missed, e.g. by a collector bug, rather than leaving the UI
   * stuck on a stale state forever). Membership and offsets are unaffected and keep
   * following the snapshot unconditionally.
   */
  groupStateFromTransition: Record<string, { state: GroupState; ts: number }>;
}

/** See KafkaWorld.groupStateFromTransition's doc. Set well above the admin-poller's
 * default 1s interval (env.ADMIN_POLL_INTERVAL_MS) so a single racy tick can't win,
 * but short enough that a genuinely stuck local transition self-heals within a couple
 * of poll cycles instead of lingering indefinitely. */
const STATE_RECONCILE_GRACE_MS = 2000;

export function createInitialWorld(): KafkaWorld {
  return { topics: [], groups: {}, snapshotFetchedAt: 0, groupStateFromTransition: {} };
}

function upsertGroup(
  world: KafkaWorld,
  groupId: string,
  update: (existing: WorldGroup) => WorldGroup,
  createDefault: () => WorldGroup,
): KafkaWorld {
  const existing = world.groups[groupId];
  const next = existing === undefined ? update(createDefault()) : update(existing);
  return { ...world, groups: { ...world.groups, [groupId]: next } };
}

/**
 * Pure reducer implementing the authority model from ADR-0003: admin.snapshot is the
 * authoritative baseline for membership/offsets (never regressed by an
 * older-than-baseline event); consumer.group.* / group.state.changed are an
 * optimistic overlay applied on top until the next snapshot reconciles them.
 * Re-validates payload via kafkaEventSchema (payload is `unknown` at the envelope
 * boundary) rather than casting.
 */
export function reduceWorld(world: KafkaWorld, event: GlassboxEvent): KafkaWorld {
  const parsed = kafkaEventSchema.safeParse({ type: event.type, payload: event.payload });
  if (!parsed.success) {
    return world;
  }
  const data = parsed.data;

  switch (data.type) {
    case "admin.snapshot": {
      if (event.ts < world.snapshotFetchedAt) {
        return world;
      }
      const groups: Record<string, WorldGroup> = {};
      const groupStateFromTransition: Record<string, { state: GroupState; ts: number }> = {};
      for (const group of data.payload.groups ?? []) {
        const previousGroup = world.groups[group.groupId];
        // Within the grace window of the last live transition, it wins over the
        // snapshot's own (possibly stale-polled) state field; past that window, the
        // snapshot's state is trusted again -- see KafkaWorld.groupStateFromTransition's
        // doc. Only carried forward for groups still present in this snapshot.
        const transition = world.groupStateFromTransition[group.groupId];
        const withinGrace =
          transition !== undefined && event.ts - transition.ts < STATE_RECONCILE_GRACE_MS;
        const state = withinGrace ? transition.state : group.state;
        if (transition !== undefined) {
          groupStateFromTransition[group.groupId] = transition;
        }
        groups[group.groupId] = {
          groupId: group.groupId,
          state,
          // The snapshot is the membership baseline (ADR-0003), but a "lost" mark is
          // carried forward across snapshots for as long as the broker still lists
          // the member: it only naturally disappears once the broker itself removes
          // the member (session timeout), at which point it's simply absent from
          // `group.members` below and this loop drops it -- see the Phase 2 review's
          // "kill flicker" fix.
          members: group.members.map((member) => {
            const wasLost = previousGroup?.members.some(
              (m) => m.clientId === member.clientId && m.liveness === "lost",
            );
            return { ...member, liveness: wasLost ? "lost" : "alive" };
          }),
          offsets: group.offsets.map((offset) => ({ ...offset, lag: offset.lag })),
        };
      }
      return {
        topics: data.payload.topics,
        groups,
        snapshotFetchedAt: event.ts,
        groupStateFromTransition,
      };
    }

    case "group.state.changed": {
      if (event.ts < world.snapshotFetchedAt) {
        return world;
      }
      const { groupId, to } = data.payload;
      const next = upsertGroup(
        world,
        groupId,
        (existing) => ({ ...existing, state: to }),
        () => ({ groupId, state: to, members: [], offsets: [] }),
      );
      return {
        ...next,
        groupStateFromTransition: {
          ...world.groupStateFromTransition,
          [groupId]: { state: to, ts: event.ts },
        },
      };
    }

    case "consumer.group.joined": {
      if (event.ts < world.snapshotFetchedAt) {
        return world;
      }
      const { groupId, clientId, memberId, assignments } = data.payload;
      // A fresh join always means "alive" -- even if this clientId had a "lost" mark
      // from an earlier consumer.connection.lost, this event proves the connection is
      // (again) live.
      const member: WorldGroupMember = { memberId, clientId, assignments, liveness: "alive" };
      return upsertGroup(
        world,
        groupId,
        (existing) => {
          // Deduped by clientId, not memberId: classic protocol assigns a fresh
          // memberId on every rejoin (e.g. after a rebalance) for the same underlying
          // actor, so matching by memberId would leave the old, now-stale entry
          // behind instead of replacing it.
          const index = existing.members.findIndex((m) => m.clientId === clientId);
          const members =
            index === -1
              ? [...existing.members, member]
              : existing.members.map((m, i) => (i === index ? member : m));
          return { ...existing, members };
        },
        () => ({ groupId, state: "Stable", members: [member], offsets: [] }),
      );
    }

    case "consumer.connection.lost": {
      if (event.ts < world.snapshotFetchedAt) {
        return world;
      }
      const { groupId, clientId } = data.payload;
      const existing = world.groups[groupId];
      if (existing === undefined) {
        return world;
      }
      const index = existing.members.findIndex((m) => m.clientId === clientId);
      if (index === -1) {
        return world;
      }
      const members = existing.members.map((m, i) =>
        i === index ? { ...m, liveness: "lost" as const } : m,
      );
      return { ...world, groups: { ...world.groups, [groupId]: { ...existing, members } } };
    }

    case "consumer.group.left": {
      if (event.ts < world.snapshotFetchedAt) {
        return world;
      }
      const { groupId, clientId } = data.payload;
      const existing = world.groups[groupId];
      if (existing === undefined) {
        return world;
      }
      const members = existing.members.filter((m) => m.clientId !== clientId);
      return { ...world, groups: { ...world.groups, [groupId]: { ...existing, members } } };
    }

    default:
      return world;
  }
}
