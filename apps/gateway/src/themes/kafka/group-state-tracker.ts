import type { GroupState } from "@glassbox/schema";

export type MemberPhase = "preparing" | "completing" | "stable";

export interface MemberState {
  clientId: string;
  phase: MemberPhase;
  memberId: string | null;
  generationId: number | undefined;
  isLeader: boolean | undefined;
}

export interface GroupTrackerState {
  groupId: string;
  members: Record<string, MemberState>;
  state: GroupState;
}

/**
 * Member-level edges, per the transition table in ADR-0003 / the spike
 * (docs/spikes/0001-platformatic-kafka-diagnostics.md):
 * - "rebalancing": an already-stable member notices a rebalance is starting
 *   (EventEmitter `consumer:group:rebalance`, itself preceded by a
 *   `consumer:heartbeat:error`). Signals the member is about to re-join.
 * - "joining": `consumer:group` channel, `operation: 'joinGroup'` start.
 * - "syncing": `consumer:group` channel, `operation: 'syncGroup'` start.
 * - "joined": EventEmitter `consumer:group:join` (Stable reached for this member,
 *   carries the new generationId).
 * - "left": `operation: 'leaveGroup'` / EventEmitter `consumer:group:leave`.
 */
export type MemberEdge =
  | { type: "rebalancing"; clientId: string }
  | { type: "joining"; clientId: string; memberId?: string | null | undefined }
  | { type: "syncing"; clientId: string }
  | {
      type: "joined";
      clientId: string;
      memberId: string;
      // Optional (not defaulted to 0/false upstream): see ConsumerGroupJoinedPayload's
      // doc in packages/schema/src/kafka-events.ts.
      generationId?: number | undefined;
      isLeader?: boolean | undefined;
    }
  | { type: "left"; clientId: string; memberId?: string | null | undefined };

export interface GroupStateTransition {
  from: GroupState;
  to: GroupState;
  generationId: number | undefined;
  edge: MemberEdge["type"];
}

export interface ApplyEdgeResult {
  state: GroupTrackerState;
  transition?: GroupStateTransition;
}

export function createInitialGroupTrackerState(groupId: string): GroupTrackerState {
  return { groupId, members: {}, state: "Empty" };
}

/**
 * Derives the group-wide 4-state model from the set of currently-known members' local
 * phases (ADR-0003): PreparingRebalance if any member is still preparing, else
 * CompletingRebalance if any is still completing, else Stable once every known member
 * has reached its own Stable phase. Empty when no members are tracked at all.
 */
function deriveGroupState(members: Record<string, MemberState>): GroupState {
  const values = Object.values(members);
  if (values.length === 0) {
    return "Empty";
  }
  if (values.some((member) => member.phase === "preparing")) {
    return "PreparingRebalance";
  }
  if (values.some((member) => member.phase === "completing")) {
    return "CompletingRebalance";
  }
  return "Stable";
}

/**
 * Pure reducer: applies a single member-level edge to a group's tracked state and
 * derives the new group-wide state. Emits a `transition` only when the derived
 * group-wide state actually changes (dedup), so callers can publish
 * `group.state.changed` events without needing their own dedup logic.
 */
export function applyMemberEdge(current: GroupTrackerState, edge: MemberEdge): ApplyEdgeResult {
  const members = { ...current.members };
  const existing = members[edge.clientId];

  switch (edge.type) {
    case "rebalancing": {
      members[edge.clientId] = {
        clientId: edge.clientId,
        phase: "preparing",
        memberId: existing?.memberId ?? null,
        generationId: existing?.generationId,
        isLeader: existing?.isLeader,
      };
      break;
    }
    case "joining": {
      members[edge.clientId] = {
        clientId: edge.clientId,
        phase: "preparing",
        memberId: edge.memberId ?? existing?.memberId ?? null,
        generationId: existing?.generationId,
        isLeader: existing?.isLeader,
      };
      break;
    }
    case "syncing": {
      members[edge.clientId] = {
        clientId: edge.clientId,
        phase: "completing",
        memberId: existing?.memberId ?? null,
        generationId: existing?.generationId,
        isLeader: existing?.isLeader,
      };
      break;
    }
    case "joined": {
      members[edge.clientId] = {
        clientId: edge.clientId,
        phase: "stable",
        memberId: edge.memberId,
        generationId: edge.generationId,
        isLeader: edge.isLeader,
      };
      break;
    }
    case "left": {
      delete members[edge.clientId];
      break;
    }
  }

  const nextGroupState = deriveGroupState(members);
  const nextState: GroupTrackerState = { groupId: current.groupId, members, state: nextGroupState };

  if (nextGroupState === current.state) {
    return { state: nextState };
  }

  const generationId = edge.type === "joined" ? edge.generationId : undefined;
  return {
    state: nextState,
    transition: { from: current.state, to: nextGroupState, generationId, edge: edge.type },
  };
}

export interface GroupStateTracker {
  applyEdge(groupId: string, edge: MemberEdge): void;
  /** Exposed for tests/observability: which groupIds currently have a tracked entry
   * (i.e. have not yet returned to Empty and been evicted). */
  trackedGroupIds(): string[];
}

/**
 * Thin stateful wrapper around applyMemberEdge: keeps one GroupTrackerState per
 * groupId and invokes onTransition whenever a group's derived state changes.
 *
 * Once a group's derived state returns to "Empty" (no tracked members left), its
 * entry is evicted from the internal Map entirely rather than kept around as an
 * empty-but-present state. Without this, every group that a scenario ever created
 * (e.g. repeated add-consumer/remove-consumer demo runs) would leave a permanent,
 * ever-growing entry in this Map for the lifetime of the gateway process.
 */
export function createGroupStateTracker(
  onTransition: (groupId: string, transition: GroupStateTransition) => void,
): GroupStateTracker {
  const states = new Map<string, GroupTrackerState>();

  return {
    applyEdge(groupId, edge) {
      const current = states.get(groupId) ?? createInitialGroupTrackerState(groupId);
      const result = applyMemberEdge(current, edge);

      if (result.state.state === "Empty") {
        states.delete(groupId);
      } else {
        states.set(groupId, result.state);
      }

      if (result.transition) {
        onTransition(groupId, result.transition);
      }
    },
    trackedGroupIds() {
      return [...states.keys()];
    },
  };
}
