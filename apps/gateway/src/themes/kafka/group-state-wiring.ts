import { type GlassboxEvent, kafkaEventSchema } from "@glassbox/schema";
import { ulid } from "ulid";
import { createGroupStateTracker, type MemberEdge } from "./group-state-tracker";

export interface EventBusLike {
  publish(event: Omit<GlassboxEvent, "seq">): unknown;
  subscribe(listener: (event: GlassboxEvent) => void): () => void;
}

interface ParsedMemberEdge {
  groupId: string;
  edge: MemberEdge;
}

/**
 * Re-validates an already-internally-constructed GlassboxEvent via the shared Zod
 * schema (rather than casting `payload: unknown`), consistent with how
 * apps/web/src/lib/store.ts extracts admin.snapshot payloads.
 */
function toMemberEdge(event: GlassboxEvent): ParsedMemberEdge | undefined {
  const result = kafkaEventSchema.safeParse({ type: event.type, payload: event.payload });
  if (!result.success) {
    return undefined;
  }
  const parsed = result.data;

  switch (parsed.type) {
    case "consumer.group.joining":
      return {
        groupId: parsed.payload.groupId,
        edge: {
          type: "joining",
          clientId: parsed.payload.clientId,
          memberId: parsed.payload.memberId,
        },
      };
    case "consumer.group.syncing":
      return {
        groupId: parsed.payload.groupId,
        edge: { type: "syncing", clientId: parsed.payload.clientId },
      };
    case "consumer.group.joined":
      return {
        groupId: parsed.payload.groupId,
        edge: {
          type: "joined",
          clientId: parsed.payload.clientId,
          memberId: parsed.payload.memberId,
          generationId: parsed.payload.generationId,
          isLeader: parsed.payload.isLeader,
        },
      };
    case "consumer.group.left":
      return {
        groupId: parsed.payload.groupId,
        edge: {
          type: "left",
          clientId: parsed.payload.clientId,
          memberId: parsed.payload.memberId,
        },
      };
    case "consumer.group.rebalancing":
      return {
        groupId: parsed.payload.groupId,
        edge: { type: "rebalancing", clientId: parsed.payload.clientId },
      };
    case "consumer.connection.lost":
      // Feeds the tracker's OWN internal member bookkeeping only (so a killed
      // member's entry doesn't linger forever, and so the group can eventually be
      // evicted once it derives Empty; see group-state-tracker's Empty-eviction).
      // This is NOT a claim that the broker has removed this member -- it still
      // believes it's alive until the session times out -- so it is deliberately
      // separate from world-reducer's handling of the same consumer.connection.lost
      // event, which marks the member "lost" in the UI rather than removing it (see
      // ADR-0003 / the Phase 2 review's "kill flicker" fix).
      //
      // For a multi-member group, removing just this one member from the tracker's
      // bookkeeping does not change deriveGroupState's result (the survivors are
      // still Stable), so no premature transition is published. For a single-member
      // group, this does immediately derive Empty (the tracker's local view has no
      // members left) even though the broker itself hasn't confirmed that yet; this
      // is treated as an acceptable, narrow edge case (see the Phase 2 report) rather
      // than one requiring a broker-authoritative-only tracker.
      return {
        groupId: parsed.payload.groupId,
        edge: { type: "left", clientId: parsed.payload.clientId },
      };
    default:
      return undefined;
  }
}

/**
 * Subscribes to an event bus and drives a group-state-tracker from the member-level
 * edges carried by consumer.group.* events (sourced from both collector.ts's
 * diagnostics_channel subscription and ConsumerActor's EventEmitter subscription).
 * Publishes group.state.changed whenever the tracker derives a group-wide transition.
 */
export function wireGroupStateTracker(eventBus: EventBusLike): () => void {
  const tracker = createGroupStateTracker((groupId, transition) => {
    eventBus.publish({
      id: ulid(),
      ts: Date.now(),
      theme: "kafka",
      source: { kind: "admin" },
      type: "group.state.changed",
      payload: {
        groupId,
        from: transition.from,
        to: transition.to,
        generationId: transition.generationId,
        edge: transition.edge,
      },
    });
  });

  return eventBus.subscribe((event) => {
    const parsed = toMemberEdge(event);
    if (parsed === undefined) {
      return;
    }
    tracker.applyEdge(parsed.groupId, parsed.edge);
  });
}
