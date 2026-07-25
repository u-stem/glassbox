import { z } from "zod";

/**
 * Converts a Kafka protocol offset (bigint) to a number for use in event payloads.
 * Learning-oriented workloads never approach Number.MAX_SAFE_INTEGER, so this is a
 * defensive check rather than a real-world constraint.
 */
export function bigintOffsetToNumber(value: bigint): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new RangeError(`offset ${value} exceeds Number.MAX_SAFE_INTEGER`);
  }
  return Number(value);
}

const producerSendStartEventSchema = z.object({
  type: z.literal("producer.send.start"),
  payload: z.object({
    topic: z.string(),
    messageCount: z.number().int().positive(),
    keys: z.array(z.string().nullable()),
    // Correlates a send with the scenario run that triggered it (e.g. produce-burst).
    scenarioRunId: z.string().ulid().optional(),
  }),
});

const producerSendEndEventSchema = z.object({
  type: z.literal("producer.send.end"),
  payload: z.object({
    topic: z.string(),
    offsets: z.array(
      z.object({
        partition: z.number().int().nonnegative(),
        offset: z.number().int().nonnegative(),
      }),
    ),
    scenarioRunId: z.string().ulid().optional(),
  }),
});

const adminPartitionOffsetsSchema = z.object({
  index: z.number().int().nonnegative(),
  earliestOffset: z.number().int().nonnegative(),
  endOffset: z.number().int().nonnegative(),
});

/**
 * The 4-state UI model for consumer group rebalance, per ADR-0003 / the spike
 * (docs/spikes/0001-platformatic-kafka-diagnostics.md). Distinct from the broker's raw
 * ConsumerGroupStateValue strings (e.g. "STABLE"); admin-poller maps broker state onto
 * this enum (DEAD groups are omitted from the snapshot entirely).
 */
export const groupStateSchema = z.enum([
  "Empty",
  "PreparingRebalance",
  "CompletingRebalance",
  "Stable",
]);

const groupAssignmentSchema = z.object({
  topic: z.string(),
  partitions: z.array(z.number().int().nonnegative()),
});

const groupMemberSchema = z.object({
  memberId: z.string(),
  clientId: z.string(),
  assignments: z.array(groupAssignmentSchema),
});

const groupOffsetSchema = z.object({
  topic: z.string(),
  partition: z.number().int().nonnegative(),
  committed: z.number().int().nonnegative(),
  // Clamped to >= 0 when present (admin-poller; see ADR-0003: describeGroups +
  // listConsumerGroupOffsets are non-atomic, so the raw subtraction can theoretically
  // go negative). Absent (not 0) when the end offset for this partition is unknown
  // this tick (e.g. its topic wasn't in this tick's topic list, or the offsets RPC
  // failed) -- 0 would incorrectly imply "no lag" rather than "unknown". The UI
  // renders this as "--" rather than assuming a value.
  lag: z.number().int().nonnegative().optional(),
});

const adminGroupSchema = z.object({
  groupId: z.string(),
  state: groupStateSchema,
  members: z.array(groupMemberSchema),
  offsets: z.array(groupOffsetSchema),
});

const adminSnapshotEventSchema = z.object({
  type: z.literal("admin.snapshot"),
  payload: z.object({
    topics: z.array(
      z.object({
        name: z.string(),
        partitions: z.array(adminPartitionOffsetsSchema),
      }),
    ),
    // Optional for backward compatibility with Phase 1 payloads that predate consumer
    // groups; admin-poller always includes it from Phase 2 onward.
    groups: z.array(adminGroupSchema).optional(),
  }),
});

const consumerGroupJoiningEventSchema = z.object({
  type: z.literal("consumer.group.joining"),
  payload: z.object({
    groupId: z.string(),
    clientId: z.string(),
    memberId: z.string().nullable().optional(),
  }),
});

const consumerGroupJoinedEventSchema = z.object({
  type: z.literal("consumer.group.joined"),
  payload: z.object({
    groupId: z.string(),
    clientId: z.string(),
    memberId: z.string(),
    // Optional rather than defaulted to 0/false: the underlying
    // ConsumerGroupJoinPayload types these as optional, and 0/false are misleadingly
    // meaningful values (generation 0, non-leader) to substitute for "unknown".
    generationId: z.number().int().nonnegative().optional(),
    isLeader: z.boolean().optional(),
    assignments: z.array(groupAssignmentSchema),
  }),
});

const consumerGroupLeftEventSchema = z.object({
  type: z.literal("consumer.group.left"),
  payload: z.object({
    groupId: z.string(),
    clientId: z.string(),
    memberId: z.string().nullable(),
  }),
});

/**
 * Published when this gateway itself severs a consumer's connection without ever
 * sending LeaveGroup (the "kill" removal mode). Deliberately distinct from
 * consumer.group.left: from the broker's point of view this member is still part of
 * the group until its session times out, so the UI must not treat this as a real
 * departure (see ADR-0003 / the Phase 2 review: hiding this distinction would show a
 * confusing "member vanishes, then reappears via the next admin.snapshot, then
 * vanishes again once the real timeout fires" flicker). Instead this is meant to be
 * rendered as an explicit "connection lost, awaiting session timeout" state, which is
 * itself the whole point of the kill scenario as a demonstration of session timeouts.
 */
const consumerConnectionLostEventSchema = z.object({
  type: z.literal("consumer.connection.lost"),
  payload: z.object({
    groupId: z.string(),
    clientId: z.string(),
    reason: z.enum(["kill"]),
  }),
});

const consumerGroupRebalancingEventSchema = z.object({
  type: z.literal("consumer.group.rebalancing"),
  payload: z.object({
    groupId: z.string(),
    clientId: z.string(),
  }),
});

const consumerGroupSyncingEventSchema = z.object({
  type: z.literal("consumer.group.syncing"),
  payload: z.object({
    groupId: z.string(),
    clientId: z.string(),
  }),
});

const consumerHeartbeatOkEventSchema = z.object({
  type: z.literal("consumer.heartbeat.ok"),
  payload: z.object({
    groupId: z.string().optional(),
    clientId: z.string(),
    memberId: z.string().nullable().optional(),
    generationId: z.number().int().nonnegative().optional(),
  }),
});

const consumerHeartbeatFailedEventSchema = z.object({
  type: z.literal("consumer.heartbeat.failed"),
  payload: z.object({
    groupId: z.string().optional(),
    clientId: z.string(),
    memberId: z.string().nullable().optional(),
    generationId: z.number().int().nonnegative().optional(),
    error: z.string(),
  }),
});

const consumerMessageEventSchema = z.object({
  type: z.literal("consumer.message"),
  payload: z.object({
    groupId: z.string(),
    clientId: z.string(),
    topic: z.string(),
    partition: z.number().int().nonnegative(),
    offset: z.number().int().nonnegative(),
    key: z.string().nullable(),
  }),
});

const consumerCommitEventSchema = z.object({
  type: z.literal("consumer.commit"),
  payload: z.object({
    groupId: z.string(),
    clientId: z.string(),
    offsets: z.array(
      z.object({
        topic: z.string(),
        partition: z.number().int().nonnegative(),
        offset: z.number().int().nonnegative(),
      }),
    ),
  }),
});

const groupStateChangedEventSchema = z.object({
  type: z.literal("group.state.changed"),
  payload: z.object({
    groupId: z.string(),
    from: groupStateSchema,
    to: groupStateSchema,
    generationId: z.number().int().nonnegative().optional(),
    // Free-form label identifying which member-level edge (per ADR-0003's transition
    // table) drove this transition, e.g. "member-joining" / "member-syncing" /
    // "member-stable" / "member-left" / "admin-reconcile".
    edge: z.string().min(1),
  }),
});

const scenarioStartedEventSchema = z.object({
  type: z.literal("scenario.started"),
  payload: z.object({
    scenarioId: z.string(),
    params: z.unknown(),
    scenarioRunId: z.string().ulid().optional(),
  }),
});

const scenarioFinishedEventSchema = z.object({
  type: z.literal("scenario.finished"),
  payload: z.object({
    scenarioId: z.string(),
    params: z.unknown(),
    error: z.string().optional(),
    scenarioRunId: z.string().ulid().optional(),
  }),
});

const actorCreatedEventSchema = z.object({
  type: z.literal("actor.created"),
  payload: z.object({
    actorKind: z.string(),
    actorId: z.string(),
  }),
});

const actorDestroyedEventSchema = z.object({
  type: z.literal("actor.destroyed"),
  payload: z.object({
    actorKind: z.string(),
    actorId: z.string(),
  }),
});

const controlResetEventSchema = z.object({
  type: z.literal("control.reset"),
  payload: z.object({
    // enum (not a bare literal) so future reset reasons can be added without a breaking change.
    reason: z.enum(["gap"]),
  }),
});

export const kafkaEventSchema = z.discriminatedUnion("type", [
  producerSendStartEventSchema,
  producerSendEndEventSchema,
  adminSnapshotEventSchema,
  scenarioStartedEventSchema,
  scenarioFinishedEventSchema,
  actorCreatedEventSchema,
  actorDestroyedEventSchema,
  controlResetEventSchema,
  consumerGroupJoiningEventSchema,
  consumerGroupJoinedEventSchema,
  consumerGroupLeftEventSchema,
  consumerConnectionLostEventSchema,
  consumerGroupRebalancingEventSchema,
  consumerGroupSyncingEventSchema,
  consumerHeartbeatOkEventSchema,
  consumerHeartbeatFailedEventSchema,
  consumerMessageEventSchema,
  consumerCommitEventSchema,
  groupStateChangedEventSchema,
]);

export type KafkaEvent = z.infer<typeof kafkaEventSchema>;
export type GroupState = z.infer<typeof groupStateSchema>;
export type GroupAssignment = z.infer<typeof groupAssignmentSchema>;
export type AdminGroup = z.infer<typeof adminGroupSchema>;
export type AdminSnapshotPayload = z.infer<typeof adminSnapshotEventSchema>["payload"];
export type ProducerSendStartPayload = z.infer<typeof producerSendStartEventSchema>["payload"];
export type ProducerSendEndPayload = z.infer<typeof producerSendEndEventSchema>["payload"];
export type ConsumerGroupJoiningPayload = z.infer<
  typeof consumerGroupJoiningEventSchema
>["payload"];
export type ConsumerGroupJoinedPayload = z.infer<typeof consumerGroupJoinedEventSchema>["payload"];
export type ConsumerGroupLeftPayload = z.infer<typeof consumerGroupLeftEventSchema>["payload"];
export type ConsumerConnectionLostPayload = z.infer<
  typeof consumerConnectionLostEventSchema
>["payload"];
export type ConsumerGroupRebalancingPayload = z.infer<
  typeof consumerGroupRebalancingEventSchema
>["payload"];
export type ConsumerGroupSyncingPayload = z.infer<
  typeof consumerGroupSyncingEventSchema
>["payload"];
export type ConsumerHeartbeatOkPayload = z.infer<typeof consumerHeartbeatOkEventSchema>["payload"];
export type ConsumerHeartbeatFailedPayload = z.infer<
  typeof consumerHeartbeatFailedEventSchema
>["payload"];
export type ConsumerMessagePayload = z.infer<typeof consumerMessageEventSchema>["payload"];
export type ConsumerCommitPayload = z.infer<typeof consumerCommitEventSchema>["payload"];
export type GroupStateChangedPayload = z.infer<typeof groupStateChangedEventSchema>["payload"];
