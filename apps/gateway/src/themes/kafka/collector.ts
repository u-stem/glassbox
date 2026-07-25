import { channel } from "node:diagnostics_channel";
import type {
  ConsumerGroupJoiningPayload,
  ConsumerGroupSyncingPayload,
  ProducerSendEndPayload,
  ProducerSendStartPayload,
} from "@glassbox/schema";
import { bigintOffsetToNumber } from "@glassbox/schema";
import { ulid } from "ulid";
import type { EventBus } from "../../event-bus";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function hasClientId(value: unknown): value is { clientId: string } {
  return isRecord(value) && typeof value.clientId === "string";
}

/**
 * Extracts the actorId from a diagnostics_channel context. Per the spike, the actor
 * instance itself (Producer/Consumer/Admin) is passed under `client` (or `instance`
 * for creation-notification channels); it is never JSON-serializable and must not be
 * forwarded into a GlassboxEvent payload as-is.
 */
function extractActorId(context: Record<string, unknown>): string | undefined {
  if (hasClientId(context.client)) {
    return context.client.clientId;
  }
  if (hasClientId(context.instance)) {
    return context.instance.clientId;
  }
  return undefined;
}

interface ProducerMessageLike {
  topic: string;
  key?: unknown;
  metadata?: unknown;
}

function isProducerMessageLike(value: unknown): value is ProducerMessageLike {
  return isRecord(value) && typeof value.topic === "string";
}

/**
 * Reads a scenario correlation id stashed in a message's `metadata` field (see
 * MessageToProduce.metadata in @platformatic/kafka), set by scenarios such as
 * produce-burst so producer.send.* events can be correlated back to a scenario run.
 */
function extractScenarioRunId(message: ProducerMessageLike): string | undefined {
  if (!isRecord(message.metadata)) {
    return undefined;
  }
  const scenarioRunId = message.metadata.scenarioRunId;
  return typeof scenarioRunId === "string" ? scenarioRunId : undefined;
}

function keyToString(key: unknown): string | null {
  if (key === undefined || key === null) {
    return null;
  }
  if (typeof key === "string") {
    return key;
  }
  if (Buffer.isBuffer(key)) {
    return key.toString("utf8");
  }
  return null;
}

export interface MappedKafkaEvent<T> {
  actorId: string;
  event: T;
}

export function mapProducerSendStart(
  context: unknown,
):
  | MappedKafkaEvent<{ type: "producer.send.start"; payload: ProducerSendStartPayload }>
  | undefined {
  if (!isRecord(context)) {
    return undefined;
  }
  const actorId = extractActorId(context);
  if (actorId === undefined) {
    return undefined;
  }
  if (!isRecord(context.options)) {
    return undefined;
  }
  const rawMessages = context.options.messages;
  if (!Array.isArray(rawMessages)) {
    return undefined;
  }
  const messages = rawMessages.filter(isProducerMessageLike);
  const firstMessage = messages[0];
  if (firstMessage === undefined) {
    return undefined;
  }

  const scenarioRunId = extractScenarioRunId(firstMessage);

  return {
    actorId,
    event: {
      type: "producer.send.start",
      payload: {
        topic: firstMessage.topic,
        messageCount: messages.length,
        keys: messages.map((message) => keyToString(message.key)),
        ...(scenarioRunId === undefined ? {} : { scenarioRunId }),
      },
    },
  };
}

interface ResultOffsetLike {
  topic: string;
  partition: number;
  offset: bigint;
}

function isResultOffsetLike(value: unknown): value is ResultOffsetLike {
  return (
    isRecord(value) &&
    typeof value.topic === "string" &&
    typeof value.partition === "number" &&
    typeof value.offset === "bigint"
  );
}

export function mapProducerSendEnd(
  context: unknown,
): MappedKafkaEvent<{ type: "producer.send.end"; payload: ProducerSendEndPayload }> | undefined {
  if (!isRecord(context)) {
    return undefined;
  }
  const actorId = extractActorId(context);
  if (actorId === undefined) {
    return undefined;
  }
  if (!isRecord(context.result)) {
    return undefined;
  }
  const rawOffsets = context.result.offsets;
  if (!Array.isArray(rawOffsets)) {
    return undefined;
  }
  const offsets = rawOffsets.filter(isResultOffsetLike);
  const firstOffset = offsets[0];
  if (firstOffset === undefined) {
    return undefined;
  }

  return {
    actorId,
    event: {
      type: "producer.send.end",
      payload: {
        topic: firstOffset.topic,
        offsets: offsets.map((offset) => ({
          partition: offset.partition,
          offset: bigintOffsetToNumber(offset.offset),
        })),
      },
    },
  };
}

interface ConsumerGroupOperationContext {
  operation: string;
  actorId: string;
  groupId: string;
  memberId: string | null;
}

function isConsumerGroupClientLike(
  value: unknown,
): value is { clientId: string; groupId?: unknown; memberId?: unknown } {
  return isRecord(value) && typeof value.clientId === "string";
}

/**
 * Extracts the common fields present on every `consumer:group` diagnostics_channel
 * context (see @platformatic/kafka's Consumer#joinGroup/#syncGroup, which pass
 * `createDiagnosticContext({ client: this, operation, ... })`).
 */
function extractConsumerGroupOperationContext(
  context: unknown,
): ConsumerGroupOperationContext | undefined {
  if (!isRecord(context) || typeof context.operation !== "string") {
    return undefined;
  }
  if (!isConsumerGroupClientLike(context.client)) {
    return undefined;
  }
  const groupId = context.client.groupId;
  if (typeof groupId !== "string") {
    return undefined;
  }
  const memberId = typeof context.client.memberId === "string" ? context.client.memberId : null;

  return {
    operation: context.operation,
    actorId: context.client.clientId,
    groupId,
    memberId,
  };
}

/**
 * Maps a `consumer:group` diagnostics_channel `start` phase for the `joinGroup`
 * operation to consumer.group.joining. This is the "member is entering
 * PreparingRebalance" edge (ADR-0003); there is no EventEmitter equivalent for the
 * start of joinGroup (only its completion, via `consumer:group:join`, is an
 * EventEmitter event), so this channel is the sole source for this edge.
 */
export function mapConsumerGroupJoining(
  context: unknown,
):
  | MappedKafkaEvent<{ type: "consumer.group.joining"; payload: ConsumerGroupJoiningPayload }>
  | undefined {
  const parsed = extractConsumerGroupOperationContext(context);
  if (parsed === undefined || parsed.operation !== "joinGroup") {
    return undefined;
  }
  return {
    actorId: parsed.actorId,
    event: {
      type: "consumer.group.joining",
      payload: { groupId: parsed.groupId, clientId: parsed.actorId, memberId: parsed.memberId },
    },
  };
}

/**
 * Maps a `consumer:group` diagnostics_channel `start` phase for the `syncGroup`
 * operation to consumer.group.syncing (the "member is entering CompletingRebalance"
 * edge, per ADR-0003). Same rationale as mapConsumerGroupJoining: no EventEmitter
 * equivalent exists for the start of syncGroup.
 */
export function mapConsumerGroupSyncing(
  context: unknown,
):
  | MappedKafkaEvent<{ type: "consumer.group.syncing"; payload: ConsumerGroupSyncingPayload }>
  | undefined {
  const parsed = extractConsumerGroupOperationContext(context);
  if (parsed === undefined || parsed.operation !== "syncGroup") {
    return undefined;
  }
  return {
    actorId: parsed.actorId,
    event: {
      type: "consumer.group.syncing",
      payload: { groupId: parsed.groupId, clientId: parsed.actorId },
    },
  };
}

/**
 * Subscribes to the diagnostics_channel names actually published by
 * @platformatic/kafka's tracingChannel: `tracing:plt:kafka:${section}:${phase}`
 * (see docs/spikes/0001-platformatic-kafka-diagnostics.md). Phase 1 wired only
 * producer:sends:start; Phase 2 adds the consumer:group channel's joinGroup/syncGroup
 * start phases (see mapConsumerGroupJoining/mapConsumerGroupSyncing for why these two
 * operations specifically need the diagnostics_channel rather than EventEmitter).
 *
 * producer:sends:end is intentionally NOT wired here: empirically (confirmed against
 * a real broker, not just the spike), that channel's context never carries a `result`
 * field, so it cannot report offsets. producer.send.end is instead published directly
 * by ProducerActor from the resolved ProduceResult (see actors/producer-actor.ts).
 * mapProducerSendEnd is kept as a tested pure function in case upstream behavior
 * changes and this channel becomes usable again.
 *
 * The `consumer:group` channel's `findGroupCoordinator` and `leaveGroup` operations
 * are intentionally NOT wired to any event: `leaveGroup` is covered by
 * ConsumerActor's EventEmitter subscription to `consumer:group:leave` instead (see
 * actors/consumer-actor.ts's class doc comment), to avoid double-publishing the same
 * member-left edge from two sources.
 */
export function startKafkaCollector(eventBus: EventBus): () => void {
  const sendStartChannel = channel("tracing:plt:kafka:producer:sends:start");
  const consumerGroupStartChannel = channel("tracing:plt:kafka:consumer:group:start");

  const onSendStart = (context: unknown): void => {
    const mapped = mapProducerSendStart(context);
    if (!mapped) {
      return;
    }
    eventBus.publish({
      id: ulid(),
      ts: Date.now(),
      theme: "kafka",
      source: { kind: "client", actorId: mapped.actorId },
      type: mapped.event.type,
      payload: mapped.event.payload,
    });
  };

  const onConsumerGroupStart = (context: unknown): void => {
    const mapped = mapConsumerGroupJoining(context) ?? mapConsumerGroupSyncing(context);
    if (!mapped) {
      return;
    }
    eventBus.publish({
      id: ulid(),
      ts: Date.now(),
      theme: "kafka",
      source: { kind: "client", actorId: mapped.actorId },
      type: mapped.event.type,
      payload: mapped.event.payload,
    });
  };

  sendStartChannel.subscribe(onSendStart);
  consumerGroupStartChannel.subscribe(onConsumerGroupStart);

  return () => {
    sendStartChannel.unsubscribe(onSendStart);
    consumerGroupStartChannel.unsubscribe(onConsumerGroupStart);
  };
}
