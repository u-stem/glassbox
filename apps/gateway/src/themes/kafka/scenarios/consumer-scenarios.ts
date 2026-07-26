import { ulid } from "ulid";
import { z } from "zod";
import type { ConsumerRegistry } from "../actors/consumer-registry";
import type { ScenarioDefinition } from "./registry";

export interface ScenarioEventBus {
  publish(event: unknown): unknown;
}

function publishScenarioEvent(
  eventBus: ScenarioEventBus,
  scenarioId: string,
  type: "scenario.started" | "scenario.finished",
  params: unknown,
  error?: string,
): void {
  eventBus.publish({
    id: ulid(),
    ts: Date.now(),
    theme: "kafka",
    source: { kind: "scenario", actorId: scenarioId },
    type,
    payload: error === undefined ? { scenarioId, params } : { scenarioId, params, error },
  });
}

// 5 minutes: generous for a learning scenario's slow-motion demos, but bounds how long
// a misconfigured/malicious request can keep a consumer's session-timeout wait (and
// thus its kill-mode watchdog) pending.
const MAX_TIMEOUT_MS = 5 * 60 * 1000;

export const addConsumerParamsSchema = z.object({
  groupId: z.string().default("glassbox-consumers").meta({
    title: "consumer group の ID",
    description: "同じ ID を指定した consumer が 1 つの group を作る",
  }),
  topics: z
    .array(z.string())
    .default(["glassbox.demo"])
    .meta({ title: "購読するトピック(JSON 配列)" }),
  // Overrides mainly used by integration tests (see ConsumerActor's doc) to keep a
  // "kill" removal's session-timeout wait short instead of the library's 1-minute
  // default.
  sessionTimeoutMs: z
    .number()
    .int()
    .positive()
    .max(MAX_TIMEOUT_MS)
    .optional()
    .meta({ title: "セッションタイムアウト(ミリ秒)", description: "未指定なら既定の 60 秒" }),
  rebalanceTimeoutMs: z
    .number()
    .int()
    .positive()
    .max(MAX_TIMEOUT_MS)
    .optional()
    .meta({ title: "リバランスタイムアウト(ミリ秒)", description: "主に統合テスト用" }),
  heartbeatIntervalMs: z
    .number()
    .int()
    .positive()
    .max(MAX_TIMEOUT_MS)
    .optional()
    .meta({ title: "ハートビート間隔(ミリ秒)", description: "主に統合テスト用" }),
});
export type AddConsumerParams = z.infer<typeof addConsumerParamsSchema>;

/**
 * add-consumer: joins a fresh ConsumerActor to the group (clientId auto-numbered by
 * the registry, per the scenario's contract). Triggers a join -> rebalance ->
 * re-assignment sequence observable via consumer.group.* / group.state.changed.
 */
export function createAddConsumerScenario(
  registry: ConsumerRegistry,
  eventBus: ScenarioEventBus,
): ScenarioDefinition<AddConsumerParams> {
  return {
    id: "add-consumer",
    paramsSchema: addConsumerParamsSchema,
    isAtCapacity: () => registry.isFull(),
    async start(params) {
      publishScenarioEvent(eventBus, "add-consumer", "scenario.started", params);
      try {
        registry.add({
          groupId: params.groupId,
          topics: params.topics,
          ...(params.sessionTimeoutMs === undefined
            ? {}
            : { sessionTimeoutMs: params.sessionTimeoutMs }),
          ...(params.rebalanceTimeoutMs === undefined
            ? {}
            : { rebalanceTimeoutMs: params.rebalanceTimeoutMs }),
          ...(params.heartbeatIntervalMs === undefined
            ? {}
            : { heartbeatIntervalMs: params.heartbeatIntervalMs }),
        });
        publishScenarioEvent(eventBus, "add-consumer", "scenario.finished", params);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        publishScenarioEvent(eventBus, "add-consumer", "scenario.finished", params, message);
        throw error;
      }
    },
  };
}

export const removeConsumerParamsSchema = z.object({
  consumerId: z.string().meta({ title: "対象の consumer ID", description: "例: consumer-1" }),
  mode: z.enum(["graceful", "kill"]).default("graceful").meta({
    title: "削除の仕方",
    description:
      "graceful は離脱通知を送って即座に抜け、kill は接続を切りセッションタイムアウトまで残る",
  }),
});
export type RemoveConsumerParams = z.infer<typeof removeConsumerParamsSchema>;

/**
 * remove-consumer: mode 'graceful' sends LeaveGroup immediately (close(true), per the
 * spike); mode 'kill' destroys the connection without LeaveGroup, so the group only
 * notices the member is gone once its session times out (see ConsumerActor#close).
 */
export function createRemoveConsumerScenario(
  registry: ConsumerRegistry,
  eventBus: ScenarioEventBus,
): ScenarioDefinition<RemoveConsumerParams> {
  return {
    id: "remove-consumer",
    paramsSchema: removeConsumerParamsSchema,
    async start(params) {
      publishScenarioEvent(eventBus, "remove-consumer", "scenario.started", params);
      try {
        await registry.remove(params.consumerId, params.mode);
        publishScenarioEvent(eventBus, "remove-consumer", "scenario.finished", params);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        publishScenarioEvent(eventBus, "remove-consumer", "scenario.finished", params, message);
        throw error;
      }
    },
  };
}

export const slowConsumerParamsSchema = z.object({
  consumerId: z.string().meta({ title: "対象の consumer ID", description: "例: consumer-1" }),
  processingDelayMs: z
    .number()
    .int()
    .min(0)
    .max(5000)
    .meta({ title: "1 件あたりの処理遅延(ミリ秒)", description: "大きくするほどラグが増える" }),
});
export type SlowConsumerParams = z.infer<typeof slowConsumerParamsSchema>;

/**
 * slow-consumer: adjusts an existing consumer's per-message processing delay, so lag
 * (end offset - committed offset) visibly accumulates in the next admin.snapshot.
 */
export function createSlowConsumerScenario(
  registry: ConsumerRegistry,
  eventBus: ScenarioEventBus,
): ScenarioDefinition<SlowConsumerParams> {
  return {
    id: "slow-consumer",
    paramsSchema: slowConsumerParamsSchema,
    async start(params) {
      publishScenarioEvent(eventBus, "slow-consumer", "scenario.started", params);
      const actor = registry.get(params.consumerId);
      if (actor === undefined) {
        const message = `unknown consumerId: ${params.consumerId}`;
        publishScenarioEvent(eventBus, "slow-consumer", "scenario.finished", params, message);
        throw new Error(message);
      }
      actor.setProcessingDelay(params.processingDelayMs);
      publishScenarioEvent(eventBus, "slow-consumer", "scenario.finished", params);
    },
  };
}
