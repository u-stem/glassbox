import { ulid } from "ulid";
import { z } from "zod";

/** `.meta()` only affects the JSON Schema the web form is generated from (see
 * registry.ts's describe()); parsing is unchanged. Without it the form labels each
 * field with its raw property name, which is exactly the vocabulary a newcomer to
 * Kafka is stuck on. */
export const produceBurstParamsSchema = z.object({
  topic: z.string().default("glassbox.demo").meta({ title: "送信先トピック" }),
  count: z
    .number()
    .int()
    .min(1)
    .max(1000)
    .default(10)
    .meta({ title: "送る件数", description: "1〜1000" }),
  rateMs: z
    .number()
    .int()
    .min(0)
    .max(1000)
    .default(50)
    .meta({ title: "送信間隔(ミリ秒)", description: "0 にすると間を空けずに連続送信する" }),
  keyStrategy: z.enum(["round-robin", "keyed"]).default("round-robin").meta({
    title: "キーの決め方",
    description: "keyed は同じキーを同じパーティションへ、round-robin は全体へ分散させる",
  }),
});

export type ProduceBurstParams = z.infer<typeof produceBurstParamsSchema>;

const KEYED_PARTITION_SPREAD = 4;

/**
 * round-robin: no key, letting the producer's default partitioner spread messages
 * across partitions in turn. keyed: a small, fixed set of keys so that identical
 * keys are visibly routed to the same partition in the UI.
 */
export function buildMessageKey(
  strategy: ProduceBurstParams["keyStrategy"],
  index: number,
): string | undefined {
  if (strategy === "round-robin") {
    return undefined;
  }
  return `key-${index % KEYED_PARTITION_SPREAD}`;
}

export interface ProduceBurstMessage {
  topic: string;
  key?: Buffer;
  value: Buffer;
  metadata?: { scenarioRunId: string };
}

export interface ProducerLike {
  send(messages: ProduceBurstMessage[]): Promise<void>;
}

export interface EventBusLike {
  publish(event: unknown): unknown;
}

const SCENARIO_ID = "produce-burst";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function publishScenarioEvent(
  eventBus: EventBusLike,
  type: "scenario.started" | "scenario.finished",
  params: ProduceBurstParams,
  scenarioRunId: string,
  error?: string,
): void {
  eventBus.publish({
    id: ulid(),
    ts: Date.now(),
    theme: "kafka",
    source: { kind: "scenario", actorId: SCENARIO_ID },
    type,
    payload:
      error === undefined
        ? { scenarioId: SCENARIO_ID, params, scenarioRunId }
        : { scenarioId: SCENARIO_ID, params, scenarioRunId, error },
  });
}

/**
 * Runs a single produce-burst. scenarioRunId (fresh per run) is stamped on
 * scenario.started/finished and on every sent message's metadata, so
 * producer.send.start events (sourced from diagnostics_channel, see collector.ts)
 * can be correlated back to the scenario run that triggered them.
 */
export async function runProduceBurst(
  producer: ProducerLike,
  eventBus: EventBusLike,
  params: ProduceBurstParams,
): Promise<void> {
  const scenarioRunId = ulid();
  publishScenarioEvent(eventBus, "scenario.started", params, scenarioRunId);

  try {
    for (let index = 0; index < params.count; index += 1) {
      const key = buildMessageKey(params.keyStrategy, index);
      const message: ProduceBurstMessage = {
        topic: params.topic,
        value: Buffer.from(`message-${index}`),
        metadata: { scenarioRunId },
        ...(key === undefined ? {} : { key: Buffer.from(key) }),
      };
      await producer.send([message]);

      const isLastMessage = index === params.count - 1;
      if (params.rateMs > 0 && !isLastMessage) {
        await sleep(params.rateMs);
      }
    }

    publishScenarioEvent(eventBus, "scenario.finished", params, scenarioRunId);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    publishScenarioEvent(eventBus, "scenario.finished", params, scenarioRunId, message);
    throw error;
  }
}

export class ProduceBurstAlreadyRunningError extends Error {
  constructor() {
    super("produce-burst is already running");
    this.name = "ProduceBurstAlreadyRunningError";
  }
}

export interface ProduceBurstRunner {
  isRunning: () => boolean;
  run: (params: ProduceBurstParams) => Promise<void>;
}

/**
 * Wraps runProduceBurst with a single-flight guard: only one produce-burst run may
 * be in flight at a time. The check-then-set of the `running` flag happens
 * synchronously (no await in between), so it is safe under Node's single-threaded
 * event loop even without an external lock.
 */
export function createProduceBurstRunner(
  producer: ProducerLike,
  eventBus: EventBusLike,
): ProduceBurstRunner {
  let running = false;

  return {
    isRunning: () => running,
    async run(params: ProduceBurstParams): Promise<void> {
      if (running) {
        throw new ProduceBurstAlreadyRunningError();
      }
      running = true;
      try {
        await runProduceBurst(producer, eventBus, params);
      } finally {
        running = false;
      }
    },
  };
}
