import { bigintOffsetToNumber } from "@glassbox/schema";
import { Producer } from "@platformatic/kafka";
import { ulid } from "ulid";
import type { EventBus } from "../../../event-bus";

export interface ProducerActorOptions {
  clientId: string;
  bootstrapBrokers: string[];
  eventBus: EventBus;
  /** Called whenever the underlying Producer emits a plain 'error' event (e.g. an
   * internal retry loop exhausting its budget). Optional, but the listener itself is
   * always registered regardless: @platformatic/kafka clients extend Node's
   * EventEmitter, and Node throws (crashing the whole process) when 'error' is
   * emitted with zero listeners attached. This is not hypothetical -- it was the root
   * cause of a gateway crash under repeated add-consumer/remove-consumer cycling
   * (see ConsumerActor's identical handler for the incident that surfaced this). */
  onClientError?: (error: unknown) => void;
}

export interface ProducerMessage {
  topic: string;
  key?: Buffer;
  value: Buffer;
  metadata?: { scenarioRunId?: string };
}

/**
 * Reads the scenario correlation id a caller (e.g. produce-burst) stashed in the
 * first message's metadata, so producer.send.end can be correlated the same way
 * producer.send.start is (see collector.ts's extractScenarioRunId).
 */
function extractScenarioRunId(messages: ProducerMessage[]): string | undefined {
  return messages[0]?.metadata?.scenarioRunId;
}

/**
 * Wraps a @platformatic/kafka Producer with a stable clientId (used as actorId) and
 * emits actor.created / actor.destroyed lifecycle events.
 */
export class ProducerActor {
  readonly actorId: string;
  readonly #producer: Producer;
  readonly #eventBus: EventBus;

  constructor(options: ProducerActorOptions) {
    this.actorId = options.clientId;
    this.#eventBus = options.eventBus;
    this.#producer = new Producer({
      clientId: options.clientId,
      bootstrapBrokers: options.bootstrapBrokers,
    });

    // Must be registered even if onClientError is not provided: see this option's doc.
    this.#producer.on("error", (error) => {
      options.onClientError?.(error);
    });

    this.#publishActorEvent("actor.created");
  }

  /**
   * Publishes producer.send.end itself using the resolved ProduceResult, rather than
   * relying on the producer:sends:end diagnostics_channel: empirically, that channel's
   * context never carries a `result` field (only producer:sends:start does), unlike
   * what Node's tracingChannel.traceCallback does for typical (err, result) callbacks.
   * producer.send.start is still sourced from the diagnostics_channel via collector.ts.
   */
  async send(messages: ProducerMessage[]): Promise<void> {
    const result = await this.#producer.send({ messages });
    const topic = messages[0]?.topic;
    if (topic === undefined) {
      return;
    }

    const scenarioRunId = extractScenarioRunId(messages);

    this.#eventBus.publish({
      id: ulid(),
      ts: Date.now(),
      theme: "kafka",
      source: { kind: "client", actorId: this.actorId },
      type: "producer.send.end",
      payload: {
        topic,
        offsets: (result.offsets ?? []).map((offset) => ({
          partition: offset.partition,
          offset: bigintOffsetToNumber(offset.offset),
        })),
        ...(scenarioRunId === undefined ? {} : { scenarioRunId }),
      },
    });
  }

  async close(): Promise<void> {
    await this.#producer.close();
    this.#publishActorEvent("actor.destroyed");
  }

  /** Test/observability hook: whether an 'error' listener is attached to the
   * underlying Producer. Must always be true -- see onClientError's doc for the
   * process crash this listener prevents. */
  hasErrorListener(): boolean {
    return this.#producer.listenerCount("error") > 0;
  }

  /** Test-only hook: emits a real 'error' event on the underlying Producer, so tests
   * can simulate the internal-library-error scenario onClientError guards against. */
  emitClientErrorForTest(error: Error): void {
    this.#producer.emit("error", error);
  }

  #publishActorEvent(type: "actor.created" | "actor.destroyed"): void {
    this.#eventBus.publish({
      id: ulid(),
      ts: Date.now(),
      theme: "kafka",
      source: { kind: "scenario", actorId: this.actorId },
      type,
      payload: { actorKind: "producer", actorId: this.actorId },
    });
  }
}
