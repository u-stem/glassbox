import type { EventBus } from "../../../event-bus";
import { ConsumerActor, type ConsumerActorOptions } from "./consumer-actor";

/**
 * The subset of ConsumerActor's public surface the registry depends on. Extracted as
 * an interface (rather than depending on the concrete class directly) so tests can
 * inject a fake actor and a synchronous scheduler to exercise the kill-mode watchdog
 * logic without a real broker or real elapsed time.
 */
export interface ConsumerActorLike {
  readonly actorId: string;
  readonly groupId: string;
  startConsuming(topics: string[]): Promise<void>;
  close(mode: "graceful" | "kill"): Promise<void>;
  setProcessingDelay(ms: number): void;
  setJoinDelay(ms: number): void;
  getSessionTimeoutMs(): number;
  finalizeKillTeardown(): Promise<void>;
}

/** Extra time to wait, beyond the configured session timeout, before finalizing a
 * killed consumer's teardown -- covers scheduling jitter and the broker's own
 * heartbeat-check interval so the session has certainly already expired. */
const KILL_TEARDOWN_MARGIN_MS = 2000;

/** Upper bound on concurrently-registered consumers, per security-auditor's finding:
 * without a cap, repeated add-consumer scenario dispatches could exhaust broker
 * connections / process resources with no legitimate learning-scenario need for more
 * than a handful of consumers at once. */
export const MAX_CONSUMERS = 32;

/** Thrown by add() when the registry is already at MAX_CONSUMERS; the add-consumer
 * scenario checks capacity synchronously before calling add() (see its isAtCapacity),
 * so this is a defense-in-depth guard rather than the primary enforcement point. */
export class ConsumerCapacityExceededError extends Error {
  constructor() {
    super(`consumer registry is at capacity (max ${MAX_CONSUMERS})`);
    this.name = "ConsumerCapacityExceededError";
  }
}

export interface ConsumerRegistryOptions {
  bootstrapBrokers: string[];
  eventBus: EventBus;
  onConsumeError?: (clientId: string, error: unknown) => void;
  /** Errors from the kill-mode teardown watchdog (finalizeKillTeardown itself already
   * swallows its own close() failure; this only fires for unexpected errors). */
  onTeardownError?: (clientId: string, error: unknown) => void;
  /** Forwarded to every ConsumerActor's onClientError (see its doc): required to
   * prevent an internal @platformatic/kafka 'error' emission from crashing the whole
   * gateway process. */
  onClientError?: (clientId: string, error: unknown) => void;
  /** Factory for the underlying actor, defaulting to `new ConsumerActor(options)`.
   * Overridable for tests. */
  createActor?: (options: ConsumerActorOptions) => ConsumerActorLike;
  /** Schedules a delayed callback, defaulting to `setTimeout`. Overridable for tests
   * so the kill-mode watchdog can be exercised without waiting real time. */
  scheduleDelayed?: (fn: () => void, delayMs: number) => void;
}

export interface AddConsumerOptions {
  groupId: string;
  topics: string[];
  /** Overrides forwarded to ConsumerActor; see its doc (mainly for shortening the
   * session timeout in integration tests of the "kill" removal mode). */
  sessionTimeoutMs?: number;
  rebalanceTimeoutMs?: number;
  heartbeatIntervalMs?: number;
}

export interface ConsumerRegistry {
  add(options: AddConsumerOptions): ConsumerActorLike;
  get(clientId: string): ConsumerActorLike | undefined;
  remove(clientId: string, mode: "graceful" | "kill"): Promise<void>;
  all(): ConsumerActorLike[];
  /** Whether the registry is at MAX_CONSUMERS; checked by the add-consumer scenario
   * before calling add() so the HTTP layer can respond 422 synchronously. */
  isFull(): boolean;
  /**
   * Applies the slow-motion join delay to every currently-registered consumer and
   * remembers it for consumers added afterwards (see ConsumerActor's protocolsMetadata
   * hook doc for why this specifically stretches PreparingRebalance).
   */
  setSlowMotion(joinDelayMs: number): void;
}

/**
 * Holds all live ConsumerActor instances for the kafka theme, auto-numbering
 * clientIds (consumer-1, consumer-2, ...) per the add-consumer scenario's contract
 * ("clientId 自動採番").
 *
 * On a 'kill' removal, the actor is removed from the registry and its connections are
 * severed immediately (see ConsumerActor#close), but its in-process Consumer instance
 * is only fully released after a delayed watchdog fires (sessionTimeoutMs + margin):
 * releasing it earlier would let @platformatic/kafka attempt a real LeaveGroup before
 * the broker has actually expired the session, which is inconsistent with "kill"
 * semantics (no LeaveGroup should ever reach the broker for a killed member). Without
 * this watchdog, the killed actor's internal heartbeat-retry loop would keep running
 * against the (permanently blocked) connection for the remaining lifetime of the
 * gateway process -- see ConsumerActor#finalizeKillTeardown's doc.
 */
export function createConsumerRegistry(options: ConsumerRegistryOptions): ConsumerRegistry {
  const consumers = new Map<string, ConsumerActorLike>();
  const createActor = options.createActor ?? ((actorOptions) => new ConsumerActor(actorOptions));
  const scheduleDelayed =
    options.scheduleDelayed ?? ((fn: () => void, delayMs: number) => void setTimeout(fn, delayMs));
  let nextIndex = 1;
  let joinDelayMs = 0;

  return {
    add({ groupId, topics, sessionTimeoutMs, rebalanceTimeoutMs, heartbeatIntervalMs }) {
      if (consumers.size >= MAX_CONSUMERS) {
        throw new ConsumerCapacityExceededError();
      }

      const clientId = `consumer-${nextIndex}`;
      nextIndex += 1;

      const actor = createActor({
        clientId,
        groupId,
        topics,
        bootstrapBrokers: options.bootstrapBrokers,
        eventBus: options.eventBus,
        joinDelayMs,
        onClientError: (error) => {
          options.onClientError?.(clientId, error);
        },
        ...(sessionTimeoutMs === undefined ? {} : { sessionTimeoutMs }),
        ...(rebalanceTimeoutMs === undefined ? {} : { rebalanceTimeoutMs }),
        ...(heartbeatIntervalMs === undefined ? {} : { heartbeatIntervalMs }),
      });
      consumers.set(clientId, actor);

      // Reported but deliberately NOT unregistered. Auto-removing the actor here was
      // tried while adding the broker stop/start buttons (ADR-0005), because a consumer
      // whose stream rejected does not retry and thus lingers as a member that no
      // longer consumes. It was reverted: this same rejection also arrives as a plain
      // `TimeoutError` during an ordinary rebalance (measured on every run of the
      // lesson B walkthrough), so unregistering on it deletes healthy consumers the
      // user just added -- a worse failure than the lingering one it fixed. Recovering
      // from a stopped broker is therefore manual (remove the consumer and add it
      // again); see docs/themes/kafka.md's 既知の限界.
      void actor.startConsuming(topics).catch((error: unknown) => {
        options.onConsumeError?.(clientId, error);
      });

      return actor;
    },

    get(clientId) {
      return consumers.get(clientId);
    },

    async remove(clientId, mode) {
      const actor = consumers.get(clientId);
      if (actor === undefined) {
        return;
      }
      consumers.delete(clientId);
      await actor.close(mode);

      if (mode === "kill") {
        const delayMs = actor.getSessionTimeoutMs() + KILL_TEARDOWN_MARGIN_MS;
        scheduleDelayed(() => {
          actor.finalizeKillTeardown().catch((error: unknown) => {
            options.onTeardownError?.(clientId, error);
          });
        }, delayMs);
      }
    },

    all() {
      return [...consumers.values()];
    },

    isFull() {
      return consumers.size >= MAX_CONSUMERS;
    },

    setSlowMotion(nextJoinDelayMs) {
      joinDelayMs = nextJoinDelayMs;
      for (const actor of consumers.values()) {
        actor.setJoinDelay(nextJoinDelayMs);
      }
    },
  };
}
