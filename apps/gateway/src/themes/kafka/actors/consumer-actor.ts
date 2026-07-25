import { Consumer, GroupProtocols } from "@platformatic/kafka";
import { ulid } from "ulid";
import type { EventBus } from "../../../event-bus";

export interface ConsumerActorOptions {
  clientId: string;
  groupId: string;
  topics: string[];
  bootstrapBrokers: string[];
  eventBus: EventBus;
  /** Artificial delay (ms) awaited between consuming each message. Adjustable at
   * runtime via setProcessingDelay for the slow-consumer scenario. */
  processingDelayMs?: number;
  /** When set, delays this actor's joinGroup protocol metadata resolution by this
   * many ms (see #joinDelayProvider below for why this is a legitimate hook). Used by
   * the slow-motion toggle to stretch PreparingRebalance to a human-observable length.
   * Adjustable at runtime via setJoinDelay. */
  joinDelayMs?: number;
  /** Overrides for the group's session/rebalance/heartbeat timeouts (@platformatic/kafka
   * defaults: 60_000 / 102_000 / 3000 ms). Primarily used by integration tests to keep
   * a "kill" scenario's session-timeout wait short instead of the 1-minute default. */
  sessionTimeoutMs?: number;
  rebalanceTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  /** Called whenever the underlying Consumer emits a plain 'error' event. Optional,
   * but the listener itself is ALWAYS registered regardless of whether a callback is
   * given: @platformatic/kafka clients extend Node's EventEmitter, and Node throws
   * (crashing the entire process) when 'error' is emitted with zero listeners
   * attached. This is exactly what caused a real gateway crash under repeated
   * add-consumer/remove-consumer cycling: a killed consumer's internal
   * heartbeat-error -> rejoinGroup retry loop (kPerformWithRetry) eventually
   * exhausts its retry budget and calls `emitWithDebug(null, 'error', error)` (a bare
   * `this.emit('error', error)`), which is unrelated to and not caught by any
   * try/catch around consume()/startConsuming(). The crash would land on whichever
   * request happened to be in flight at that moment (e.g. an unrelated actor's own
   * join), which is why the symptom looked like a random "stuck in joining" rather
   * than an obviously-attributable kill-related failure. */
  onClientError?: (error: unknown) => void;
}

function keyToString(key: Buffer | undefined): string | null {
  return key === undefined ? null : key.toString("utf8");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** @platformatic/kafka's own default (dist/clients/consumer/options.js), used when
 * the caller doesn't override sessionTimeoutMs. */
const DEFAULT_SESSION_TIMEOUT_MS = 60_000;

/**
 * Wraps a @platformatic/kafka Consumer, pinned to groupProtocol: 'classic' (per
 * ADR-0001/spike, so the group state machine matches the UI's 4-state model rather
 * than KIP-848's).
 *
 * Event sourcing split (documented per the task's requirement to avoid double
 * publishing): this actor subscribes to the Consumer *instance*'s EventEmitter events
 * (`consumer:group:join/leave/rebalance`, `consumer:heartbeat:*`), since attribution is
 * unambiguous there (spike: "帰属問題は原理的に発生しない"). The `joining` (joinGroup
 * start) and `syncing` (syncGroup start) member-level edges have no EventEmitter
 * equivalent and are instead sourced from the `consumer:group` diagnostics_channel in
 * collector.ts. `consumer:group:rejoin` (fires with no payload) and the `leaveGroup`
 * diagnostics_channel operation are intentionally NOT wired to anything: `left` is
 * published from the EventEmitter `consumer:group:leave` instead, so wiring the
 * diagnostics_channel's `leaveGroup` operation too would double-publish the same edge.
 */
export class ConsumerActor {
  readonly actorId: string;
  readonly groupId: string;
  readonly #consumer: Consumer;
  readonly #eventBus: EventBus;
  readonly #sessionTimeoutMs: number;
  #processingDelayMs: number;
  #joinDelayMs: number;
  #closed = false;
  #blockReconnect:
    | ((payload: { connection: { socket: { destroy: () => void } } }) => void)
    | undefined;

  constructor(options: ConsumerActorOptions) {
    this.actorId = options.clientId;
    this.groupId = options.groupId;
    this.#eventBus = options.eventBus;
    this.#processingDelayMs = options.processingDelayMs ?? 0;
    this.#joinDelayMs = options.joinDelayMs ?? 0;
    this.#sessionTimeoutMs = options.sessionTimeoutMs ?? DEFAULT_SESSION_TIMEOUT_MS;

    this.#consumer = new Consumer({
      clientId: options.clientId,
      groupId: options.groupId,
      bootstrapBrokers: options.bootstrapBrokers,
      groupProtocol: GroupProtocols.CLASSIC,
      ...(options.sessionTimeoutMs === undefined
        ? {}
        : { sessionTimeout: options.sessionTimeoutMs }),
      ...(options.rebalanceTimeoutMs === undefined
        ? {}
        : { rebalanceTimeout: options.rebalanceTimeoutMs }),
      ...(options.heartbeatIntervalMs === undefined
        ? {}
        : { heartbeatInterval: options.heartbeatIntervalMs }),
      // A real (not simulated) hook into the join protocol: protocolsMetadata may
      // return a Promise, resolved *before* this actor's JoinGroupRequest is sent
      // (see @platformatic/kafka's Consumer#createJoinGroupProtocols /
      // #resolveProtocolsMetadata). Delaying here genuinely stretches the wall-clock
      // time the broker (and other members) observe this member spending in
      // PreparingRebalance, unlike a delay placed purely in our own code after the
      // fact. There is no equivalent hook for the syncGroup phase (partitionAssigner
      // is invoked synchronously), so slow-motion cannot stretch CompletingRebalance
      // the same way; see the Phase 2 report for this limitation.
      protocolsMetadata: (_protocols, _topics, _metadata) => {
        if (this.#joinDelayMs > 0) {
          return sleep(this.#joinDelayMs).then(() => Buffer.alloc(0));
        }
        return Buffer.alloc(0);
      },
    });

    // Must be registered even if onClientError is not provided: see this option's
    // doc for the crash this prevents.
    this.#consumer.on("error", (error) => {
      options.onClientError?.(error);
    });

    this.#wireGroupEvents();
    this.#publishActorEvent("actor.created");
  }

  setProcessingDelay(ms: number): void {
    this.#processingDelayMs = ms;
  }

  setJoinDelay(ms: number): void {
    this.#joinDelayMs = ms;
  }

  /** Configured (or library-default) session timeout, in ms. Used by
   * consumer-registry's kill-mode watchdog to know how long to wait before the
   * broker will have expired this member's session. */
  getSessionTimeoutMs(): number {
    return this.#sessionTimeoutMs;
  }

  /** Test/observability hook: whether close("kill") has registered the
   * reconnect-blocking listener that finalizeKillTeardown() is responsible for
   * removing. Used to verify the listener doesn't outlive teardown. */
  isBlockingReconnect(): boolean {
    return this.#blockReconnect !== undefined;
  }

  /** Test/observability hook: whether an 'error' listener is attached to the
   * underlying Consumer. Must always be true -- see onClientError's doc for the
   * process crash this listener prevents. */
  hasErrorListener(): boolean {
    return this.#consumer.listenerCount("error") > 0;
  }

  /** Test-only hook: emits a real 'error' event on the underlying Consumer, so tests
   * can simulate the internal-library-error scenario onClientError guards against
   * without needing a real broker to actually exhaust a retry loop. */
  emitClientErrorForTest(error: Error): void {
    this.#consumer.emit("error", error);
  }

  #wireGroupEvents(): void {
    this.#consumer.on("consumer:group:join", (payload) => {
      this.#eventBus.publish({
        id: ulid(),
        ts: Date.now(),
        theme: "kafka",
        source: { kind: "client", actorId: this.actorId },
        type: "consumer.group.joined",
        payload: {
          groupId: payload.groupId,
          clientId: this.actorId,
          memberId: payload.memberId,
          assignments: payload.assignments ?? [],
          // Not defaulted to 0/false: those are misleadingly meaningful values for
          // "unknown" (see ConsumerGroupJoinedPayload's doc).
          ...(payload.generationId === undefined ? {} : { generationId: payload.generationId }),
          ...(payload.isLeader === undefined ? {} : { isLeader: payload.isLeader }),
        },
      });
    });

    this.#consumer.on("consumer:group:leave", (payload) => {
      this.#eventBus.publish({
        id: ulid(),
        ts: Date.now(),
        theme: "kafka",
        source: { kind: "client", actorId: this.actorId },
        type: "consumer.group.left",
        payload: {
          groupId: payload.groupId,
          clientId: this.actorId,
          memberId: payload.memberId ?? null,
        },
      });
    });

    this.#consumer.on("consumer:group:rebalance", (payload) => {
      this.#eventBus.publish({
        id: ulid(),
        ts: Date.now(),
        theme: "kafka",
        source: { kind: "client", actorId: this.actorId },
        type: "consumer.group.rebalancing",
        payload: { groupId: payload.groupId, clientId: this.actorId },
      });
    });

    this.#consumer.on("consumer:heartbeat:end", (payload) => {
      this.#eventBus.publish({
        id: ulid(),
        ts: Date.now(),
        theme: "kafka",
        source: { kind: "client", actorId: this.actorId },
        type: "consumer.heartbeat.ok",
        payload: {
          groupId: payload?.groupId,
          clientId: this.actorId,
          memberId: payload?.memberId,
          generationId: payload?.generationId,
        },
      });
    });

    this.#consumer.on("consumer:heartbeat:error", (payload) => {
      this.#eventBus.publish({
        id: ulid(),
        ts: Date.now(),
        theme: "kafka",
        source: { kind: "client", actorId: this.actorId },
        type: "consumer.heartbeat.failed",
        payload: {
          groupId: payload.groupId,
          clientId: this.actorId,
          memberId: payload.memberId,
          generationId: payload.generationId,
          error: payload.error.message,
        },
      });
    });
  }

  /**
   * Starts consuming in the background. Runs until close() is called or the stream
   * ends (e.g. a kill-mode remove-consumer causes the underlying connection to drop);
   * does not retry automatically, mirroring produce-burst's single-flight-run style.
   *
   * Deliberately autocommit: false, committing each message explicitly *after* this
   * loop's own processing (including the artificial processingDelayMs sleep) rather
   * than relying on the library's autocommit, which runs on its own interval
   * independent of how far this loop has actually gotten. With autocommit, the
   * broker-visible committed offset (what admin-poller's lag is computed from) could
   * race ahead of a "slow" consumer's real processing progress, making the
   * slow-consumer scenario's whole point -- visibly showing lag build up while a
   * consumer struggles to keep up -- unreliable or altogether invisible. Per-message
   * commit is deliberately not batched: this gateway's message volumes are demo-scale,
   * and correctness of the visible lag matters far more here than commit throughput.
   */
  async startConsuming(topics: string[]): Promise<void> {
    const stream = await this.#consumer.consume({ topics, mode: "latest", autocommit: false });

    for await (const message of stream) {
      if (this.#closed) {
        break;
      }

      this.#eventBus.publish({
        id: ulid(),
        ts: Date.now(),
        theme: "kafka",
        source: { kind: "client", actorId: this.actorId },
        type: "consumer.message",
        payload: {
          groupId: this.groupId,
          clientId: this.actorId,
          topic: message.topic,
          partition: message.partition,
          offset: Number(message.offset),
          key: keyToString(message.key),
        },
      });

      if (this.#processingDelayMs > 0) {
        await sleep(this.#processingDelayMs);
      }

      await message.commit();

      this.#eventBus.publish({
        id: ulid(),
        ts: Date.now(),
        theme: "kafka",
        source: { kind: "client", actorId: this.actorId },
        type: "consumer.commit",
        payload: {
          groupId: this.groupId,
          clientId: this.actorId,
          // The position committed is the *next* offset to read, matching what
          // admin-poller's committedOffset (from the broker's own bookkeeping) means.
          offsets: [
            {
              topic: message.topic,
              partition: message.partition,
              offset: Number(message.offset) + 1,
            },
          ],
        },
      });
    }
  }

  /**
   * mode: 'graceful' sends LeaveGroup (force close, per the spike: `close(true)` still
   * sends a LeaveGroup request; it is not a "no leave" close). mode: 'kill' simulates a
   * dropped connection: no LeaveGroup is sent, so from the broker's perspective the
   * member is only removed once its session times out.
   *
   * 'kill' does NOT fully release this actor's in-process resources by itself (the
   * underlying Consumer's internal heartbeat-retry loop keeps running against the
   * blocked connection, per @platformatic/kafka's classic-protocol #heartbeat, which
   * reschedules itself even on a plain connection error). That full release is a
   * separate, deliberately-delayed step: see finalizeKillTeardown() and its caller
   * (consumer-registry's watchdog), which waits out the session timeout first so the
   * broker-visible "kill" semantics (no LeaveGroup ever sent) are preserved.
   */
  async close(mode: "graceful" | "kill"): Promise<void> {
    this.#closed = true;
    if (mode === "graceful") {
      await this.#consumer.close(true);
    } else {
      // Deliberately bypasses Consumer#close (and therefore LeaveGroup). A single
      // socket.destroy() on the connections underneath the public `connections`
      // ConnectionPool getter is NOT enough: verified against a real broker that the
      // pool transparently reconnects the next time the consumer needs a connection
      // (e.g. for its next heartbeat), so heartbeats kept succeeding and no rebalance
      // was ever triggered. Instead, every currently-open connection is destroyed AND
      // the pool's public `connect` event (ConnectionPoolEvents, emitted whenever it
      // establishes a new one) is subscribed to destroy any future reconnection
      // attempt too, so no Kafka request can ever complete again. Only then does the
      // broker fall back to the session timeout to detect the member is gone. This
      // reproduces "kill" semantics distinct from close(force), which still sends
      // LeaveGroup; see the Phase 2 report for the reconnect pitfall this fixes.
      for (const [, connection] of this.#consumer.connections) {
        connection.socket.destroy();
      }
      this.#blockReconnect = ({ connection }) => {
        connection.socket.destroy();
      };
      this.#consumer.connections.on("connect", this.#blockReconnect);

      // Deliberately NOT consumer.group.left: from the broker's point of view this
      // member is still part of the group until its session times out, and pretending
      // it already left would be a lie about broker state (see ADR-0003 / the Phase 2
      // review). consumer.connection.lost lets the UI show an explicit "connection
      // lost, awaiting session timeout" state instead of either hiding the member or
      // making it flicker away-and-back across admin.snapshot polls.
      this.#eventBus.publish({
        id: ulid(),
        ts: Date.now(),
        theme: "kafka",
        source: { kind: "scenario", actorId: this.actorId },
        type: "consumer.connection.lost",
        payload: { groupId: this.groupId, clientId: this.actorId, reason: "kill" },
      });
    }
    this.#publishActorEvent("actor.destroyed");
  }

  /**
   * Fully releases this actor's in-process Consumer resources after a 'kill'. Must
   * only be called once the broker has actually expired the session (see
   * getSessionTimeoutMs() + consumer-registry's watchdog margin): calling
   * consumer.close(true) triggers @platformatic/kafka's internal
   * force-close-streams -> #performLeaveGroup path, which synchronously cancels the
   * heartbeat retry timer (clearTimeout) *before* attempting the network LeaveGroup
   * round-trip. That network round-trip is expected to fail or be rejected by the
   * broker (the member no longer exists there), and its outcome is irrelevant to this
   * method's actual goal, which is purely to stop the in-process retry loop; hence the
   * broad catch. The reconnect-blocking listener is removed first so it doesn't
   * interfere with (or outlive) this final close attempt.
   */
  async finalizeKillTeardown(): Promise<void> {
    if (this.#blockReconnect !== undefined) {
      this.#consumer.connections.off("connect", this.#blockReconnect);
      this.#blockReconnect = undefined;
    }
    try {
      await this.#consumer.close(true);
    } catch {
      // Best-effort: see this method's doc for why a rejection here is expected and
      // does not indicate the in-process cleanup failed.
    }
  }

  #publishActorEvent(type: "actor.created" | "actor.destroyed"): void {
    this.#eventBus.publish({
      id: ulid(),
      ts: Date.now(),
      theme: "kafka",
      source: { kind: "scenario", actorId: this.actorId },
      type,
      payload: { actorKind: "consumer", actorId: this.actorId },
    });
  }
}
