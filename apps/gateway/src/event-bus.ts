import type { GlassboxEvent } from "@glassbox/schema";

export interface EventBusOptions {
  capacity: number;
}

export type EventBusListener = (event: GlassboxEvent) => void;

export type GetSinceResult =
  | { gap: false; events: GlassboxEvent[] }
  | { gap: true; events: GlassboxEvent[] };

/**
 * In-memory event bus with a ring buffer of recent events, keyed by a monotonically
 * increasing seq. Used to resync SSE clients after a reconnect (see ADR-0002).
 */
export class EventBus {
  #capacity: number;
  #buffer: GlassboxEvent[] = [];
  #nextSeq = 0;
  #listeners = new Set<EventBusListener>();

  constructor(options: EventBusOptions) {
    this.#capacity = options.capacity;
  }

  publish(event: Omit<GlassboxEvent, "seq">): GlassboxEvent {
    const published: GlassboxEvent = { ...event, seq: this.#nextSeq };
    this.#nextSeq += 1;

    this.#buffer.push(published);
    if (this.#buffer.length > this.#capacity) {
      this.#buffer.shift();
    }

    for (const listener of this.#listeners) {
      listener(published);
    }

    return published;
  }

  subscribe(listener: EventBusListener): () => void {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  getSince(lastSeq: number): GetSinceResult {
    const oldest = this.#buffer[0];
    if (oldest === undefined) {
      return { gap: false, events: [] };
    }

    if (lastSeq < oldest.seq - 1) {
      return { gap: true, events: [...this.#buffer] };
    }

    return { gap: false, events: this.#buffer.filter((event) => event.seq > lastSeq) };
  }

  /**
   * Atomically combines getSince + subscribe so an SSE reconnect can never lose an
   * event published in the window between reading the backlog and registering a live
   * subscriber. A temporary buffering listener is registered first (before the backlog
   * snapshot is even read); once the snapshot is taken, anything the buffering listener
   * captured that isn't already covered by the snapshot is replayed before handing off
   * to the caller's real listener.
   */
  getSinceAndSubscribe(
    lastSeq: number,
    listener: EventBusListener,
  ): { result: GetSinceResult; unsubscribe: () => void } {
    const buffered: GlassboxEvent[] = [];
    const unsubscribeBuffering = this.subscribe((event) => buffered.push(event));

    const result = this.getSince(lastSeq);
    unsubscribeBuffering();

    const lastDeliveredSeq = result.events.at(-1)?.seq ?? lastSeq;
    const missed = buffered.filter((event) => event.seq > lastDeliveredSeq);

    const unsubscribe = this.subscribe(listener);
    for (const event of missed) {
      listener(event);
    }

    return { result, unsubscribe };
  }
}
