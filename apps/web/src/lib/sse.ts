import { type GlassboxEvent, glassboxEventSchema } from "@glassbox/schema";

export interface EventSourceLike {
  onMessage(listener: (data: string) => void): void;
  close(): void;
}

/**
 * Thin adapter over the browser's EventSource so that connectKafkaEvents can be
 * unit-tested against a fake EventSourceLike without a DOM.
 */
export function createBrowserEventSource(url: string): EventSourceLike {
  const source = new EventSource(url);
  return {
    onMessage(listener) {
      source.addEventListener("message", (event) => listener(event.data));
    },
    close() {
      source.close();
    },
  };
}

export interface KafkaSseHandlers {
  onEvent: (event: GlassboxEvent) => void;
  onReset: () => void;
}

/**
 * Wires an EventSourceLike to Zod-validated GlassboxEvent handling. control.reset
 * frames (sent by the gateway on a ring-buffer gap, see ADR-0002) are routed to
 * onReset instead of onEvent so the caller can clear its local state.
 */
export function connectKafkaEvents(
  eventSource: EventSourceLike,
  handlers: KafkaSseHandlers,
): () => void {
  eventSource.onMessage((data) => {
    let raw: unknown;
    try {
      raw = JSON.parse(data);
    } catch {
      return;
    }

    const result = glassboxEventSchema.safeParse(raw);
    if (!result.success) {
      return;
    }

    if (result.data.type === "control.reset") {
      handlers.onReset();
      return;
    }

    handlers.onEvent(result.data);
  });

  return () => {
    eventSource.close();
  };
}
