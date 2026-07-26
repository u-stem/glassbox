"use client";

import type { GlassboxEvent } from "@glassbox/schema";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useMemo, useRef, useState } from "react";
import { EMPTY_TIMELINE_FILTER, filterEvents, type TimelineFilter } from "./filter";

const ROW_HEIGHT = 32;
const LIST_HEIGHT = 384;

/**
 * Event timeline: raw event list with a type/actorId filter row and payload detail
 * expansion, virtualized with @tanstack/react-virtual so the 2000-event backlog
 * (lib/store.ts) never renders more DOM nodes than fit on screen.
 */
export function Timeline({ events }: { events: readonly GlassboxEvent[] }) {
  const [filter, setFilter] = useState<TimelineFilter>(EMPTY_TIMELINE_FILTER);
  const [selectedId, setSelectedId] = useState<string | undefined>(undefined);

  const filtered = useMemo(() => filterEvents(events, filter), [events, filter]);
  const selectedEvent = filtered.find((event) => event.id === selectedId);

  const eventTypes = useMemo(() => [...new Set(events.map((e) => e.type))].sort(), [events]);
  const actorIds = useMemo(
    () =>
      [
        ...new Set(
          events.map((e) => e.source.actorId).filter((id): id is string => id !== undefined),
        ),
      ].sort(),
    [events],
  );

  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 12,
  });

  return (
    <div className="viz-root flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-2 text-sm">
        <label className="flex shrink-0 items-center gap-1 whitespace-nowrap">
          種別(type)
          <select
            value={filter.type}
            onChange={(e) => setFilter((f) => ({ ...f, type: e.target.value }))}
            className="rounded border px-1 py-0.5 text-xs"
            style={{ borderColor: "var(--axis)", background: "var(--surface-1)" }}
          >
            <option value="">(all)</option>
            {eventTypes.map((type) => (
              <option key={type} value={type}>
                {type}
              </option>
            ))}
          </select>
        </label>
        <label className="flex shrink-0 items-center gap-1 whitespace-nowrap">
          実行主体(actorId)
          <select
            value={filter.actorId}
            onChange={(e) => setFilter((f) => ({ ...f, actorId: e.target.value }))}
            className="rounded border px-1 py-0.5 text-xs"
            style={{ borderColor: "var(--axis)", background: "var(--surface-1)" }}
          >
            <option value="">(all)</option>
            {actorIds.map((actorId) => (
              <option key={actorId} value={actorId}>
                {actorId}
              </option>
            ))}
          </select>
        </label>
        <span className="text-xs" style={{ color: "var(--text-muted)" }}>
          {filtered.length} / {events.length} 件
        </span>
      </div>

      <div
        ref={parentRef}
        className="overflow-y-auto rounded border text-sm"
        style={{ height: LIST_HEIGHT, borderColor: "var(--gridline)" }}
      >
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {virtualizer.getVirtualItems().map((virtualRow) => {
            const event = filtered[virtualRow.index];
            if (event === undefined) {
              return null;
            }
            const isSelected = selectedId === event.id;
            return (
              <button
                key={event.id}
                type="button"
                onClick={() => setSelectedId(isSelected ? undefined : event.id)}
                className="absolute left-0 top-0 flex w-full items-center gap-2 border-b px-2 text-left"
                style={{
                  height: ROW_HEIGHT,
                  transform: `translateY(${virtualRow.start}px)`,
                  borderColor: "var(--gridline)",
                  background: isSelected ? "var(--page-plane)" : undefined,
                }}
              >
                <span className="text-xs" style={{ color: "var(--text-muted)" }}>
                  {new Date(event.ts).toLocaleTimeString()}
                </span>
                <span className="font-mono text-xs">{event.type}</span>
                {event.source.actorId && (
                  <span className="text-xs" style={{ color: "var(--text-muted)" }}>
                    ({event.source.actorId})
                  </span>
                )}
              </button>
            );
          })}
        </div>
      </div>

      {selectedEvent && (
        <pre
          className="max-h-40 overflow-auto rounded p-2 text-xs"
          style={{ background: "var(--page-plane)", color: "var(--text-primary)" }}
        >
          {JSON.stringify(selectedEvent.payload, null, 2)}
        </pre>
      )}
    </div>
  );
}
