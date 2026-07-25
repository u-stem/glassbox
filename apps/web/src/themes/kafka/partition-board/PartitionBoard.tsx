"use client";

import type { AdminSnapshotPayload } from "@glassbox/schema";
import { scaleLinear } from "d3-scale";
import { memo, useMemo, useState } from "react";
import { partitionColorVar } from "../colors";
import type { WorldGroup } from "../world-reducer";
import { buildPartitionRows, computeLagBand, computeWindow, type PartitionRow } from "./layout";

const WINDOW_SIZE = 50;
const LANE_HEIGHT = 36;
const TRACK_HEIGHT = 6;
const LABEL_WIDTH = 32;
const CHART_PADDING = 12;
/** Internal coordinate system the layout math is computed in; the <svg> below scales
 * this to its actual rendered width via viewBox rather than a fixed pixel width, so
 * the board never overflows a narrower card (see the Phase 3 review). */
const DESIGN_WIDTH = 480;

interface TooltipState {
  partition: number;
  end: number;
  committed: number | undefined;
  groupId: string | undefined;
  lag: number | undefined;
  x: number;
  y: number;
}

/**
 * 3a: partition/offset board. One horizontal lane per partition, auto-following the
 * tail of the log (see layout.ts:computeWindow). The gap between a group's committed
 * offset and the log end offset is drawn as a wash band -- lag made visible as area,
 * not just a number. Per marks-and-anatomy.md: thin marks, a 2px surface ring on
 * markers, direct labels only at the two values that matter (end, committed), the
 * rest reachable via tooltip.
 *
 * Wrapped in `memo` below: this component takes no store subscription of its own (it
 * doesn't need the raw event timeline), so without memoizing it would still re-render
 * on every SSE event purely because its parent page component re-renders for
 * Timeline/TopologyCanvas, which do need the timeline (see the Phase 3 review).
 */
function PartitionBoardImpl({
  topic,
  groups,
}: {
  topic: AdminSnapshotPayload["topics"][number] | undefined;
  groups: readonly WorldGroup[];
}) {
  const [tooltip, setTooltip] = useState<TooltipState | undefined>(undefined);

  const rows = useMemo<PartitionRow[]>(
    () => (topic === undefined ? [] : buildPartitionRows(topic, groups)),
    [topic, groups],
  );

  const window_ = useMemo(
    () =>
      computeWindow(
        rows.map((row) => row.end),
        WINDOW_SIZE,
      ),
    [rows],
  );

  const plotWidth = DESIGN_WIDTH - LABEL_WIDTH - CHART_PADDING * 2;
  const scale = useMemo(
    () => scaleLinear().domain([window_.start, window_.end]).range([0, plotWidth]).clamp(true),
    [window_, plotWidth],
  );

  const height = rows.length * LANE_HEIGHT + CHART_PADDING * 2;

  if (topic === undefined) {
    return (
      <p className="viz-root text-sm" style={{ color: "var(--text-secondary)" }}>
        No admin.snapshot received yet.
      </p>
    );
  }

  return (
    <div className="viz-root relative">
      <svg
        role="img"
        aria-label={`${topic.name} partition/offset board`}
        viewBox={`0 0 ${DESIGN_WIDTH} ${height}`}
        width="100%"
        height={height}
        style={{ background: "var(--surface-1)", display: "block" }}
      >
        {rows.map((row, rowIndex) => {
          const laneY = CHART_PADDING + rowIndex * LANE_HEIGHT;
          const color = partitionColorVar(row.partition);
          const endX = scale(row.end);
          // Only label a committed marker directly when there's visible lag to explain
          // -- otherwise it sits right on top of (or very near) the end label and just
          // duplicates it (see the Phase 3 review); the tooltip always carries the
          // exact value regardless.
          const lastMarkerWithLag = [...row.committedByGroup]
            .reverse()
            .find((marker) => (marker.lag ?? 0) > 0);

          return (
            <g key={row.partition} transform={`translate(0, ${laneY})`}>
              <text
                x={0}
                y={LANE_HEIGHT / 2}
                dy="0.35em"
                fontSize={11}
                fill="var(--text-secondary)"
              >
                P{row.partition}
              </text>

              <g transform={`translate(${LABEL_WIDTH}, 0)`}>
                {/* baseline track: the produced range within the current window */}
                <rect
                  x={0}
                  y={LANE_HEIGHT / 2 - TRACK_HEIGHT / 2}
                  width={plotWidth}
                  height={TRACK_HEIGHT}
                  rx={TRACK_HEIGHT / 2}
                  fill="var(--gridline)"
                />

                {row.committedByGroup.map((marker) => {
                  const band = computeLagBand(marker.committed, row.end, window_.start);
                  const committedX = scale(marker.committed);
                  return (
                    <g key={marker.groupId}>
                      {band && (
                        <rect
                          x={scale(band.from)}
                          y={LANE_HEIGHT / 2 - TRACK_HEIGHT / 2}
                          width={scale(band.to) - scale(band.from)}
                          height={TRACK_HEIGHT}
                          fill={color}
                          opacity={0.1}
                        />
                      )}
                      {/* biome-ignore lint/a11y/useSemanticElements: <button> is not a valid
                          SVG element; role/tabIndex/focus already give this the same
                          keyboard and AT support. */}
                      <circle
                        cx={committedX}
                        cy={LANE_HEIGHT / 2}
                        r={5}
                        fill={color}
                        stroke="var(--surface-1)"
                        strokeWidth={2}
                        role="button"
                        aria-label={`${marker.groupId}: committed ${marker.committed}, lag ${marker.lag ?? "unknown"}`}
                        tabIndex={0}
                        style={{ cursor: "pointer" }}
                        onMouseEnter={(e) =>
                          setTooltip({
                            partition: row.partition,
                            end: row.end,
                            committed: marker.committed,
                            groupId: marker.groupId,
                            lag: marker.lag,
                            x: e.clientX,
                            y: e.clientY,
                          })
                        }
                        onMouseLeave={() => setTooltip(undefined)}
                        onFocus={(e) =>
                          setTooltip({
                            partition: row.partition,
                            end: row.end,
                            committed: marker.committed,
                            groupId: marker.groupId,
                            lag: marker.lag,
                            x: e.currentTarget.getBoundingClientRect().x,
                            y: e.currentTarget.getBoundingClientRect().y,
                          })
                        }
                        onBlur={() => setTooltip(undefined)}
                      >
                        <title>{`${marker.groupId}: committed ${marker.committed}, lag ${marker.lag ?? "--"}`}</title>
                      </circle>
                      {/* selective direct label: only the (last) marker that actually has
                          visible lag gets its committed value inline -- see
                          lastMarkerWithLag above. */}
                      {lastMarkerWithLag?.groupId === marker.groupId && (
                        <text
                          x={committedX}
                          y={LANE_HEIGHT / 2 - 8}
                          fontSize={10}
                          textAnchor="middle"
                          fill="var(--text-secondary)"
                        >
                          {marker.committed}
                        </text>
                      )}
                    </g>
                  );
                })}

                {row.committedByGroup.length === 0 && (
                  <text
                    x={plotWidth / 2}
                    y={LANE_HEIGHT / 2 - 8}
                    fontSize={10}
                    textAnchor="middle"
                    fill="var(--text-muted)"
                  >
                    –
                  </text>
                )}

                {/* end-of-log marker, always drawn last so it sits on top */}
                {/* biome-ignore lint/a11y/useSemanticElements: <button> is not a valid SVG
                    element; role/tabIndex/focus already give this the same keyboard and
                    AT support. */}
                <circle
                  cx={endX}
                  cy={LANE_HEIGHT / 2}
                  r={5}
                  fill={color}
                  stroke="var(--surface-1)"
                  strokeWidth={2}
                  role="button"
                  aria-label={`partition ${row.partition}: end ${row.end}`}
                  tabIndex={0}
                  style={{ cursor: "pointer" }}
                  onMouseEnter={(e) =>
                    setTooltip({
                      partition: row.partition,
                      end: row.end,
                      committed: undefined,
                      groupId: undefined,
                      lag: undefined,
                      x: e.clientX,
                      y: e.clientY,
                    })
                  }
                  onMouseLeave={() => setTooltip(undefined)}
                  onFocus={(e) =>
                    setTooltip({
                      partition: row.partition,
                      end: row.end,
                      committed: undefined,
                      groupId: undefined,
                      lag: undefined,
                      x: e.currentTarget.getBoundingClientRect().x,
                      y: e.currentTarget.getBoundingClientRect().y,
                    })
                  }
                  onBlur={() => setTooltip(undefined)}
                >
                  <title>{`partition ${row.partition}: end ${row.end}`}</title>
                </circle>
                <text
                  x={endX}
                  y={LANE_HEIGHT / 2 + 16}
                  fontSize={10}
                  textAnchor="middle"
                  fill="var(--text-secondary)"
                >
                  {row.end}
                </text>
              </g>
            </g>
          );
        })}
      </svg>

      {tooltip && (
        <div
          role="tooltip"
          className="pointer-events-none fixed z-10 rounded px-2 py-1.5 text-xs shadow"
          style={{
            left: tooltip.x + 12,
            top: tooltip.y + 12,
            background: "var(--surface-1)",
            color: "var(--text-primary)",
            border: "1px solid var(--border)",
          }}
        >
          <p>
            partition <strong>{tooltip.partition}</strong>
          </p>
          <p>
            end <strong>{tooltip.end}</strong>
          </p>
          {tooltip.groupId !== undefined && (
            <>
              <p>
                {tooltip.groupId} committed <strong>{tooltip.committed}</strong>
              </p>
              <p>
                lag <strong>{tooltip.lag ?? "–"}</strong>
              </p>
            </>
          )}
        </div>
      )}
    </div>
  );
}

export const PartitionBoard = memo(PartitionBoardImpl);
