"use client";

import type { GroupState } from "@glassbox/schema";
import { memo } from "react";
import { useKafkaStore } from "@/lib/store";
import type { TransitionHistoryEntry } from "./history";

const NODE_Y = 70;
const NODE_SPACING = 190;
const NODE_START_X = 70;

const NODES: { state: GroupState; x: number; y: number; label: string }[] = [
  { state: "Empty", x: NODE_START_X, y: NODE_Y, label: "Empty" },
  {
    state: "PreparingRebalance",
    x: NODE_START_X + NODE_SPACING,
    y: NODE_Y,
    label: "Preparing\nRebalance",
  },
  {
    state: "CompletingRebalance",
    x: NODE_START_X + NODE_SPACING * 2,
    y: NODE_Y,
    label: "Completing\nRebalance",
  },
  { state: "Stable", x: NODE_START_X + NODE_SPACING * 3, y: NODE_Y, label: "Stable" },
];

/** The 3 forward transitions sit on one straight horizontal line (no crossings).
 * The 4th, Stable -> PreparingRebalance (a fresh member joining a settled group),
 * would double back across that line if drawn straight, so it's rendered as a gentle
 * arc above the row instead (see the `arcEdge` render below). */
const STRAIGHT_EDGES: { from: GroupState; to: GroupState }[] = [
  { from: "Empty", to: "PreparingRebalance" },
  { from: "PreparingRebalance", to: "CompletingRebalance" },
  { from: "CompletingRebalance", to: "Stable" },
];
const ARC_EDGE: { from: GroupState; to: GroupState } = {
  from: "Stable",
  to: "PreparingRebalance",
};

const NODE_RADIUS = 34;
const WIDTH = NODE_START_X * 2 + NODE_SPACING * 3;
const HEIGHT = 170;
const ARC_APEX_Y = NODE_Y - 60;

/** Stable empty-array reference so the store selector below never returns a fresh
 * `[]` when a group has no history yet -- returning a new array every read would
 * make zustand's reference-equality check see "changed" state on every unrelated
 * store update and re-render this component needlessly (see the Phase 3 review). */
const EMPTY_HISTORY: readonly TransitionHistoryEntry[] = [];

function nodeCenter(state: GroupState) {
  const node = NODES.find((n) => n.state === state);
  if (node === undefined) {
    throw new Error(`unknown group state: ${state}`);
  }
  return node;
}

/**
 * 3b: rebalance state transition diagram. The 4 states are always on screen at a
 * fixed layout -- this is a status display (which state is current), not a
 * categorical series, so it deliberately does NOT reuse the partition palette (see
 * colors.ts). Current state uses `--accent`/`--on-accent` (globals.css), tokens kept
 * independent of the `--series-N` categorical slots so a highlighted state is never
 * visually confusable with a partition color; everything else stays ink-muted.
 *
 * Reads `currentState`/`history` directly from the store for just this `groupId`
 * (rather than receiving them as page-level props derived from the full event
 * timeline), and is wrapped in `memo` below -- so it only re-renders when this
 * specific group's state or history actually changes, not on every SSE event the
 * page component receives for other panels (see the Phase 3 review).
 *
 * `override` (Phase 4, time travel): when the page is scrubbing to a past point, it
 * passes the state/history reconstructed as of that point (see time-travel.ts's
 * worldAtSeq/transitionHistoryAtSeq) instead of this component reading the live
 * store. Presence of `override` itself (not just its `.state`) is what switches this
 * component into "time travel" mode: at a past point before this group existed,
 * `override.state` is legitimately `undefined` and must NOT fall back to the live
 * state (which would show "now"'s state at a past timestamp) -- a plain `??` on an
 * optional `overrideState` prop couldn't distinguish those two cases. Both store
 * selectors below are still called unconditionally (rules of hooks) and are simply
 * ignored while `override` is present.
 */
function RebalanceDiagramImpl({
  groupId,
  override,
}: {
  groupId: string;
  override?: { state: GroupState | undefined; history: readonly TransitionHistoryEntry[] };
}) {
  const liveState = useKafkaStore((state) => state.world.groups[groupId]?.state);
  const liveHistory = useKafkaStore(
    (state) => state.transitionHistoryByGroup[groupId] ?? EMPTY_HISTORY,
  );
  const currentState = override !== undefined ? override.state : liveState;
  const history = override !== undefined ? override.history : liveHistory;
  const lastTransition = history[0];
  const isArcActive = lastTransition?.from === ARC_EDGE.from && lastTransition.to === ARC_EDGE.to;
  const arcFrom = nodeCenter(ARC_EDGE.from);
  const arcTo = nodeCenter(ARC_EDGE.to);
  const arcMidX = (arcFrom.x + arcTo.x) / 2;

  return (
    <div className="viz-root flex flex-col gap-2">
      <svg
        role="img"
        aria-label="consumer group rebalance state diagram"
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        width="100%"
        height={HEIGHT}
        style={{ background: "var(--surface-1)", display: "block" }}
      >
        <defs>
          <marker
            id="rebalance-arrow"
            markerWidth={8}
            markerHeight={8}
            refX={7}
            refY={4}
            orient="auto"
          >
            <path d="M0,0 L8,4 L0,8 Z" fill="var(--axis)" />
          </marker>
          <marker
            id="rebalance-arrow-active"
            markerWidth={8}
            markerHeight={8}
            refX={7}
            refY={4}
            orient="auto"
          >
            <path d="M0,0 L8,4 L0,8 Z" fill="var(--accent)" />
          </marker>
        </defs>

        {STRAIGHT_EDGES.map((edgeDef) => {
          const from = nodeCenter(edgeDef.from);
          const to = nodeCenter(edgeDef.to);
          const isActive =
            lastTransition?.from === edgeDef.from && lastTransition.to === edgeDef.to;
          return (
            <line
              key={`${edgeDef.from}-${edgeDef.to}`}
              x1={from.x + NODE_RADIUS}
              y1={from.y}
              x2={to.x - NODE_RADIUS}
              y2={to.y}
              stroke={isActive ? "var(--accent)" : "var(--axis)"}
              strokeWidth={isActive ? 3 : 1.5}
              markerEnd={isActive ? "url(#rebalance-arrow-active)" : "url(#rebalance-arrow)"}
            />
          );
        })}

        {/* the back edge (Stable -> PreparingRebalance), arced above the row so it
            never overlaps the straight forward edges or the Completing node between
            them */}
        <path
          d={`M ${arcFrom.x - 10} ${arcFrom.y - NODE_RADIUS + 6} Q ${arcMidX} ${ARC_APEX_Y} ${arcTo.x + 10} ${arcTo.y - NODE_RADIUS + 6}`}
          fill="none"
          stroke={isArcActive ? "var(--accent)" : "var(--axis)"}
          strokeWidth={isArcActive ? 3 : 1.5}
          markerEnd={isArcActive ? "url(#rebalance-arrow-active)" : "url(#rebalance-arrow)"}
        />

        {NODES.map((node) => {
          const isCurrent = node.state === currentState;
          return (
            <g key={node.state}>
              <circle
                cx={node.x}
                cy={node.y}
                r={NODE_RADIUS}
                fill={isCurrent ? "var(--accent)" : "var(--surface-1)"}
                stroke={isCurrent ? "var(--accent)" : "var(--axis)"}
                strokeWidth={2}
              />
              {node.label.split("\n").map((line, i, lines) => (
                <text
                  key={line}
                  x={node.x}
                  y={node.y + (i - (lines.length - 1) / 2) * 12}
                  dy="0.35em"
                  fontSize={11}
                  textAnchor="middle"
                  fill={isCurrent ? "var(--on-accent)" : "var(--text-secondary)"}
                >
                  {line}
                </text>
              ))}
            </g>
          );
        })}
      </svg>

      <ol className="flex max-h-24 flex-col gap-0.5 overflow-y-auto text-xs">
        {history.length === 0 && <li style={{ color: "var(--text-muted)" }}>No transitions yet</li>}
        {history.map((entry) => (
          <li
            key={`${entry.ts}-${entry.from}-${entry.to}`}
            style={{ color: "var(--text-secondary)" }}
          >
            <span style={{ color: "var(--text-muted)" }}>
              {new Date(entry.ts).toLocaleTimeString()}
            </span>{" "}
            {entry.from} → {entry.to}{" "}
            <span style={{ color: "var(--text-muted)" }}>({entry.edge})</span>
          </li>
        ))}
      </ol>
    </div>
  );
}

export const RebalanceDiagram = memo(RebalanceDiagramImpl);
