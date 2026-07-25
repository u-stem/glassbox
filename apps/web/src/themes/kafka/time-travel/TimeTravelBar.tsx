"use client";

import { useEffect, useRef, useState } from "react";

/** How often (ms) auto-play advances the scrub position by one event. Throttled via
 * requestAnimationFrame (see the play effect below) rather than setInterval, per the
 * plan ("スクラブ中は requestAnimationFrame でスロットル"). */
const AUTOPLAY_STEP_INTERVAL_MS = 150;

/**
 * Scrub bar for time travel (Phase 4). Purely presentational/control-owning: the
 * page computes the actual past `world` from `value` (see time-travel.ts's
 * worldAtSeq) -- this component only owns the seek position and the play/pause
 * animation loop.
 */
export function TimeTravelBar({
  isActive,
  min,
  max,
  value,
  onEnter,
  onExit,
  onSeek,
}: {
  isActive: boolean;
  min: number;
  max: number;
  value: number;
  onEnter: () => void;
  onExit: () => void;
  onSeek: (seq: number) => void;
}) {
  const [isPlaying, setIsPlaying] = useState(false);
  const valueRef = useRef(value);
  valueRef.current = value;
  const onSeekRef = useRef(onSeek);
  onSeekRef.current = onSeek;

  useEffect(() => {
    if (!isActive || !isPlaying) {
      return;
    }
    let rafId: number;
    let lastStepAt = 0;

    function tick(now: number): void {
      if (now - lastStepAt >= AUTOPLAY_STEP_INTERVAL_MS) {
        lastStepAt = now;
        const next = valueRef.current + 1;
        if (next >= max) {
          onSeekRef.current(max);
          setIsPlaying(false);
          return;
        }
        onSeekRef.current(next);
      }
      rafId = requestAnimationFrame(tick);
    }
    rafId = requestAnimationFrame(tick);

    return () => cancelAnimationFrame(rafId);
  }, [isActive, isPlaying, max]);

  if (!isActive) {
    return (
      <button
        type="button"
        onClick={onEnter}
        className="w-fit rounded border border-(--text-muted) px-3 py-1.5 text-sm hover:border-(--text-primary)"
      >
        Time travel
      </button>
    );
  }

  const isAtLiveEdge = value >= max;

  return (
    // The band carries the warning hue as a translucent wash; the mode chip cannot,
    // because --status-warning is 1.74:1 against the light plane and would read as plain
    // bold text. Being in the past must never be missable, so the chip is ink-outlined.
    <div className="flex flex-wrap items-center gap-3 rounded border border-(--status-warning)/60 bg-(--status-warning)/12 p-2">
      <span className="rounded border border-(--text-primary) px-2 py-0.5 text-xs font-semibold">
        {isAtLiveEdge ? "LIVE" : "PAST"}
      </span>
      <button
        type="button"
        onClick={() => setIsPlaying((p) => !p)}
        disabled={isAtLiveEdge && !isPlaying}
        className="rounded border border-(--text-muted) px-2 py-1 text-sm enabled:hover:border-(--text-primary) disabled:opacity-50"
      >
        {isPlaying ? "Pause" : "Play"}
      </button>
      <input
        type="range"
        min={min}
        max={max}
        value={value}
        onChange={(e) => {
          setIsPlaying(false);
          onSeek(Number(e.target.value));
        }}
        className="w-48 accent-(--accent)"
        aria-label="time travel seek"
      />
      <span className="text-xs text-(--text-secondary)">
        seq {value} / {max}
      </span>
      <button
        type="button"
        onClick={() => {
          setIsPlaying(false);
          onExit();
        }}
        className="rounded bg-(--accent) px-2 py-1 text-sm text-(--on-accent) hover:opacity-90"
      >
        LIVE に戻る
      </button>
    </div>
  );
}
