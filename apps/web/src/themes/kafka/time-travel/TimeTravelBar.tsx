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
        className="rounded bg-gray-700 px-3 py-1.5 text-sm text-white"
      >
        Time travel
      </button>
    );
  }

  const isAtLiveEdge = value >= max;

  return (
    <div className="flex flex-wrap items-center gap-3 rounded border border-amber-400 bg-amber-50 p-2">
      <span className="rounded bg-amber-500 px-2 py-0.5 text-xs font-semibold text-white">
        {isAtLiveEdge ? "LIVE" : "PAST"}
      </span>
      <button
        type="button"
        onClick={() => setIsPlaying((p) => !p)}
        disabled={isAtLiveEdge && !isPlaying}
        className="rounded border border-amber-600 px-2 py-1 text-sm disabled:opacity-50"
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
        className="w-48"
        aria-label="time travel seek"
      />
      <span className="text-xs text-gray-600">
        seq {value} / {max}
      </span>
      <button
        type="button"
        onClick={() => {
          setIsPlaying(false);
          onExit();
        }}
        className="rounded bg-blue-600 px-2 py-1 text-sm text-white"
      >
        LIVE に戻る
      </button>
    </div>
  );
}
