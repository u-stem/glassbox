"use client";

import { useEffect, useState } from "react";
import {
  applyThemePreference,
  nextThemePreference,
  parseThemePreference,
  THEME_STORAGE_KEY,
  type ThemePreference,
} from "./color-scheme";

const LABELS: Record<ThemePreference, string> = {
  system: "システム",
  light: "ライト",
  dark: "ダーク",
};

const ICON_PROPS = {
  width: 20,
  height: 20,
  viewBox: "0 0 24 24",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 2,
  strokeLinecap: "round",
  strokeLinejoin: "round",
} as const;

function SystemIcon() {
  return (
    <svg {...ICON_PROPS} aria-hidden="true">
      <rect x="2" y="4" width="20" height="13" rx="2" />
      <path d="M8 21h8M12 17v4" />
    </svg>
  );
}

function SunIcon() {
  return (
    <svg {...ICON_PROPS} aria-hidden="true">
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
    </svg>
  );
}

function MoonIcon() {
  return (
    <svg {...ICON_PROPS} aria-hidden="true">
      <path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8Z" />
    </svg>
  );
}

/** Cycles system -> light -> dark, persisting to localStorage. An inline script in
 * the root layout applies the stored preference before first paint, and the visible
 * icon is driven purely by CSS off the root's data-theme attribute -- all three icons
 * are rendered and toggled by ancestor selectors -- so a reload never flashes the
 * default icon while React state catches up. State only feeds the hover title. */
export function ThemeToggle() {
  const [preference, setPreference] = useState<ThemePreference>("system");

  useEffect(() => {
    setPreference(parseThemePreference(window.localStorage.getItem(THEME_STORAGE_KEY)));
  }, []);

  function handleClick() {
    // Read the source of truth at click time rather than trusting state, so a click
    // that lands before the mount effect syncs still cycles from the real value.
    const current = parseThemePreference(window.localStorage.getItem(THEME_STORAGE_KEY));
    const next = nextThemePreference(current);
    setPreference(next);
    applyThemePreference(document.documentElement, next);
    if (next === "system") {
      window.localStorage.removeItem(THEME_STORAGE_KEY);
    } else {
      window.localStorage.setItem(THEME_STORAGE_KEY, next);
    }
  }

  return (
    <button
      type="button"
      onClick={handleClick}
      aria-label="配色テーマを切り替える"
      title={`配色テーマ: ${LABELS[preference]}`}
      className="flex size-9 items-center justify-center rounded border border-(--border) bg-(--surface-1) text-(--text-secondary) hover:text-(--text-primary)"
    >
      <span className="hidden [:root:not([data-theme])_&]:inline-flex">
        <SystemIcon />
      </span>
      <span className="hidden [[data-theme=light]_&]:inline-flex">
        <SunIcon />
      </span>
      <span className="hidden [[data-theme=dark]_&]:inline-flex">
        <MoonIcon />
      </span>
    </button>
  );
}
