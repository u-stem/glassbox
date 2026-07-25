/** How the user wants the UI colored: follow the OS ("system") or force one scheme.
 * globals.css keys off the root element's data-theme attribute -- absent means the
 * OS preference wins, "light"/"dark" force that scheme. */
export type ThemePreference = "system" | "light" | "dark";

export const THEME_STORAGE_KEY = "glassbox-theme";

const CYCLE: Record<ThemePreference, ThemePreference> = {
  system: "light",
  light: "dark",
  dark: "system",
};

/** Parses a raw localStorage value into a ThemePreference, defaulting anything
 * unrecognized (including null) to "system" rather than failing. */
export function parseThemePreference(value: unknown): ThemePreference {
  if (value === "light" || value === "dark") {
    return value;
  }
  return "system";
}

export function nextThemePreference(current: ThemePreference): ThemePreference {
  return CYCLE[current];
}

/** Minimal shape applyThemePreference needs from the root element, so tests can
 * exercise it without a DOM (mirrors sse.ts's adapter pattern). */
export interface ThemeAttributeTarget {
  setAttribute: (name: string, value: string) => void;
  removeAttribute: (name: string) => void;
}

export function applyThemePreference(
  root: ThemeAttributeTarget,
  preference: ThemePreference,
): void {
  if (preference === "system") {
    root.removeAttribute("data-theme");
    return;
  }
  root.setAttribute("data-theme", preference);
}
