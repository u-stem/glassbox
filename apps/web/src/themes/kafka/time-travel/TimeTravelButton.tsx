"use client";

/**
 * Entry point into time travel, living in the theme page's header row alongside
 * the lesson/glossary buttons rather than inside TimeTravelBar. Splitting it out
 * lets the bar itself be rendered only while scrubbing (see the page), so the
 * dashboard doesn't carry an always-present control that is inert most of the time.
 */
export function TimeTravelButton({
  onEnter,
  disabled,
}: {
  onEnter: () => void;
  disabled: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onEnter}
      disabled={disabled}
      className="rounded border border-(--text-muted) px-3 py-1.5 text-sm enabled:hover:border-(--text-primary) disabled:opacity-50"
    >
      タイムトラベル
    </button>
  );
}
