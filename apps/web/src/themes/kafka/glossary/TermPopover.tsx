"use client";

import { type RefObject, useLayoutEffect, useRef, useState } from "react";
import { relatedEntries } from "./glossary";
import { computePopoverPosition } from "./popover-position";
import { GLOSSARY } from "./terms";
import type { GlossaryTermId } from "./types";

const POPOVER_WIDTH = 288;

/**
 * The explanation card a glossary trigger opens.
 *
 * Every element here is a `<span>` (blocked out with `block`) rather than a `<div>`
 * or `<p>`: triggers sit inside headings and prose, so this markup has to be legal
 * as phrasing content wherever it lands.
 *
 * Positioned with `position: fixed` off the trigger's measured rect, which is why
 * nothing between here and the viewport may create a containing block -- no
 * `transform`, `filter`, `will-change` or `contain` on the drawer or the panels.
 * (`sticky` is fine; it doesn't create one.) The design tokens still apply because
 * custom properties inherit through the DOM, not the layout tree.
 */
export function TermPopover({
  id,
  popoverId,
  anchorRef,
  onOpenGlossary,
}: {
  id: GlossaryTermId;
  popoverId: string;
  anchorRef: RefObject<HTMLElement | null>;
  onOpenGlossary: (id: GlossaryTermId) => void;
}) {
  // Which term the card is currently showing. Starts at the trigger's own term and
  // changes as the reader follows related-term chips, so one popover can answer the
  // follow-up question without a second trip to the drawer.
  const [shownId, setShownId] = useState<GlossaryTermId>(id);
  const cardRef = useRef<HTMLSpanElement>(null);
  const [position, setPosition] = useState<{ left: number; top: number } | undefined>(undefined);

  const entry = GLOSSARY[shownId];
  const related = relatedEntries(shownId);

  // Layout effect, not a plain effect: the card's own height is only known after it
  // renders, and measuring it after paint would show one frame at the wrong place.
  // Until the measurement lands the card is rendered but not painted (see the style
  // below), so there is no flash either way.
  // Following a related-term chip swaps in a body of a different height, so the card
  // has to be re-measured even though shownId is never read inside the effect.
  // biome-ignore lint/correctness/useExhaustiveDependencies: re-measure on content change
  useLayoutEffect(() => {
    function place(): void {
      const anchor = anchorRef.current;
      const card = cardRef.current;
      if (anchor === null || card === null) {
        return;
      }
      const rect = anchor.getBoundingClientRect();
      setPosition(
        computePopoverPosition({
          anchor: rect,
          popover: {
            width: rect.width === 0 ? POPOVER_WIDTH : card.offsetWidth,
            height: card.offsetHeight,
          },
          viewport: { width: window.innerWidth, height: window.innerHeight },
        }),
      );
    }

    place();
    // Capture phase because scroll doesn't bubble: the trigger can be inside the
    // drawer's own scroll container as well as the page.
    window.addEventListener("scroll", place, { capture: true, passive: true });
    window.addEventListener("resize", place);
    return () => {
      window.removeEventListener("scroll", place, { capture: true });
      window.removeEventListener("resize", place);
    };
  }, [anchorRef, shownId]);

  return (
    <span
      ref={cardRef}
      id={popoverId}
      className="fixed z-50 block w-72 rounded border border-(--axis) bg-(--surface-1) p-3 shadow-lg"
      style={{
        left: position?.left ?? 0,
        top: position?.top ?? 0,
        // Rendered (so it can be measured) but not painted until it has a real
        // position; `hidden` or a conditional render would give nothing to measure.
        visibility: position === undefined ? "hidden" : "visible",
      }}
    >
      <span className="block text-sm font-semibold">
        {entry.ja}
        <span className="ml-1 font-normal text-(--text-secondary)">({entry.en})</span>
      </span>
      <span className="mt-1 block text-sm text-(--text-secondary)">{entry.body}</span>

      {related.length > 0 && (
        <span className="mt-2 flex flex-wrap gap-1">
          {related.map((relatedEntry) => (
            <button
              key={relatedEntry.id}
              type="button"
              onClick={() => setShownId(relatedEntry.id)}
              className="rounded border border-(--text-muted) px-1.5 py-0.5 text-xs hover:border-(--text-primary)"
            >
              {relatedEntry.ja}
            </button>
          ))}
        </span>
      )}

      <button
        type="button"
        onClick={() => onOpenGlossary(shownId)}
        className="mt-2 block w-full rounded border border-(--text-muted) px-2 py-1 text-xs hover:border-(--text-primary)"
      >
        用語集で見る
      </button>
    </span>
  );
}
