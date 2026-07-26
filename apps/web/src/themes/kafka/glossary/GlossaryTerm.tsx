"use client";

import { type ReactNode, useEffect, useId, useRef, useState } from "react";
import { useGlossary } from "./GlossaryContext";
import { TermPopover } from "./TermPopover";
import { GLOSSARY } from "./terms";
import type { GlossaryTermId } from "./types";

/**
 * Shared behaviour of both triggers: a disclosure button that reveals the term card
 * next to it.
 *
 * The ARIA pattern is disclosure (`aria-expanded` + `aria-controls`), not `dialog`
 * or `tooltip`. `dialog` would over-promise -- nothing here is modal, the dashboard
 * stays fully usable behind it -- and `tooltip` is wrong because the card contains
 * its own buttons, which a tooltip may not.
 *
 * Do not place either trigger inside a `<button>` (nested buttons are invalid), a
 * wrapping `<label>` (the click would be redirected to its input), an SVG, or a
 * ReactFlow node -- xyflow transforms its viewport, which would capture the card's
 * `position: fixed`.
 */
function TermTrigger({
  id,
  className,
  ariaLabel,
  children,
}: {
  id: GlossaryTermId;
  className: string;
  ariaLabel?: string;
  children: ReactNode;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const popoverId = useId();
  const wrapperRef = useRef<HTMLSpanElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const { openGlossary } = useGlossary();

  // Both listeners live on the document rather than on the wrapper: the card is
  // rendered inside the wrapper but positioned against the viewport, and Escape has
  // to work from anywhere inside it (including its related-term chips), not only
  // while the trigger itself holds focus.
  useEffect(() => {
    if (!isOpen) {
      return;
    }
    function onPointerDown(event: PointerEvent): void {
      const target = event.target;
      if (target instanceof Node && wrapperRef.current?.contains(target) === true) {
        return;
      }
      setIsOpen(false);
    }
    function onKeyDown(event: KeyboardEvent): void {
      if (event.key === "Escape") {
        setIsOpen(false);
        triggerRef.current?.focus();
      }
    }
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [isOpen]);

  return (
    // Wrapper exists for the outside-click test, not for positioning: the card is
    // fixed to the viewport, not to this element.
    <span ref={wrapperRef} className="inline-block">
      <button
        ref={triggerRef}
        type="button"
        aria-expanded={isOpen}
        aria-controls={popoverId}
        {...(ariaLabel === undefined ? {} : { "aria-label": ariaLabel })}
        onClick={() => setIsOpen((open) => !open)}
        className={className}
      >
        {children}
      </button>
      {isOpen && (
        <TermPopover
          id={id}
          popoverId={popoverId}
          anchorRef={triggerRef}
          onOpenGlossary={(termId) => {
            setIsOpen(false);
            openGlossary(termId);
          }}
        />
      )}
    </span>
  );
}

/**
 * Makes a word in running text its own explanation trigger, marked with a dotted
 * underline. Use where the term appears naturally in a sentence or a label.
 */
export function GlossaryTerm({ id, children }: { id: GlossaryTermId; children?: ReactNode }) {
  return (
    <TermTrigger
      id={id}
      className="underline decoration-(--text-muted) decoration-dotted underline-offset-2 hover:decoration-(--text-primary)"
    >
      {children ?? GLOSSARY[id].ja}
    </TermTrigger>
  );
}

/**
 * A small "?" next to a heading. Kept outside the `<h2>` itself so the heading's
 * accessible name stays the panel name -- `aria-labelledby` on the panel regions and
 * `getByRole("heading")` in the E2E specs both read that name.
 */
export function GlossaryMarker({ id }: { id: GlossaryTermId }) {
  return (
    <TermTrigger
      id={id}
      ariaLabel={`${GLOSSARY[id].ja} とは`}
      className="inline-flex size-4 items-center justify-center rounded-full border border-(--text-muted) text-[10px] leading-none text-(--text-secondary) hover:border-(--text-primary)"
    >
      ?
    </TermTrigger>
  );
}
