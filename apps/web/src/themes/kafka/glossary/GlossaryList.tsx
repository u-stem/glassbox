"use client";

import { useEffect, useRef } from "react";
import { groupTerms } from "./glossary";

function termElementId(id: string): string {
  return `glossary-term-${id}`;
}

/**
 * The glossary pane of the side drawer: every term, grouped, in reading order.
 *
 * Terms are `<li>` rather than headings on purpose. The drawer already sits in a
 * page whose panels and lesson steps own the heading outline, and adding 28 more
 * headings would bury them -- `getByRole("heading", { name: "パーティションとは" })`
 * in the E2E specs is looking for a lesson step, not a term.
 *
 * No search box: 28 entries in four labelled groups is a scroll, not a search
 * problem.
 */
export function GlossaryList({ focusTermId }: { focusTermId?: string }) {
  const listRef = useRef<HTMLDivElement>(null);

  // Arriving from a popover's "用語集で見る" should land on that term, not at the
  // top of a list the reader then has to scan.
  useEffect(() => {
    if (focusTermId === undefined) {
      return;
    }
    const target = listRef.current?.querySelector<HTMLElement>(`#${termElementId(focusTermId)}`);
    target?.scrollIntoView({ block: "start" });
    target?.focus();
  }, [focusTermId]);

  return (
    <div ref={listRef} className="flex flex-col gap-4">
      {groupTerms().map((group) => (
        <div key={group.category} className="flex flex-col gap-2">
          <h3 className="text-sm font-semibold">{group.label}</h3>
          <ul className="flex flex-col gap-2">
            {group.entries.map((entry) => (
              <li
                key={entry.id}
                id={termElementId(entry.id)}
                tabIndex={-1}
                className="flex flex-col gap-0.5"
              >
                <span className="text-sm font-medium">
                  {entry.ja}
                  <span className="ml-1 font-normal text-(--text-secondary)">({entry.en})</span>
                </span>
                <span className="text-xs text-(--text-secondary)">{entry.body}</span>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  );
}
