"use client";

import { useEffect, useRef, useState } from "react";
import { GlossaryMarker } from "../glossary/GlossaryTerm";
import { type ScenarioDescription, ScenarioForm } from "./ScenarioForm";

/**
 * Shortcut buttons for the two flows used constantly while exploring this theme
 * (produce some messages, add a consumer) -- run with the scenario's own server-side
 * defaults, no form needed. Keeps the pre-Phase-4 one-click UX for the common case;
 * the auto-generated form below covers every scenario (including ones needing a
 * consumerId with no sensible default, e.g. remove-consumer/slow-consumer).
 */
const QUICK_ACTIONS: { scenarioId: string; label: string }[] = [
  { scenarioId: "produce-burst", label: "メッセージを 10 件送る" },
  { scenarioId: "add-consumer", label: "consumer を 1 台追加" },
];

export function ScenarioRunner({
  gatewayUrl,
  onError,
}: {
  gatewayUrl: string;
  onError: (message: string | undefined) => void;
}) {
  const [scenarios, setScenarios] = useState<ScenarioDescription[] | undefined>(undefined);
  const [selectedId, setSelectedId] = useState<string | undefined>(undefined);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Same pattern as the page-level addEvent/reset refs (see page.tsx): onError is a
  // fresh closure every render, but this effect should only re-run the fetch when
  // gatewayUrl itself changes, always calling whichever onError is current via the ref.
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;

  useEffect(() => {
    let cancelled = false;
    fetch(`${gatewayUrl}/api/themes/kafka/scenarios`)
      .then((res) => res.json())
      .then((data: { scenarios: ScenarioDescription[] }) => {
        if (cancelled) {
          return;
        }
        setScenarios(data.scenarios);
        setSelectedId((current) => current ?? data.scenarios[0]?.id);
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          onErrorRef.current(error instanceof Error ? error.message : String(error));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [gatewayUrl]);

  async function runScenario(scenarioId: string, body: Record<string, unknown>): Promise<void> {
    setIsSubmitting(true);
    onError(undefined);
    try {
      const response = await fetch(`${gatewayUrl}/api/themes/kafka/scenarios/${scenarioId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!response.ok) {
        throw new Error(`gateway responded with ${response.status}`);
      }
    } catch (error) {
      onError(error instanceof Error ? error.message : String(error));
    } finally {
      setIsSubmitting(false);
    }
  }

  const selectedScenario = scenarios?.find((s) => s.id === selectedId);

  return (
    // Two groups rather than one long wrapping row: the shortcuts are the two things
    // you press constantly, while the picker below is the "anything else" path. Run
    // together they read as one undifferentiated wall of controls.
    <div className="flex w-full flex-col gap-3">
      <div className="flex flex-wrap items-center gap-2">
        <h2 className="text-sm font-medium text-(--text-secondary)">よく使う操作</h2>
        {QUICK_ACTIONS.map((action) => (
          <button
            key={action.scenarioId}
            type="button"
            onClick={() => {
              void runScenario(action.scenarioId, {});
            }}
            disabled={isSubmitting}
            className="rounded bg-(--accent) px-3 py-1.5 text-sm text-(--on-accent) hover:opacity-90 disabled:opacity-50"
          >
            {action.label}
          </button>
        ))}
      </div>

      {/* items-start, not items-end: fields carry help text of differing heights, and
       * aligning bottoms would stagger the inputs themselves down the row. */}
      <div className="flex flex-wrap items-start gap-3 border-t border-(--border) pt-3">
        {/* Same one-label-line offset as the fields, so the group name sits level with
         * the controls it introduces. */}
        <span className="flex items-center gap-1 pt-7">
          <h2 className="text-sm font-medium text-(--text-secondary)">シナリオを実行</h2>
          <GlossaryMarker id="scenario" />
        </span>

        <div className="flex flex-col gap-1 text-sm">
          <label htmlFor="scenario-picker">シナリオ</label>
          <select
            id="scenario-picker"
            value={selectedId ?? ""}
            onChange={(e) => setSelectedId(e.target.value)}
            className="rounded border border-(--text-muted) bg-(--surface-1) px-2 py-1 text-sm text-(--text-primary)"
          >
            {(scenarios ?? []).map((scenario) => (
              <option key={scenario.id} value={scenario.id}>
                {scenario.title}
              </option>
            ))}
          </select>
        </div>

        {selectedScenario && (
          <ScenarioForm
            key={selectedScenario.id}
            scenario={selectedScenario}
            isSubmitting={isSubmitting}
            onRun={(body) => runScenario(selectedScenario.id, body)}
          />
        )}

        {selectedScenario?.description !== undefined && (
          <p className="w-full text-xs text-(--text-secondary)">{selectedScenario.description}</p>
        )}
      </div>
    </div>
  );
}
