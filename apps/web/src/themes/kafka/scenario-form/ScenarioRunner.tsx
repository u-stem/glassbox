"use client";

import { useEffect, useRef, useState } from "react";
import { type ScenarioDescription, ScenarioForm } from "./ScenarioForm";

/**
 * Shortcut buttons for the two flows used constantly while exploring this theme
 * (produce some messages, add a consumer) -- run with the scenario's own server-side
 * defaults, no form needed. Keeps the pre-Phase-4 one-click UX for the common case;
 * the auto-generated form below covers every scenario (including ones needing a
 * consumerId with no sensible default, e.g. remove-consumer/slow-consumer).
 */
const QUICK_ACTIONS: { scenarioId: string; label: string }[] = [
  { scenarioId: "produce-burst", label: "Produce 10" },
  { scenarioId: "add-consumer", label: "Add consumer" },
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
    <div className="flex flex-wrap items-end gap-4">
      <div className="flex gap-2">
        {QUICK_ACTIONS.map((action) => (
          <button
            key={action.scenarioId}
            type="button"
            onClick={() => {
              void runScenario(action.scenarioId, {});
            }}
            disabled={isSubmitting}
            className="rounded bg-blue-600 px-3 py-1.5 text-sm text-white disabled:opacity-50"
          >
            {action.label}
          </button>
        ))}
      </div>

      <label className="flex flex-col gap-1 text-sm">
        scenario
        <select
          value={selectedId ?? ""}
          onChange={(e) => setSelectedId(e.target.value)}
          className="rounded border border-gray-300 px-2 py-1 text-sm"
        >
          {(scenarios ?? []).map((scenario) => (
            <option key={scenario.id} value={scenario.id}>
              {scenario.title}
            </option>
          ))}
        </select>
      </label>

      {selectedScenario && (
        <ScenarioForm
          key={selectedScenario.id}
          scenario={selectedScenario}
          isSubmitting={isSubmitting}
          onRun={(body) => runScenario(selectedScenario.id, body)}
        />
      )}
    </div>
  );
}
