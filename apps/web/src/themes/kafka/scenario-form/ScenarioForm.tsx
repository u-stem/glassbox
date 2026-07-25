"use client";

import { useState } from "react";
import { buildScenarioBody, initialFormValues, type ScenarioFormValues } from "./form-values";
import { type JsonSchemaObject, schemaToFields } from "./schema-to-fields";

export interface ScenarioDescription {
  id: string;
  title: string;
  description?: string;
  paramsSchema: JsonSchemaObject;
}

const INPUT_CLASS = "w-28 rounded border border-gray-300 px-2 py-1 text-sm";

/**
 * Auto-generates a parameter form from a scenario's JSON Schema (schemaToFields),
 * one input per property (number/enum/string, plus a JSON-text fallback for
 * array/object params -- see schema-to-fields.ts). Field state resets whenever the
 * selected scenario changes because ScenarioRunner mounts this with `key={scenario.id}`,
 * so a fresh instance (and fresh useState initializer) is created per scenario rather
 * than trying to reconcile one field set into another.
 */
export function ScenarioForm({
  scenario,
  onRun,
  isSubmitting,
}: {
  scenario: ScenarioDescription;
  onRun: (body: Record<string, unknown>) => Promise<void>;
  isSubmitting: boolean;
}) {
  const fields = schemaToFields(scenario.paramsSchema);
  const [values, setValues] = useState<ScenarioFormValues>(() => initialFormValues(fields));
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});

  async function handleSubmit(): Promise<void> {
    const { body, errors } = buildScenarioBody(fields, values);
    setFieldErrors(errors);
    if (Object.keys(errors).length > 0) {
      return;
    }
    await onRun(body);
  }

  return (
    <div className="flex flex-wrap items-end gap-3">
      {fields.map((field) => {
        const inputId = `${scenario.id}-${field.name}`;
        return (
          <label key={field.name} htmlFor={inputId} className="flex flex-col gap-1 text-sm">
            {field.name}
            {field.kind === "enum" ? (
              <select
                id={inputId}
                value={values[field.name] ?? ""}
                onChange={(e) => setValues((prev) => ({ ...prev, [field.name]: e.target.value }))}
                className={INPUT_CLASS}
              >
                {field.options.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
            ) : (
              <input
                id={inputId}
                type={field.kind === "number" ? "number" : "text"}
                value={values[field.name] ?? ""}
                min={field.kind === "number" ? field.min : undefined}
                max={field.kind === "number" ? field.max : undefined}
                placeholder={field.required ? "required" : undefined}
                onChange={(e) => setValues((prev) => ({ ...prev, [field.name]: e.target.value }))}
                className={INPUT_CLASS}
              />
            )}
            {fieldErrors[field.name] && (
              <span className="text-xs text-red-600">{fieldErrors[field.name]}</span>
            )}
          </label>
        );
      })}
      <button
        type="button"
        onClick={() => {
          void handleSubmit();
        }}
        disabled={isSubmitting}
        className="rounded bg-blue-600 px-3 py-1.5 text-sm text-white disabled:opacity-50"
      >
        Run {scenario.title}
      </button>
    </div>
  );
}
