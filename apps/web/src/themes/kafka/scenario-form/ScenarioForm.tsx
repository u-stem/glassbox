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

const INPUT_CLASS =
  "w-28 rounded border border-(--text-muted) bg-(--surface-1) px-2 py-1 text-sm text-(--text-primary)";

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
    // Top-aligned: each field is label / input / optional help text, and help text of
    // differing heights would stagger the inputs if the bottoms were aligned instead.
    <div className="flex flex-wrap items-start gap-3">
      {fields.map((field) => {
        const inputId = `${scenario.id}-${field.name}`;
        const descriptionId = `${inputId}-description`;
        return (
          // The label is a sibling of the input rather than wrapping it, and the help
          // text is linked with aria-describedby: wrapped, the description would be
          // read out as part of the field's own name.
          <div key={field.name} className="flex flex-col gap-1 text-sm">
            <label htmlFor={inputId}>{field.label ?? field.name}</label>
            {field.kind === "enum" ? (
              <select
                id={inputId}
                value={values[field.name] ?? ""}
                {...(field.description === undefined ? {} : { "aria-describedby": descriptionId })}
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
                placeholder={field.required ? "必須" : undefined}
                {...(field.description === undefined ? {} : { "aria-describedby": descriptionId })}
                onChange={(e) => setValues((prev) => ({ ...prev, [field.name]: e.target.value }))}
                className={INPUT_CLASS}
              />
            )}
            {field.description !== undefined && (
              <span id={descriptionId} className="max-w-48 text-xs text-(--text-secondary)">
                {field.description}
              </span>
            )}
            {fieldErrors[field.name] && (
              // The dot lives inside this span so it sits beside the message rather
              // than stacking above it in this flex column.
              <span className="flex items-center gap-1.5 text-xs">
                <span
                  aria-hidden="true"
                  className="inline-block size-2 shrink-0 rounded-full"
                  style={{ backgroundColor: "var(--status-critical)" }}
                />
                {fieldErrors[field.name]}
              </span>
            )}
          </div>
        );
      })}
      {/* The spacer reserves exactly one label line, so the button lines up with the
       * inputs beside it rather than with their labels (the row is top-aligned
       * because the fields' help text varies in height). */}
      <div className="flex flex-col gap-1 text-sm">
        <span aria-hidden="true" className="invisible">
          実行
        </span>
        {/* The visible label stays short -- the Japanese scenario titles are a sentence
         * long -- while the accessible name still says which scenario it runs. */}
        <button
          type="button"
          onClick={() => {
            void handleSubmit();
          }}
          disabled={isSubmitting}
          aria-label={`${scenario.title} を実行`}
          className="rounded bg-(--accent) px-3 py-1.5 text-sm text-(--on-accent) hover:opacity-90 disabled:opacity-50"
        >
          実行
        </button>
      </div>
    </div>
  );
}
