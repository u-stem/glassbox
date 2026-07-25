import type { ScenarioFormField } from "./schema-to-fields";

/** Every field's live value is kept as a string in form state (regardless of the
 * field's kind), matching what a controlled <input>/<select> naturally produces;
 * buildScenarioBody is where that's coerced back to a typed value per field.kind. */
export type ScenarioFormValues = Record<string, string>;

export function initialFormValues(fields: readonly ScenarioFormField[]): ScenarioFormValues {
  const values: ScenarioFormValues = {};
  for (const field of fields) {
    values[field.name] =
      field.kind === "number" ? (field.defaultValue?.toString() ?? "") : field.defaultValue;
  }
  return values;
}

export interface BuildScenarioBodyResult {
  body: Record<string, unknown>;
  errors: Record<string, string>;
}

/**
 * Coerces the form's raw string values back into a typed request body, omitting
 * fields left blank (so the gateway's own paramsSchema default applies) unless the
 * field is required. `kind: "json"` fields (the array/object fallback) are parsed as
 * JSON; a parse failure is reported per-field rather than thrown, so one bad field
 * doesn't block submitting the rest of a large form.
 */
export function buildScenarioBody(
  fields: readonly ScenarioFormField[],
  values: ScenarioFormValues,
): BuildScenarioBodyResult {
  const body: Record<string, unknown> = {};
  const errors: Record<string, string> = {};

  for (const field of fields) {
    const raw = values[field.name] ?? "";

    if (raw === "" && field.kind !== "json") {
      if (field.required) {
        errors[field.name] = "required";
      }
      continue;
    }

    switch (field.kind) {
      case "string":
      case "enum":
        body[field.name] = raw;
        break;
      case "number": {
        const parsed = Number(raw);
        if (Number.isNaN(parsed)) {
          errors[field.name] = "must be a number";
        } else {
          body[field.name] = parsed;
        }
        break;
      }
      case "json": {
        if (raw === "") {
          if (field.required) {
            errors[field.name] = "required";
          }
          break;
        }
        try {
          body[field.name] = JSON.parse(raw);
        } catch {
          errors[field.name] = "must be valid JSON";
        }
        break;
      }
    }
  }

  return { body, errors };
}
