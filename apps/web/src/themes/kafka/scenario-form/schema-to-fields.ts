/**
 * Minimal JSON Schema subset understood by this form generator -- just enough to
 * cover every kafka scenario's paramsSchema as rendered by zod's `z.toJSONSchema`
 * (io: "input") on the gateway (see registry.ts's ScenarioDescription). Not a
 * general-purpose JSON Schema type.
 */
export interface JsonSchemaProperty {
  type?: "string" | "number" | "integer" | "boolean" | "array" | "object";
  enum?: readonly string[];
  default?: unknown;
  minimum?: number;
  maximum?: number;
  exclusiveMinimum?: number;
  exclusiveMaximum?: number;
}

export interface JsonSchemaObject {
  type?: "object";
  properties?: Record<string, JsonSchemaProperty>;
  required?: readonly string[];
}

export type ScenarioFormField =
  | {
      kind: "enum";
      name: string;
      options: readonly string[];
      defaultValue: string;
      required: boolean;
    }
  | {
      kind: "number";
      name: string;
      defaultValue: number | undefined;
      min: number | undefined;
      max: number | undefined;
      required: boolean;
    }
  | { kind: "string"; name: string; defaultValue: string; required: boolean }
  // Fallback for array/object/boolean params (e.g. add-consumer's `topics` array):
  // edited as raw JSON text rather than growing dedicated widgets for every shape,
  // per the plan ("数値・enum・文字列の 3 種で十分").
  | { kind: "json"; name: string; defaultValue: string; required: boolean };

/**
 * Resolves an HTML <input type="number"> `min`/`max` bound from a JSON Schema
 * numeric constraint. `inclusive` (JSON Schema's `minimum`/`maximum`) maps directly.
 * `exclusive` (`exclusiveMinimum`/`exclusiveMaximum`) is NOT itself a valid value,
 * so it can't be used as-is for an inclusive HTML `min`/`max` (caught in Phase 4
 * review): for an integer field the nearest valid inclusive bound is exclusive ± 1
 * (min: +1, max: -1). A non-integer field has no such well-defined "next" value
 * (there's no fixed step), so its exclusive bound is passed through unadjusted --
 * this only makes the HTML input's own native min/max hint very slightly loose at
 * the boundary; the server's own zod schema is the actual source of truth and
 * rejects an out-of-range submission with a 400 regardless.
 */
function numericBound(
  inclusive: number | undefined,
  exclusive: number | undefined,
  isInteger: boolean,
  direction: "min" | "max",
): number | undefined {
  if (inclusive !== undefined) {
    return inclusive;
  }
  if (exclusive === undefined) {
    return undefined;
  }
  if (!isInteger) {
    return exclusive;
  }
  return direction === "min" ? exclusive + 1 : exclusive - 1;
}

function toField(name: string, prop: JsonSchemaProperty, required: boolean): ScenarioFormField {
  if (prop.enum !== undefined) {
    const fallback = prop.enum[0] ?? "";
    return {
      kind: "enum",
      name,
      options: prop.enum,
      defaultValue: typeof prop.default === "string" ? prop.default : fallback,
      required,
    };
  }
  if (prop.type === "number" || prop.type === "integer") {
    const isInteger = prop.type === "integer";
    return {
      kind: "number",
      name,
      defaultValue: typeof prop.default === "number" ? prop.default : undefined,
      min: numericBound(prop.minimum, prop.exclusiveMinimum, isInteger, "min"),
      max: numericBound(prop.maximum, prop.exclusiveMaximum, isInteger, "max"),
      required,
    };
  }
  if (prop.type === "string") {
    return {
      kind: "string",
      name,
      defaultValue: typeof prop.default === "string" ? prop.default : "",
      required,
    };
  }
  return {
    kind: "json",
    name,
    defaultValue: JSON.stringify(prop.default ?? null),
    required,
  };
}

/**
 * Pure conversion from a scenario's JSON Schema (object type, flat properties -- no
 * nesting is used by any current scenario) into an ordered list of form fields. Field
 * order follows `Object.entries` (insertion order), which matches property
 * declaration order for `z.toJSONSchema`'s output.
 */
export function schemaToFields(schema: JsonSchemaObject): ScenarioFormField[] {
  const properties = schema.properties ?? {};
  const required = new Set(schema.required ?? []);
  return Object.entries(properties).map(([name, prop]) => toField(name, prop, required.has(name)));
}
