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
  /** From the scenario's `.meta({ title })` on the gateway: a human-readable label
   * for the input, in place of the raw property name. */
  title?: string;
  /** From `.meta({ description })`: help text shown under the input. */
  description?: string;
}

export interface JsonSchemaObject {
  type?: "object";
  properties?: Record<string, JsonSchemaProperty>;
  required?: readonly string[];
}

/**
 * Copy shared by every field kind. `label` and `description` are optional and are
 * omitted rather than set to undefined when the schema carries no metadata: the form
 * falls back to the property name (the same `title ?? id` shape the gateway's
 * describe() uses), and callers constructing fields by hand stay valid.
 */
interface ScenarioFormFieldBase {
  name: string;
  label?: string;
  description?: string;
  required: boolean;
}

export type ScenarioFormField =
  | (ScenarioFormFieldBase & {
      kind: "enum";
      options: readonly string[];
      defaultValue: string;
    })
  | (ScenarioFormFieldBase & {
      kind: "number";
      defaultValue: number | undefined;
      min: number | undefined;
      max: number | undefined;
    })
  | (ScenarioFormFieldBase & { kind: "string"; defaultValue: string })
  // Fallback for array/object/boolean params (e.g. add-consumer's `topics` array):
  // edited as raw JSON text rather than growing dedicated widgets for every shape,
  // per the plan ("数値・enum・文字列の 3 種で十分").
  | (ScenarioFormFieldBase & { kind: "json"; defaultValue: string });

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
  // Spread-in rather than assigned, so a property with no metadata produces a field
  // object without the key at all (exactOptionalPropertyTypes rules out `undefined`).
  const base = {
    name,
    required,
    ...(prop.title === undefined ? {} : { label: prop.title }),
    ...(prop.description === undefined ? {} : { description: prop.description }),
  };

  if (prop.enum !== undefined) {
    const fallback = prop.enum[0] ?? "";
    return {
      ...base,
      kind: "enum",
      options: prop.enum,
      defaultValue: typeof prop.default === "string" ? prop.default : fallback,
    };
  }
  if (prop.type === "number" || prop.type === "integer") {
    const isInteger = prop.type === "integer";
    return {
      ...base,
      kind: "number",
      defaultValue: typeof prop.default === "number" ? prop.default : undefined,
      min: numericBound(prop.minimum, prop.exclusiveMinimum, isInteger, "min"),
      max: numericBound(prop.maximum, prop.exclusiveMaximum, isInteger, "max"),
    };
  }
  if (prop.type === "string") {
    return {
      ...base,
      kind: "string",
      defaultValue: typeof prop.default === "string" ? prop.default : "",
    };
  }
  return {
    ...base,
    kind: "json",
    defaultValue: JSON.stringify(prop.default ?? null),
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
