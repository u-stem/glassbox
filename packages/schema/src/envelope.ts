import { z } from "zod";

const eventSourceSchema = z.object({
  kind: z.enum(["client", "admin", "scenario"]),
  actorId: z.string().optional(),
});

export const glassboxEventSchema = z.object({
  id: z.string().ulid(),
  seq: z.number().int().nonnegative(),
  ts: z.number().int().nonnegative(),
  theme: z.string(),
  source: eventSourceSchema,
  type: z.string(),
  payload: z.unknown(),
});

export type GlassboxEvent = z.infer<typeof glassboxEventSchema>;
export type GlassboxEventSource = z.infer<typeof eventSourceSchema>;
