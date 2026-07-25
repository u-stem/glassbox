import { z } from "zod";

export const healthzResponseSchema = z.object({
  ok: z.literal(true),
  kafka: z.enum(["connected", "unreachable", "unknown"]),
});

export type HealthzResponse = z.infer<typeof healthzResponseSchema>;
