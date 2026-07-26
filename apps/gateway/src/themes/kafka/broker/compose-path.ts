import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

/**
 * The compose file shipped with this repo, resolved from this module's own location.
 * `process.cwd()` is deliberately not used: it is `apps/gateway` under `tsx watch`
 * but the repo root under `bun test`, so a cwd-relative path would resolve to two
 * different places. The gateway is executed directly by tsx (never bundled), so
 * `import.meta.url` always points at this source file.
 */
export const DEFAULT_COMPOSE_PATH = fileURLToPath(
  new URL("../../../../../../docker/compose.yaml", import.meta.url),
);

export type ComposePathResult = { found: true; path: string } | { found: false };

export interface ResolveComposePathOptions {
  /** `GLASSBOX_COMPOSE_FILE` if set; the env var name itself is owned by env.ts, so
   * this module stays a plain path resolver. */
  configuredPath?: string | undefined;
  /** Injected so this stays testable without touching the real filesystem. */
  exists?: (candidate: string) => boolean;
}

/**
 * Decides which compose file the broker routes operate on: the configured override
 * when there is one, otherwise the compose file shipped with this repo. A
 * configured-but-missing file is reported rather than handed to docker, so the UI can
 * say the compose file is missing instead of surfacing an opaque docker error.
 */
export function resolveComposePath(options: ResolveComposePathOptions = {}): ComposePathResult {
  const exists = options.exists ?? existsSync;
  const configured = options.configuredPath;
  const path = configured === undefined || configured === "" ? DEFAULT_COMPOSE_PATH : configured;
  return exists(path) ? { found: true, path } : { found: false };
}
