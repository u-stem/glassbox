/** Verbatim copies of the commands from README.md's "セットアップ" section, shown
 * inline by EnvironmentStatus when the gateway/broker aren't reachable. Kept as
 * exported constants (rather than literals inside JSX) so setup-commands.test.ts can
 * assert they still match the README, catching drift if either side changes. */
export const BROKER_SETUP_COMMAND = "docker compose -f docker/compose.yaml up -d";
export const GATEWAY_SETUP_COMMAND = "bun run --cwd apps/gateway dev";
