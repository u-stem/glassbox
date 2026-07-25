"use client";

import { useEffect, useState } from "react";
import { GATEWAY_URL } from "@/lib/gateway";
import { type EnvironmentStatus as EnvironmentStatusValue, startHealthPolling } from "./health";
import { BROKER_SETUP_COMMAND, GATEWAY_SETUP_COMMAND } from "./setup-commands";

const STATUS_DOT_COLOR: Record<EnvironmentStatusValue["kind"], string> = {
  checking: "var(--text-muted)",
  ready: "var(--status-good)",
  "kafka-down": "var(--status-warning)",
  "gateway-down": "var(--status-critical)",
};

/**
 * Thin presentational wrapper around startHealthPolling: owns just the current
 * EnvironmentStatus and re-renders when it changes, so the polling/hysteresis logic
 * itself (health.ts) stays a plain, independently-testable function. `initialStatus`
 * comes from the home page's server-side fetch, so the first paint already shows the
 * real status instead of flashing "checking" on every reload.
 */
export function EnvironmentStatus({ initialStatus }: { initialStatus: EnvironmentStatusValue }) {
  const [status, setStatus] = useState<EnvironmentStatusValue>(initialStatus);

  useEffect(() => {
    return startHealthPolling({
      url: `${GATEWAY_URL}/healthz`,
      onStatus: setStatus,
    });
  }, []);

  return (
    <div className="flex flex-col gap-1 text-xs" role="status">
      <div className="flex items-center gap-1.5">
        <span
          aria-hidden="true"
          className="inline-block h-2 w-2 rounded-full"
          style={{ backgroundColor: STATUS_DOT_COLOR[status.kind] }}
        />
        <StatusLabel status={status} />
      </div>
      {status.kind === "kafka-down" && (
        <p className="text-(--text-secondary)">
          Kafka ブローカーが起動していません。<code>{BROKER_SETUP_COMMAND}</code>{" "}
          を実行してください。
        </p>
      )}
      {status.kind === "gateway-down" && (
        <p className="text-(--text-secondary)">
          gateway に接続できません。<code>{BROKER_SETUP_COMMAND}</code> と{" "}
          <code>{GATEWAY_SETUP_COMMAND}</code> を実行してください。
        </p>
      )}
    </div>
  );
}

function StatusLabel({ status }: { status: EnvironmentStatusValue }) {
  switch (status.kind) {
    case "checking":
      return <span className="text-(--text-secondary)">確認中…</span>;
    case "ready":
      return <span className="text-(--text-secondary)">Gateway 接続 OK / Kafka 稼働中</span>;
    case "kafka-down":
      return <span className="text-(--text-primary)">Kafka 未起動</span>;
    case "gateway-down":
      return <span className="text-(--text-primary)">Gateway 未起動</span>;
  }
}
