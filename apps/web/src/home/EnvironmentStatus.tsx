"use client";

import type { BrokerAction } from "@glassbox/schema";
import { useEffect, useRef, useState } from "react";
import { GATEWAY_URL } from "@/lib/gateway";
import {
  type BrokerPolling,
  type BrokerViewStatus,
  requestBrokerAction,
  startBrokerPolling,
  toBrokerButtonModel,
  toBrokerStatusLabel,
} from "./broker";
import { type EnvironmentStatus as EnvironmentStatusValue, startHealthPolling } from "./health";
import { BROKER_SETUP_COMMAND, GATEWAY_SETUP_COMMAND } from "./setup-commands";

const BROKER_URL = `${GATEWAY_URL}/api/themes/kafka/broker`;

/** Generous because the gateway's own `up` timeout is 180s (a first run may pull the
 * image). The UI does not depend on this returning: polling converges either way. */
const ACTION_TIMEOUT_MS = 200_000;

const STATUS_DOT_COLOR: Record<EnvironmentStatusValue["kind"], string> = {
  checking: "var(--text-muted)",
  ready: "var(--status-good)",
  "kafka-down": "var(--status-warning)",
  "gateway-down": "var(--status-critical)",
};

const BROKER_DOT_COLOR: Record<BrokerViewStatus["kind"], string> = {
  checking: "var(--text-muted)",
  running: "var(--status-good)",
  starting: "var(--status-warning)",
  stopped: "var(--text-muted)",
  absent: "var(--text-muted)",
  unavailable: "var(--status-critical)",
  unreachable: "var(--status-critical)",
};

/**
 * Thin presentational wrapper around the two pollers this card needs: the gateway's
 * `/healthz` (can the gateway talk to the broker) and the gateway's broker endpoint
 * (what is the container doing). Both the polling/hysteresis logic and every mapping
 * from state to wording live in plain functions (health.ts, broker.ts) so they stay
 * independently testable.
 *
 * The two are reported as separate lines rather than merged into one: they legitimately
 * disagree -- the container can be up while the broker is not answering yet, and after
 * a stop the gateway takes a few seconds to notice -- and per ADR-0003/0005 each line
 * names its own authority instead of pretending to a single truth.
 *
 * `initialStatus` comes from the home page's server-side fetch, so the first paint
 * already shows the real gateway status. The broker state is deliberately not fetched
 * during SSR: `docker compose ps` takes a few hundred ms and would delay the whole page.
 */
export function EnvironmentStatus({ initialStatus }: { initialStatus: EnvironmentStatusValue }) {
  const [status, setStatus] = useState<EnvironmentStatusValue>(initialStatus);
  const [brokerStatus, setBrokerStatus] = useState<BrokerViewStatus>({ kind: "checking" });
  const [pending, setPending] = useState<BrokerAction | undefined>(undefined);
  const pollingRef = useRef<BrokerPolling | undefined>(undefined);

  useEffect(() => {
    return startHealthPolling({
      url: `${GATEWAY_URL}/healthz`,
      onStatus: setStatus,
    });
  }, []);

  useEffect(() => {
    const polling = startBrokerPolling({ url: BROKER_URL, onStatus: setBrokerStatus });
    pollingRef.current = polling;
    return () => {
      polling.stop();
      pollingRef.current = undefined;
    };
  }, []);

  const button = toBrokerButtonModel(brokerStatus, pending);

  async function runBrokerAction(action: BrokerAction): Promise<void> {
    setPending(action);
    try {
      setBrokerStatus(
        await requestBrokerAction(BROKER_URL, action, {
          timeoutMs: ACTION_TIMEOUT_MS,
        }),
      );
    } finally {
      setPending(undefined);
      // Converge on the real state even if the request itself failed or timed out.
      pollingRef.current?.refreshNow();
    }
  }

  return (
    <div className="flex flex-col gap-2 text-xs">
      <div className="flex flex-col gap-1" role="status">
        <div className="flex items-center gap-1.5">
          <Dot color={STATUS_DOT_COLOR[status.kind]} />
          <StatusLabel status={status} />
        </div>
        <div className="flex items-center gap-1.5">
          <Dot color={BROKER_DOT_COLOR[brokerStatus.kind]} />
          <span className="text-(--text-secondary)">
            ブローカー(コンテナ): {toBrokerStatusLabel(brokerStatus)}
          </span>
        </div>
      </div>

      <button
        type="button"
        onClick={() => {
          void runBrokerAction(button.action);
        }}
        disabled={button.disabled}
        aria-busy={pending !== undefined}
        className="w-fit rounded border border-(--text-muted) px-2 py-1 text-xs enabled:hover:border-(--text-primary) disabled:opacity-50"
      >
        {button.label}
      </button>

      {(brokerStatus.kind === "unavailable" || brokerStatus.kind === "unreachable") &&
        status.kind !== "gateway-down" && (
          <p className="text-(--text-secondary)">
            Docker を実行できないため、ブローカーを操作できません。
            <code>{BROKER_SETUP_COMMAND}</code> が手元で通るか確認してください。
          </p>
        )}
      {status.kind === "gateway-down" && (
        <p className="text-(--text-secondary)">
          gateway に接続できません。<code>{GATEWAY_SETUP_COMMAND}</code> を実行してください。
        </p>
      )}
    </div>
  );
}

function Dot({ color }: { color: string }) {
  return (
    <span
      aria-hidden="true"
      className="inline-block h-2 w-2 shrink-0 rounded-full"
      style={{ backgroundColor: color }}
    />
  );
}

function StatusLabel({ status }: { status: EnvironmentStatusValue }) {
  switch (status.kind) {
    case "checking":
      return <span className="text-(--text-secondary)">確認中…</span>;
    case "ready":
      return <span className="text-(--text-secondary)">Gateway 接続 OK / Kafka 稼働中</span>;
    case "kafka-down":
      // Says what the gateway observes, not what the container is doing -- the broker
      // line below owns that, and after a stop the two disagree for a few seconds.
      return <span className="text-(--text-primary)">Gateway 接続 OK / ブローカーに未接続</span>;
    case "gateway-down":
      return <span className="text-(--text-primary)">Gateway 未起動</span>;
  }
}
