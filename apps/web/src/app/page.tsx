import Link from "next/link";
import { EnvironmentStatus } from "@/home/EnvironmentStatus";
import {
  type EnvironmentStatus as EnvironmentStatusValue,
  fetchHealthOutcome,
  toEnvironmentStatus,
} from "@/home/health";
import { GATEWAY_URL } from "@/lib/gateway";
import { lessons } from "@/themes/kafka/lessons/lessons";
import { lessonHref } from "@/themes/kafka/lessons/navigation";

/** The whole point of the server-side health fetch is a per-request status in the
 * first paint; without this the page would be prerendered once at build time with a
 * stale (always-"checking") status baked in. */
export const dynamic = "force-dynamic";

/** Shorter than the client poller's 2s: a slow SSR fetch delays the whole page. */
const INITIAL_HEALTH_TIMEOUT_MS = 500;

/** A failed/slow server-side fetch stays "checking" rather than "gateway-down":
 * with no prior good state to hold, a transient SSR-time blip would otherwise paint
 * a false outage that the first client poll immediately reverts (a new flicker). */
async function fetchInitialStatus(): Promise<EnvironmentStatusValue> {
  const outcome = await fetchHealthOutcome(`${GATEWAY_URL}/healthz`, {
    timeoutMs: INITIAL_HEALTH_TIMEOUT_MS,
  });
  return outcome.ok ? toEnvironmentStatus(outcome) : { kind: "checking" };
}

export default async function HomePage() {
  const initialStatus = await fetchInitialStatus();
  return (
    <main className="viz-root min-h-screen bg-(--page-plane) text-(--text-primary)">
      <div className="mx-auto flex max-w-7xl flex-col gap-6 p-8">
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          <section className="flex flex-col gap-4 rounded-lg border border-(--border) bg-(--surface-1) p-5">
            <div className="flex flex-col gap-1">
              <h2 className="text-lg font-semibold">Kafka</h2>
              <p className="text-sm text-(--text-secondary)">メッセージの流れとリバランスを観察</p>
              <EnvironmentStatus initialStatus={initialStatus} />
            </div>

            <Link
              href="/themes/kafka"
              className="flex h-9 w-fit items-center rounded bg-(--accent) px-4 text-sm font-medium text-(--on-accent) hover:opacity-90"
            >
              ダッシュボードを開く
            </Link>

            <ul className="flex flex-col">
              {lessons.map((lesson) => (
                <li key={lesson.id}>
                  <Link
                    href={lessonHref(lesson.id)}
                    className="inline-flex min-h-8 items-center text-sm text-(--accent) underline"
                  >
                    {lesson.title}
                  </Link>
                </li>
              ))}
            </ul>
          </section>

          <section className="flex flex-col gap-1 rounded-lg border border-(--border) p-5">
            <h2 className="text-lg font-semibold text-(--text-muted)">次のテーマ(準備中)</h2>
          </section>
        </div>
      </div>
    </main>
  );
}
