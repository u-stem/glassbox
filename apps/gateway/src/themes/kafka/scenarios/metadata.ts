export type KafkaScenarioId =
  | "produce-burst"
  | "add-consumer"
  | "remove-consumer"
  | "slow-consumer";

export interface ScenarioCopy {
  title: string;
  description: string;
}

/**
 * Human-readable names for the scenario picker in the web UI. Japanese leads because
 * that is the reading language, with the scenario id in parentheses: the id is what
 * appears in the event timeline (scenario.started / scenario.finished) and in the
 * REST path, so dropping it would leave no way to connect the two.
 *
 * A full Record over the id union, so registering a scenario without writing its copy
 * fails to compile -- that is a stronger guarantee than a test asserting the same
 * thing, which is why there isn't one.
 */
export const SCENARIO_COPY: Readonly<Record<KafkaScenarioId, ScenarioCopy>> = {
  "produce-burst": {
    title: "メッセージをまとめて送る(produce-burst)",
    description:
      "producer から指定した件数のメッセージを送る。キーの決め方でパーティションへの振り分け方が変わる。",
  },
  "add-consumer": {
    title: "consumer を 1 台追加する(add-consumer)",
    description: "group に新しい consumer を参加させる。参加のたびにリバランスが起きる。",
  },
  "remove-consumer": {
    title: "consumer を削除する(remove-consumer)",
    description:
      "graceful は離脱通知を送って即座にメンバーから消え、kill はセッションタイムアウトが過ぎるまで残る。",
  },
  "slow-consumer": {
    title: "consumer の処理を遅くする(slow-consumer)",
    description: "1 件あたりの処理に遅延を入れ、ラグが増えていく様子を作る。",
  },
};
