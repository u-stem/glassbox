import { latestAliveMemberClientId } from "./action-resolver";
import type { Lesson } from "./types";

const DEMO_TOPIC = "glassbox.demo";
const DEMO_GROUP_ID = "glassbox-consumers";

/**
 * Lesson A: keyed produce routes identical keys to the same partition; round-robin
 * spreads messages across partitions instead. See docs/themes/kafka.md for the
 * write-up this lesson's content mirrors.
 */
export const partitioningLesson: Lesson = {
  id: "partitioning-101",
  title: "Partitioning 入門",
  summary:
    "キー付き produce がなぜ同じパーティションに集まるのかを、盤面とトポロジーで観察します。",
  steps: [
    {
      id: "intro",
      title: "パーティションとは",
      body:
        "Kafka のトピックは複数のパーティション(ログ)に分割されています。producer が送るメッセージは" +
        "パーティションのいずれか 1 つに書き込まれ、同じパーティション内では書き込み順序が保証されます。" +
        "トポロジーのパネルを見ると、Topic ノードの中に P0, P1, ... というパーティションのレーンが並んでいます。",
      observe: "トポロジーの Topic ノード内に並ぶパーティションレーン(色分けされた P0, P1, ...)",
    },
    {
      id: "keyed-produce",
      title: "キー付きで 20 件 produce する",
      body:
        "produce-burst シナリオを keyStrategy=keyed で実行します。このモードでは `key-0` 〜 `key-3` の" +
        "4 種類のキーだけを使い回すため、同じキーのメッセージは常に同じパーティションに書き込まれます。",
      observe:
        "「パーティション」パネルで、特定のレーンだけ end offset が集中的に伸びていくこと。" +
        "トポロジーのパルスアニメーションでも、同じ色のパルスが同じ経路を繰り返し通ることが確認できます。",
      action: {
        kind: "scenario",
        scenarioId: "produce-burst",
        params: { count: 20, keyStrategy: "keyed" },
      },
    },
    {
      id: "round-robin-produce",
      title: "round-robin で 20 件 produce する(比較)",
      body:
        "同じ produce-burst シナリオを、今度は keyStrategy=round-robin(キーなし)で実行します。" +
        "この場合はデフォルトのパーティショナーが順番にパーティションを割り当てるため、メッセージは" +
        "ほぼ均等に全パーティションへ分散します。",
      observe:
        "「パーティション」パネルで、先ほどと違い全レーンの end offset がまんべんなく伸びていくこと。",
      action: {
        kind: "scenario",
        scenarioId: "produce-burst",
        params: { count: 20, keyStrategy: "round-robin" },
      },
    },
    {
      id: "summary",
      title: "まとめ",
      body:
        "同じキーは同じパーティションに(順序保証・関連メッセージのグルーピングに有効)、キーなしは" +
        "全パーティションに分散(スループット重視)という、2 つの produce パターンの使い分けを観察できました。",
    },
  ],
};

/**
 * Lesson B: adding a 2nd consumer to a group triggers a rebalance
 * (Empty -> PreparingRebalance -> CompletingRebalance -> Stable); killing a consumer
 * shows the "lost" state until the broker's session timeout finally drops it from
 * membership. Slow-motion is turned on partway through so the (normally
 * sub-second) PreparingRebalance/CompletingRebalance states are actually visible.
 */
export const rebalanceLesson: Lesson = {
  id: "consumer-group-rebalance",
  title: "Consumer group リバランス",
  summary:
    "consumer の追加・停止のたびに group がどう状態遷移するかを、状態遷移図とスローモーションで観察します。",
  steps: [
    {
      id: "intro",
      title: "Consumer group とリバランスとは",
      body:
        "同じ groupId を持つ consumer は 1 つの consumer group を構成し、トピックのパーティションを" +
        "分担して読み込みます。メンバーが増減するたびに group は Empty → PreparingRebalance → " +
        "CompletingRebalance → Stable という状態を遷移しながら、パーティションの再割当てを行います。",
      observe: "「リバランス状態」パネルの状態遷移図(4 つの状態が横に並んだ図)",
    },
    {
      id: "add-first-consumer",
      title: "1 台目の consumer を追加する",
      body: "add-consumer シナリオを実行し、group に最初の consumer を参加させます。",
      observe:
        "状態遷移図が Empty → PreparingRebalance → CompletingRebalance → Stable と一直線に進み、" +
        "トポロジーに consumer-1 が現れること。",
      action: { kind: "scenario", scenarioId: "add-consumer", params: { groupId: DEMO_GROUP_ID } },
    },
    {
      id: "enable-slow-motion",
      title: "スローモーションを ON にする",
      body:
        "実際のリバランスは数百 ms 未満で終わってしまい、Preparing/Completing の各状態を目で追うのは" +
        "困難です。スローモーショントグルは、consumer の再参加(rejoin)処理に遅延を注入し、この過渡状態を" +
        "人の目で観察できる長さまで引き延ばします。",
      observe: "「再生モード」の「スローモーション」ボタンが ON になること",
      action: { kind: "slow-motion", enabled: true, factorMs: 4000 },
    },
    {
      id: "add-second-consumer",
      title: "2 台目の consumer を追加する(スローモーション中)",
      body:
        "同じ group にもう 1 台 consumer を追加します。スローモーションが有効なため、今度は" +
        "PreparingRebalance で数秒間留まる様子を確認できます(スローモーションは主に Preparing 側に効きます。" +
        "詳しくは docs/themes/kafka.md の既知の限界を参照)。",
      observe: "状態遷移図が PreparingRebalance のノードで数秒ハイライトされ続けること。",
      action: { kind: "scenario", scenarioId: "add-consumer", params: { groupId: DEMO_GROUP_ID } },
    },
    {
      id: "kill-consumer",
      title: "consumer を kill する",
      body:
        "直近に参加した consumer を kill モードで削除します。kill は LeaveGroup を送らず接続を" +
        "即座に切断するため、broker 側はセッションタイムアウトが過ぎるまでそのメンバーを group に" +
        "残したままにします。",
      observe:
        'トポロジー / メンバー一覧でその consumer が薄く破線表示の "lost" になり、しばらくしてから' +
        "リバランスが発生してメンバーから消えること。",
      action: {
        kind: "scenario",
        scenarioId: "remove-consumer",
        params: (world) => {
          const clientId = latestAliveMemberClientId(world, DEMO_GROUP_ID);
          return clientId === undefined ? undefined : { consumerId: clientId, mode: "kill" };
        },
      },
    },
    {
      id: "disable-slow-motion",
      title: "スローモーションを OFF に戻す",
      body: "観察が終わったら、通常速度に戻しておきます。",
      action: { kind: "slow-motion", enabled: false },
    },
    {
      id: "summary",
      title: "まとめ",
      body:
        "consumer の参加・離脱が group 全体の状態遷移を引き起こすこと、kill と graceful な離脱とでは" +
        "broker からメンバーが消えるタイミングが異なることを観察できました。",
    },
  ],
};

export const lessons: Lesson[] = [partitioningLesson, rebalanceLesson];

/** Referenced by lesson steps above and by the E2E smoke test; kept here (not
 * re-derived from produceBurstParamsSchema) since the web app has no dependency on
 * the gateway's zod schemas. */
export const LESSON_DEMO_TOPIC = DEMO_TOPIC;
export const LESSON_DEMO_GROUP_ID = DEMO_GROUP_ID;
