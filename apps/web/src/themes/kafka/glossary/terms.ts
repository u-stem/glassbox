import type { GlossaryCategory, GlossaryEntry, GlossaryTermId } from "./types";

/**
 * The dashboard's glossary. Wording is condensed from docs/themes/kafka.md
 * ("このテーマで学べること" / "4 パネルの読み方" / "既知の限界") and ADR-0003's
 * definitions, rather than written afresh -- those texts are already the reviewed
 * description of what this theme shows, and two divergent explanations of the same
 * mechanic would be worse than one.
 *
 * Typed as a full Record over GlossaryTermId, so adding an id without writing its
 * entry fails to compile instead of producing a term that renders as undefined.
 */
export const GLOSSARY: Readonly<Record<GlossaryTermId, GlossaryEntry>> = {
  topic: {
    ja: "トピック",
    en: "topic",
    body: "メッセージの論理的な入れ物。1 つのトピックは複数のパーティションに分割され、producer が送ったメッセージはそのいずれかに書き込まれる。",
    category: "basics",
    related: ["partition", "producer"],
  },
  partition: {
    ja: "パーティション",
    en: "partition",
    body: "トピックを分割した追記専用のログ。書き込み順序が保証されるのはこの単位であり、トピック全体では保証されない。画面では P0, P1, ... と表示する。",
    category: "basics",
    related: ["topic", "offset", "message-key"],
  },
  broker: {
    ja: "ブローカー",
    en: "broker",
    body: "メッセージを実際に保持する Kafka のサーバープロセス。このテーマでは Docker Compose で 1 台だけ動かしている。",
    category: "basics",
    related: ["topic", "admin-snapshot"],
  },
  producer: {
    ja: "プロデューサー",
    en: "producer",
    body: "トピックにメッセージを送る側。どのパーティションに書くかはキーとパーティショナーで決まる。",
    category: "basics",
    related: ["message-key", "partitioner", "consumer"],
  },
  consumer: {
    ja: "コンシューマー",
    en: "consumer",
    body: "トピックからメッセージを読む側。同じ group に属する consumer どうしでパーティションを分担する。",
    category: "basics",
    related: ["consumer-group", "assignment", "producer"],
  },
  "message-key": {
    ja: "メッセージキー",
    en: "message key",
    body: "メッセージに付ける任意の識別子。キーが同じメッセージは必ず同じパーティションに書き込まれるため、関連するメッセージの順序を保ちたいときに使う。キーなしなら全パーティションに分散する。",
    category: "basics",
    related: ["partitioner", "partition"],
  },
  partitioner: {
    ja: "パーティショナー",
    en: "partitioner",
    body: "書き込み先のパーティションを決める規則。キーがあればそのハッシュで決まり、なければ round-robin で順に振り分けられる。",
    category: "basics",
    related: ["message-key", "partition"],
  },

  offset: {
    ja: "オフセット",
    en: "offset",
    body: "パーティション内でのメッセージの位置を示す 0 から始まる通番。パーティションごとに独立しており、トピック全体で通し番号にはならない。",
    category: "offset",
    related: ["end-offset", "committed-offset", "partition"],
  },
  "end-offset": {
    ja: "ログ終端オフセット",
    en: "end offset",
    body: "そのパーティションに次に書き込まれる位置。produce するたびに増えていく。パーティション盤面では右側のマーカーで示す。",
    category: "offset",
    related: ["offset", "lag"],
  },
  "committed-offset": {
    ja: "コミット済みオフセット",
    en: "committed offset",
    body: "consumer group がそこまで処理し終えたと記録した位置。パーティション盤面では左側のマーカーで示し、ここから end offset までの隙間がラグになる。",
    category: "offset",
    related: ["commit", "lag", "offset"],
  },
  commit: {
    ja: "コミット",
    en: "commit",
    body: "「ここまで処理した」を broker に記録する操作。このテーマの consumer は自動コミットではなく、各メッセージの処理が終わってから明示的にコミットするため、表示が実際の処理進捗と一致する。",
    category: "offset",
    related: ["committed-offset", "consumer"],
  },
  lag: {
    ja: "ラグ",
    en: "lag",
    body: "end offset と committed offset の差、つまり未処理のメッセージ数。consumer の処理が produce に追いつかないと増え続ける。2 つの値は別々の問い合わせで取るため理論上は負になりうるが、意味のない値なので 0 で下限を切っている。",
    category: "offset",
    related: ["end-offset", "committed-offset", "slow-motion"],
  },

  "consumer-group": {
    ja: "コンシューマーグループ",
    en: "consumer group",
    body: "同じ groupId を持つ consumer の集まり。トピックのパーティションをメンバー間で分担して読み、1 つのパーティションは同時に 1 メンバーだけが担当する。",
    category: "group",
    related: ["group-id", "member", "assignment"],
  },
  "group-id": {
    ja: "グループ ID",
    en: "groupId",
    body: "consumer group を識別する文字列。同じ ID を指定した consumer が 1 つの group を構成する。この画面のデモは glassbox-consumers を使う。",
    category: "group",
    related: ["consumer-group"],
  },
  member: {
    ja: "メンバー",
    en: "member",
    body: "consumer group に参加している個々の consumer。参加・離脱のたびにリバランスが起きる。",
    category: "group",
    related: ["consumer-group", "rebalance", "lost-member"],
  },
  assignment: {
    ja: "割り当て",
    en: "assignment",
    body: "どのメンバーがどのパーティションを担当するかの割り振り。リバランスのたびに決め直される。トポロジーでは Topic から consumer へ伸びる線がこれを表す。",
    category: "group",
    related: ["rebalance", "member", "topology"],
  },
  rebalance: {
    ja: "リバランス",
    en: "rebalance",
    body: "メンバーの増減に応じてパーティションの割り当てを組み直す処理。この間は消費が一時的に止まる。通常は 1 秒未満で終わるため、観察するにはスローモーションを使う。",
    category: "group",
    related: ["group-state", "assignment", "slow-motion"],
  },
  "group-state": {
    ja: "グループの状態",
    en: "group state",
    body: "リバランスの進行段階。メンバーがいない Empty、参加者を待つ PreparingRebalance、割り当てを配る CompletingRebalance、消費中の Stable の 4 つを行き来する。",
    category: "group",
    related: ["rebalance", "consumer-group"],
  },
  "session-timeout": {
    ja: "セッションタイムアウト",
    en: "session timeout",
    body: "broker がメンバーの生存を諦めるまでの時間(既定 60 秒)。接続が切れても、この時間が過ぎるまで broker はそのメンバーを group に残したままにする。",
    category: "group",
    related: ["lost-member", "leave-group", "member"],
  },
  "leave-group": {
    ja: "離脱通知",
    en: "LeaveGroup",
    body: "consumer が group から抜けることを broker に伝える要求。graceful な停止ではこれが送られて即座にメンバーから消えるが、kill では送られない。",
    category: "group",
    related: ["lost-member", "session-timeout"],
  },
  "lost-member": {
    ja: "消失したメンバー",
    en: "lost",
    body: "この画面が接続を切ったのに、broker 側ではまだメンバーとして残っている状態。kill で consumer を消すと LeaveGroup が送られないため、セッションタイムアウトが過ぎるまでこの表示になる。トポロジーでは破線・薄い表示になる。",
    category: "group",
    related: ["session-timeout", "leave-group", "member"],
  },

  topology: {
    ja: "トポロジー",
    en: "topology",
    body: "Producer → Topic(パーティションのレーン)→ consumer group のメンバー、というデータの流れを図にしたパネル。メッセージが流れるたびに経路上を色付きのパルスが走る(色はパーティションごとに固定)。",
    category: "dashboard",
    related: ["assignment", "partition"],
  },
  event: {
    ja: "イベント",
    en: "event",
    body: "gateway が捕捉した 1 件の出来事。producer.send.end や consumer.commit のように種別名が付いており、イベントタイムラインに新しい順で並ぶ。",
    category: "dashboard",
    related: ["actor-id", "time-travel"],
  },
  "actor-id": {
    ja: "実行主体",
    en: "actorId",
    body: "そのイベントを起こした producer / consumer / admin の識別子。イベントタイムラインで絞り込みに使うと、特定の consumer の動きだけを追える。",
    category: "dashboard",
    related: ["event", "consumer"],
  },
  scenario: {
    ja: "シナリオ",
    en: "scenario",
    body: "この画面から gateway に実行させる操作のひとかたまり。メッセージの一括送信や consumer の追加・削除など、観察したい状況を意図的に作り出すために使う。",
    category: "dashboard",
    related: ["slow-motion", "producer", "consumer"],
  },
  "slow-motion": {
    ja: "スローモーション",
    en: "slow-motion",
    body: "リバランスの途中に遅延を入れて、通常は一瞬で終わる過渡状態を目で追えるようにする仕掛け。主に PreparingRebalance が引き延ばされる。",
    category: "dashboard",
    related: ["rebalance", "group-state"],
  },
  "time-travel": {
    ja: "タイムトラベル",
    en: "time travel",
    body: "蓄積済みのイベント履歴を巻き戻して、その時点の画面を再現する機能。遡れるのは直近 2000 件までで、それより古いものは破棄されている。",
    category: "dashboard",
    related: ["event"],
  },
  "admin-snapshot": {
    ja: "管理スナップショット",
    en: "admin snapshot",
    body: "gateway が 1 秒ごとに broker へ問い合わせて得る全体の断面。メンバー一覧とオフセットはこれを正としているため、表示は最大 1 秒ほど遅れることがある。",
    category: "dashboard",
    related: ["broker", "event"],
  },
};

/** Reading order of the drawer's glossary list: the mechanics first, then the two
 * things the panels actually measure, then the vocabulary specific to this screen. */
export const CATEGORY_ORDER: readonly GlossaryCategory[] = [
  "basics",
  "offset",
  "group",
  "dashboard",
];

export const CATEGORY_LABELS: Readonly<Record<GlossaryCategory, string>> = {
  basics: "基本のしくみ",
  offset: "オフセットとラグ",
  group: "consumer group とリバランス",
  dashboard: "この画面の用語",
};
