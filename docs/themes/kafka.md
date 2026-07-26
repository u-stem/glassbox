# Kafka テーマ

glassbox の最初のテーマ。本物の Apache Kafka(Docker Compose、KRaft シングルノード)を動かし、produce/consume・パーティショニング・consumer group のリバランス・オフセットとラグを、実際のイベントを捕捉しながら可視化して学ぶ。

## このテーマで学べること

- **パーティショニング**: トピックが複数パーティションに分かれていること、produce するメッセージのキーがパーティション選択にどう影響するか(キーあり = 同じキーは同じパーティション、キーなし = round-robin で分散)
- **produce/consume の流れ**: producer → topic(パーティション)→ consumer group という基本的なデータフロー
- **consumer group とリバランス**: consumer の参加・離脱のたびに group 全体が `Empty → PreparingRebalance → CompletingRebalance → Stable` と状態遷移すること
- **オフセットとラグ**: committed offset と end(log end)offset の差がラグであること、consumer が遅いとラグが蓄積すること
- **graceful な離脱と kill の違い**: 前者は即座に LeaveGroup が送られるが、後者(kill)は broker がセッションタイムアウトを検知するまでメンバーとして残り続けること

## 4 パネルの読み方

画面には 4 つのパネルが並ぶ。

1. **Topology**(トポロジーキャンバス): Producer → Topic(パーティションレーン)→ Consumer group のメンバーという構成を図示する。メッセージが実際に流れるたびに、経路に沿って色付きのパルスがアニメーションする(色はパーティションごとに固定)。kill された consumer は破線・薄い表示で "lost" と分かるようにしている。
2. **Rebalance state**(状態遷移図): consumer group の 4 状態(Empty/PreparingRebalance/CompletingRebalance/Stable)を固定レイアウトで常設表示し、現在の状態をハイライト、直近の遷移履歴を下に一覧表示する。
3. **Partitions**(パーティション盤面): 各パーティションのレーンに committed offset と end offset をマーカーで示し、その間隙(= ラグ)を帯で可視化する。produce/consume/ラグを最も直接的に示すパネル。
4. **Event timeline**(イベントタイムライン): gateway から届いた生イベントを新しい順に一覧表示する。フィルタで種別・actor を絞り込める。

これら 4 パネルの裏側にある状態管理の権威モデル(admin snapshot と client イベントのどちらを優先するか)は [ADR-0003](../adr/0003-state-authority-model.md) を参照。

## シナリオの見どころ

コントロールバー上部の「シナリオ実行」フォーム(Phase 4 で自動生成、後述)から、以下のシナリオを実行できる。

| シナリオ | 見どころ |
|---|---|
| `produce-burst`(topic, count, rateMs, keyStrategy) | `keyStrategy=keyed` は同じキーが同じパーティションに集まる様子、`round-robin` は全パーティションへの均等な分散を、パーティション盤面とトポロジーのパルスで比較できる |
| `add-consumer`(groupId, topics) | consumer が group に参加し、リバランスを経て assignment が確定するまでの一連を状態遷移図で観察できる |
| `remove-consumer`(consumerId, mode: graceful/kill) | graceful は即座にメンバーから消えるが、kill は broker のセッションタイムアウト(既定 60 秒)が過ぎるまで "lost" 状態で残り続ける |
| `slow-consumer`(consumerId, processingDelayMs) | consumer の処理を意図的に遅くし、パーティション盤面でラグが単調に増えていく様子を観察できる。offset は autocommit ではなく各メッセージの処理(processingDelayMs のsleep を含む)完了後に明示的に commit しており、committed offset が実際の処理進捗と一致するようにしている |
| スローモーション トグル | 下記「スローモーションの限界」を参照 |

各シナリオのパラメータは `paramsSchema`(Zod)から JSON Schema として生成され、web 側で自動的にフォームが組み立てられる(`GET /api/themes/kafka/scenarios`、`apps/web/src/themes/kafka/scenario-form/`)。頻繁に使う「Produce 10」「Add consumer」はデフォルト値のままワンクリックで実行できるショートカットボタンとして残してあり、それ以外のパラメータを変えたい場合はシナリオを選んでフォームを埋める。

## ガイド付きレッスン

画面右上の「レッスン」ボタンから、データ駆動のステップ列として書かれたガイド付きレッスンを開始できる(`apps/web/src/themes/kafka/lessons/`)。レッスンパネルはモードレスで、開いている間も他の操作は妨げられない。

- **Partitioning 入門**: キー付き produce がなぜ同じパーティションに集まるのかを、盤面とトポロジーで観察し、round-robin と比較する。
- **Consumer group リバランス**: consumer の追加・停止のたびに group がどう状態遷移するかを観察する。実際のリバランスは数百 ms 未満で終わってしまうため、途中でスローモーションを ON にしてから 2 台目の consumer を追加し、`PreparingRebalance` で数秒間留まる様子を確認する。最後に consumer を kill し、"lost" 表示とその後のリバランスを観察する。

各ステップは「実行するシナリオ + パラメータ」を持ち、「このステップを実行」ボタンで gateway に対して実際にシナリオを発火できる。kill 対象の consumerId のように実行時まで確定しない値は、現在の world 状態から動的に解決する(直近に参加した生存中の consumer を選ぶ)。

## タイムトラベル(スクラブ再生)

画面上部の「Time travel」ボタンで、蓄積済みのイベント履歴(最大 2000 件)を任意の時点までシークして、その時点の world を再現できる。

- 入る瞬間の event バッファを固定してインデックスを構築し(`apps/web/src/themes/kafka/time-travel/time-travel.ts`)、シークバーで指定した `seq` まで再度 reduce して各パネルに表示する。再構成はチェックポイント(200 イベントごと)を挟んで行うため、毎フレームすべてのイベントを最初から reduce し直すことはない。
- 過去にシークしている間も、ライブの新規イベントは裏側で蓄積が継続する(「LIVE に戻る」を押すと最新状態の表示に戻る)。
- Play ボタンで自動的にシークバーを進める(`requestAnimationFrame` でスロットルしたスクラブ)。
- 4 パネルすべて(トポロジー・状態遷移図・パーティション盤面・イベントタイムライン)がシークした時点の状態を表示する。

## 既知の限界

- **スローモーションは主に `PreparingRebalance` にしか効かない**: `protocolsMetadata` フック(joinGroup 側)には遅延注入の余地があるが、`partitionAssigner`(syncGroup 側)は同期実行のため、`CompletingRebalance` を同じ手段で引き延ばすことはできない。詳細は `apps/gateway/src/themes/kafka/actors/consumer-actor.ts` の `protocolsMetadata` 実装コメントを参照。
- **KIP-848(次世代 consumer group プロトコル)は対象外**: このテーマの consumer は `groupProtocol: classic` に明示的にピンしており、UI の 4 状態モデル(Empty/PreparingRebalance/CompletingRebalance/Stable)もそれに合わせている。KIP-848 は状態モデルそのものが異なるため、対応する場合は将来の別テーマとして扱う([ADR-0001](../adr/0001-kafka-client-selection.md) 参照)。
- **ラグは常に 0 以上に clamp される**: `describeGroups` と `listConsumerGroupOffsets` が別々の RPC(非原子)であるため、理論上ラグが負になり得るが、意味のないノイズとして `Math.max(0, end - committed)` で除去している([ADR-0003](../adr/0003-state-authority-model.md) 参照)。
- **タイムトラベルの再現範囲はイベントバッファ(最大 2000 件)に限られる**: それより古いイベントは既に破棄されているため遡れない。
- **ブローカーを停止して再開すると、一部の consumer は消費を再開しない**: ホーム画面からブローカーを停止・再起動した場合、admin poller は自動で `connected` に戻り(実測で 3〜6 秒)、producer も次の送信で再接続する。一方 consumer は、消費ストリームが切断で reject するとそのまま再試行しない(`apps/gateway/src/themes/kafka/actors/consumer-actor.ts` のクラスコメント "does not retry automatically")。どの consumer がそうなるかは決定的ではなく、実測では「2 台とも脱落」「1 台だけ脱落」の両方が起きた。
  - 脱落した consumer は gateway のレジストリにもブローカーの group にも member として残り続けるが、ラグは増える一方で減らない。gateway のログに `consumer consumer-N stopped consuming` が出ているのが唯一の手がかりになる。復旧するには「consumer を削除」してから追加し直す。
  - **これを自動化しようとして取りやめた経緯がある**。`onConsumeError` を受けて consumer をレジストリから自動撤去する実装を試したが、同じ reject は通常のリバランス中にも `TimeoutError: Request timed out` として発生し(レッスン B のウォークスルーでは毎回発生した)、追加したばかりの健全な consumer まで消えてしまった。「消費していない member が残る」より「操作した直後に consumer が消える」ほうが体験として悪いため、自動撤去は入れていない([ADR-0005](../adr/0005-broker-lifecycle-ownership.md) の Consequences 参照)。
- **停止中に切断された consumer は、ブローカー側の group からも自動では消えない**: `remove-consumer` の graceful 撤去は LeaveGroup を送ろうとするが、停止したブローカーには届かない。その後も `@platformatic/kafka` の `Consumer` がメンバーシップを維持し続けるため、ブローカーは数分後も STABLE の member として全パーティションを割り当てたまま報告する(gateway プロセスを再起動すると解消する)。kill モードで削除すれば接続が破棄され、セッションタイムアウト(既定 60 秒)で期限切れになる。
- **admin snapshot の反映には最大 `ADMIN_POLL_INTERVAL_MS`(既定 1 秒)のずれがあり得る**: state フィールドについては猶予期間(`STATE_RECONCILE_GRACE_MS`)方式で client イベント側を優先するが、メンバーシップ・オフセットは常に snapshot が権威を持つ([ADR-0003](../adr/0003-state-authority-model.md) 参照)。
