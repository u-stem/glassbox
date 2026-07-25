# 0001: Kafka クライアントに `@platformatic/kafka` を採用する

## Status

Accepted

## Context

glassbox は Kafka の内部イベント(produce/consume、consumer group のリバランス、オフセット変化など)を実際の Kafka broker から捕捉し、ブラウザ上でリアルタイムに可視化する学習プラットフォームである。この目的を達成するには、Node.js 用 Kafka クライアントが「内部イベントを取得できるか」を第一基準として選定する必要がある。

候補として次の 3 つを比較した(2026-07 時点の一次情報)。

| 候補 | 内部イベントの取得可否 |
|---|---|
| kafkajs | `instrumentation-events`(`REBALANCING` / `GROUP_JOIN` 等)を持つが、2023 年以降リリースが止まっており保守状況が不安 |
| `@confluentinc/kafka-javascript` | 内部が librdkafka(C 実装)に隠蔽されており、`on`/`events` API で内部状態遷移を取得する手段がない(公開 issue #241 で認知済みの制約) |
| `@platformatic/kafka` | Pure TypeScript。EventEmitter イベント(`consumer:group:join` 等)と `diagnostics_channel`(`plt:kafka:*` 名前空間)の両方で内部イベントを公開。活発にリリースされている(v2.8.0、週 1〜2 回のリリース頻度) |

`@platformatic/kafka` は最有力候補だったが、`diagnostics_channel` がプロセスグローバルであるという性質上、「同一プロセス内で複数の consumer インスタンスを動かしたとき、イベントの発生源(actor)を判別できるか」が公開ドキュメントだけでは確認できず、decision gate として実機 spike での検証が必要だった([`docs/spikes/0001-platformatic-kafka-diagnostics.md`](../spikes/0001-platformatic-kafka-diagnostics.md))。

## Decision

**`@platformatic/kafka`(v2.8.0)を Kafka クライアントとして採用する。**

spike で以下を実機(Kafka broker、KRaft シングルノード、`docker/compose.yaml`)への実接続により確認し、decision gate の 2 点とも問題なくクリアした。

1. **actor 帰属は可能**: `diagnostics_channel` の payload には発火元インスタンス(Consumer/Producer/Admin/MessagesStream)そのものが `client`(または `instance`)キーで渡される。インスタンスは `clientId`(および consumer なら `groupId`/`memberId`)の getter を持つため、Collector 側で `payload.client.clientId` を読めば、同一プロセス内に複数の consumer が存在してもイベントの発生源を一意に判別できることを、2 台の consumer(`clientId: spike-a` / `spike-b`)を同時に動かして実測確認した。EventEmitter イベントは個々のインスタンスに対して購読するため、そもそも帰属問題が発生しない。
2. **リバランス観測は可能**: consumer B が group に join した際、既存の consumer A 側で `consumer:heartbeat:error` → EventEmitter `consumer:group:rebalance` が edge-triggered で発火し、その後 `consumer:group` チャネルの `operation: 'joinGroup'`(PreparingRebalance 相当)→ `operation: 'syncGroup'`(CompletingRebalance 相当)→ EventEmitter `consumer:group:join`(Stable 到達、`generationId`/`isLeader` 付き)という順序でメンバー単位の edge を実測できた。ブローカーログの `PreparingRebalance` → `Stabilized(generation N)` という遷移と時系列的に対応することを確認した。ポーリング不要でクライアントイベント駆動によるリバランス状態機械の駆動が可能と判断する。

そのほか、以下も実機で確認済み:

- Consumer コンストラクタのオプション名は `groupProtocol`(値は `'classic'` / `'consumer'`)。`classic` を明示指定することでリバランス状態機械を Kafka 4.x デフォルトの KIP-848 とは別の従来モデルに固定できる
- Admin API(`describeGroups` / `listConsumerGroupOffsets` / `listOffsets`)はいずれも動作し、戻り値の構造(`Group.state` は大文字文字列、`listConsumerGroupOffsets` は `topics` を明示しないと空配列を返す落とし穴、`listOffsets` の `timestamp: -1n`/`-2n` 特殊値)を実測確認した

## Consequences

- Collector 実装では、Node の `diagnostics_channel.tracingChannel()` が実際に publish するチャネル名が `` `tracing:plt:kafka:${section}:${phase}` `` という命名規則になる(ライブラリの `createTracingChannel()` が設定する `channel.name` 表示ラベルとは異なる)ことを踏まえて購読する必要がある。詳細は spike ドキュメント参照
- diagnostics_channel の payload に含まれる `client`/`instance` はクラスインスタンスであり、そのまま GlassboxEvent の payload(Zod でパースされる `unknown` 境界)に流し込むことはできない。Collector 境界で `clientId`/`groupId`/`memberId` 等の必要なプリミティブ値のみを抽出してから正規化する
- リバランス状態機械は、メンバー単位の edge(`consumer:group:rebalance` → `syncGroup` → `consumer:group:join`)を gateway 側で `groupId` ごとに集約して group 全体の 4 状態(`Empty`/`PreparingRebalance`/`CompletingRebalance`/`Stable`)に合成するロジックが別途必要になる。この合成ロジックは ADR-0003(権威モデル)の reducer 実装で扱う
- `consumer:commits` / `consumer:lag` は spike の実行時間内では発火しなかった(autocommit・lag monitoring の既定インターバルの問題と推測)。Phase 1〜2 の produce-burst 実装時に実挙動を追加確認する
- Consumer は `groupProtocol: 'classic'` を明示指定する。KIP-848(`groupProtocol: 'consumer'`)は将来の発展テーマとして docs に記載するに留める
- 保守状況(週 1〜2 回のリリース頻度)は 2026-07 時点の観測であり、今後の破壊的変更リスクは残る。メジャーバージョンアップ時は diagnostics_channel のチャネル名・payload 構造が変わる可能性があるため、Collector のテスト(統合テスト、実 Kafka に対する spike と同様のシナリオ)で早期検知する
