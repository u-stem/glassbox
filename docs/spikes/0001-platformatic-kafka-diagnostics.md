# 0001: `@platformatic/kafka` の diagnostics_channel / クライアントイベント spike

## 目的

`@platformatic/kafka`(v2.8.0)を暫定採用するにあたり、docs だけでは確認できない 2 点を実機(`docker/compose.yaml` の Kafka broker、KRaft シングルノード、localhost:9092)への実接続で実測した。

1. **actor 帰属**: `diagnostics_channel` はプロセスグローバルである。同一プロセス内に consumer を 2 つ(別 clientId)作ったとき、各イベントの payload から「どちらの consumer のイベントか」を判別できるか
2. **リバランス観測**: consumer の join/leave 時に、どのチャネル/イベントがどの順序で発火するか。ブローカー側の `Empty → PreparingRebalance → CompletingRebalance → Stable` に対応する遷移をクライアント側イベントのエッジで再構成できるか

## 手順

- scratchpad に使い捨てパッケージを作成し `@platformatic/kafka@2.8.0` を導入(Node v25.9.0、engines 要件 `>= 22.22.0 || >= 24.6.0` を満たす)
- `node_modules/@platformatic/kafka/dist/diagnostic.js` を読み、実在する全チャネル名を列挙(後述)
- 全チャネルを `diagnostics_channel.subscribe` で購読しつつ、tracing channel(`start`/`end`/`asyncStart`/`asyncEnd`/`error`)も個別に購読。加えて Consumer/Producer/Admin インスタンスに対して EventEmitter イベントも購読し、タイムスタンプ付き JSONL に dump するスクリプトを書いた
- シーケンス実行: topic 作成(4 partitions)→ producer で 20 件 produce(keyed)→ consumer A join(group `spike-group`, clientId `spike-a`)→ 3 秒待って消費確認 → consumer B join(clientId `spike-b`, リバランス発火)→ 3 秒待って再割当確認 → admin 3 API 実行 → B close(graceful)→ A close
- Admin API(`describeGroups` / `listConsumerGroupOffsets` / `listOffsets`)も実行し、戻り値の構造を確認

## 実測したチャネル一覧

`node_modules/@platformatic/kafka/dist/diagnostic.js` に定義されている全チャネル(namespace は `plt:kafka:*`)。今回のシナリオで実際に発火したものに ✅ を付けた。

| チャネル名 | 種別 | 発火 |
|---|---|---|
| `instances` | simple channel(単発 publish、`{ type, instance }`) | ✅ |
| `connections:connects` | tracing channel | ✅ |
| `connections:api` | tracing channel | ✅ |
| `connections:pool:get` | tracing channel | ✅ |
| `base:apis` | tracing channel | — (未発火。個別 API 系チャネルが優先して使われる) |
| `base:metadata` | tracing channel | ✅ |
| `admin:topics` | tracing channel | ✅(createTopics) |
| `admin:groups` | tracing channel | ✅(describeGroups) |
| `admin:clientQuotas` | tracing channel | — |
| `admin:logDirs` | tracing channel | — |
| `admin:consumerGroupOffsets` | tracing channel | ✅(listConsumerGroupOffsets) |
| `admin:configs` | tracing channel | — |
| `admin:acls` | tracing channel | — |
| `admin:offsets` | tracing channel | ✅(listOffsets) |
| `producer:initIdempotent` | tracing channel | — |
| `producer:sends` | tracing channel | ✅ |
| `producer:transactions` | tracing channel | — |
| `consumer:group` | tracing channel | ✅(findGroupCoordinator/joinGroup/syncGroup/leaveGroup を `operation` で判別) |
| `consumer:heartbeat` | tracing channel | ✅ |
| `consumer:receives` | tracing channel | ✅ |
| `consumer:fetches` | tracing channel | ✅ |
| `consumer:consumes` | tracing channel | ✅ |
| `consumer:commits` | tracing channel | — (autocommit のタイミングが実行時間内に来なかった) |
| `consumer:offsets` | tracing channel | ✅ |
| `consumer:lag` | simple channel | — (`consumer.on('consumer:lag', ...)` の EventEmitter 経路で、監視間隔がデフォルトでは今回の実行時間内に発火せず) |

重要な実装上の注意(はまりどころ): Node の `diagnostics_channel.tracingChannel(name)` が実際に `publish` するチャネル名は `tracing:${name}:${phase}` というプレフィックス付きの名前になる(`${name}:${phase}` ではない)。ライブラリの `createTracingChannel()` は `channel.name` に `plt:kafka:xxx`(プレフィックスなし)を代入しているが、これは表示用のラベルであり、実際に `diagnostics_channel.channel()` で subscribe すべき名前は `tracing:plt:kafka:xxx:start` 等である。Collector 実装時にこの命名規則を踏まえる必要がある(`node:diagnostics_channel` の `tracingChannel` 仕様どおりで、platformatic 固有の話ではない)。

EventEmitter 側で実際に発火した事象: `consumer:heartbeat:start` / `consumer:heartbeat:end` / `consumer:heartbeat:error` / `consumer:group:join` / `consumer:group:leave` / `consumer:group:rebalance` / `consumer:rejoin` / `client:close`。ドキュメント上言及されていた `consumer:group:rejoin` という名前ではなく、実際には `rejoin` イベントは引数なしで発火し、`group:join` イベントが generationId・isLeader・assignments を伴って発火する。

## 判定 1: actor 帰属 — 可能

`diagnostics_channel` の payload には `client`(または `instance`)キーで **発火元の Consumer/Producer/Admin/MessagesStream インスタンスそのもの**が渡される。実例(`consumer:group` channel、`joinGroup` 操作の `start` フェーズ):

```json
{
  "operationId": "122",
  "client": { "clientId": "spike-a", "groupId": "spike-group", "memberId": null },
  "operation": "joinGroup",
  "options": { "...": "..." }
}
```

インスタンスは `clientId` / `groupId` / `memberId` の getter を持つ(`dist/clients/base/base.js` の `kClientId` シンボル経由)ため、Collector 側で `payload.client.clientId` を読めば「どの consumer のイベントか」を一意に判別できる。同時に 2 台の consumer(`spike-a` / `spike-b`)を動かした実測でも、両者のイベントが同一プロセス内の同一チャネルに混在して流れてくるが、`clientId` で正しく分離できることを確認した。

EventEmitter 側のイベント(`consumer.on('consumer:group:join', ...)`)は、そもそも個々の Consumer インスタンスに対して購読するため、帰属問題は原理的に発生しない(Collector 側で `consumer.on(...)` する際に、どの consumer に対して listen したかを自分で知っている)。

結論: **帰属可能**。Phase 1〜2 の実装では、diagnostics_channel 購読時に payload の `client.clientId`(admin/producer/consumer 共通)を actorId として使う。

## 判定 2: リバランス観測 — 可能(4 状態への対応込み)

### 実測したイベントシーケンス(consumer B が join し、A・B ともにリバランスに巻き込まれる場面)

ブローカーログ(参考、実測):

```
Preparing to rebalance group spike-group in state PreparingRebalance with old generation 4
  (reason: Adding new member spike-b-... with group instance id null)
Stabilized group spike-group generation 5 with 2 members.
Assignment received from leader spike-a-... for group spike-group for generation 5.
```

同じ場面のクライアント側 dump(`t` はプロセス起動からの経過 ms、抜粋・整形):

```
t=6171  CHAN consumer:group:start   client=spike-b operation=joinGroup   memberId=null       # B が新規 join
t=6173  CHAN consumer:group:end     client=spike-b operation=joinGroup
t=6201  EVENT consumerA heartbeat:start                                                       # A は通常どおり heartbeat
t=6204  CHAN consumer:heartbeat:error client=spike-a error="Received response with error ... Heartbeat(v4)"
t=6204  EVENT consumerA group:rebalance {groupId: spike-group}                                 # ← A がリバランス検知(edge trigger)
t=6204  CHAN consumer:group:start   client=spike-a operation=joinGroup   memberId=spike-a-...  # A が再 joinGroup(PreparingRebalance 相当)
t=6209  CHAN consumer:group:start   client=spike-a operation=syncGroup                         # A が syncGroup(CompletingRebalance 相当)
t=6212  CHAN consumer:group:start   client=spike-b operation=syncGroup                          # B も syncGroup
t=6219  EVENT consumerA group:join  {generationId:18, isLeader:true,  assignments:[...]}       # A: Stable 到達
t=6220  EVENT consumerA rejoin      []
t=6221  EVENT consumerB group:join  {generationId:18, isLeader:false, assignments:[...]}       # B: Stable 到達(同一 generationId)
```

### 状態機械への対応

| UI 状態 | クライアント側で観測できる edge |
|---|---|
| `Empty → PreparingRebalance` | 既存メンバーがいる場合: 当該メンバーの次回 heartbeat が失敗し `consumer:heartbeat:error` → 直後に EventEmitter `consumer:group:rebalance` が発火。新規 join 側は `consumer:group` チャネルの `operation: 'joinGroup'` の `start` フェーズ |
| `PreparingRebalance → CompletingRebalance` | 各メンバーの `consumer:group` チャネル `operation: 'syncGroup'` の `start` フェーズ(joinGroup の応答でメンバーが確定した後) |
| `CompletingRebalance → Stable` | 各メンバーの EventEmitter `consumer:group:join`(`generationId` が新しい世代に一致した時点。全メンバーの `generationId` が揃った時が group 全体の Stable) |
| リーダー判定 | `consumer:group:join` の payload に `isLeader: boolean` が含まれる(実測で確認、リバランス後の再選出も観測可能) |

**残る注意点**: 上記は「どのメンバーがいつ PreparingRebalance/CompletingRebalance に入ったか」という **メンバー単位のローカル edge** である。「group 全体が今どの状態か」を 1 つの状態機械として合成するには、gateway 側で `groupId` ごとに参加メンバー集合を追跡し、全メンバーの edge が出揃った時点で group 全体の状態を遷移させるロジックが要る(ADR-0003 権威モデルの reducer 実装で対応)。今回の spike ではメンバーが 2 台程度の少数構成のみ検証しており、多数メンバーでの edge 到達順序のばらつきは未検証(Phase 2 で追加検証を推奨)。

`consumer:group` チャネルの payload には `operation` フィールドで `findGroupCoordinator` / `joinGroup` / `syncGroup` / `leaveGroup` の 4 種が判別できることを確認済み。

### leave(graceful)の実測

`consumer.close(true)` を呼ぶと、`consumer:group` チャネルで `operation: 'leaveGroup', force: true` の start/end が発火し、その後 EventEmitter `consumer:group:leave` → `client:close` の順で発火することを確認した。`force: true` は「ストリームを閉じてから LeaveGroup を送る」という意味であり、LeaveGroup リクエスト自体は通常どおり送信される(= graceful leave)。ストリームが開いたまま `force` なしで `close()` を呼ぶと `UserError: Cannot leave group while consuming messages.` で失敗する。

## group protocol オプション

Consumer コンストラクタのオプション名は **`groupProtocol`**(`docs` に頼らず `dist/clients/consumer/options.js` の JSON schema 定義と `dist/apis/enumerations.js` で確認)。

```ts
export const GroupProtocols = { CLASSIC: 'classic', CONSUMER: 'consumer' }
```

`new Consumer({ ..., groupProtocol: 'classic' })` で明示ピン可能。内部実装は `groupProtocol === 'consumer'` の場合のみ KIP-848(新プロトコル)の経路を使うため、未指定時のデフォルト挙動は classic 相当だが、計画どおり明示指定する。

## Admin API の実測

- `admin.describeGroups({ groups: [groupId] })` → `Map<groupId, Group>`。`Group.state` は Kafka のブローカー内部状態名がそのまま大文字文字列で返る(実測: `"STABLE"`)。`members` はテスト実行では空オブジェクト `{}` が返るケースがあり(1 メンバーのみの group、樹立直後)、メンバー詳細取得には追加オプションが必要な可能性がある(未確認、Phase 1〜2 実装時に要再確認)
- `admin.listConsumerGroupOffsets({ groups: [{ groupId, topics: [{ name, partitionIndexes }] }] })` → `groups` に単純に `groupId` の文字列だけを渡すと `topics: []` が返り、committed offset を取得するには `topics: [{ name, partitionIndexes }]` を明示する必要がある(実測して判明。docs だけでは分からなかった落とし穴)
- `admin.listOffsets({ topics: [{ name, partitions: [{ partitionIndex, timestamp }] }] })` → `timestamp: -1n` で log end offset(latest)、`-2n` で earliest が取得できる(Kafka プロトコル標準の特殊値、実測で `-1n` 指定時に妥当な offset が返ることを確認)。戻り値の `offset`/`timestamp` は `bigint`

## Phase 1〜2 実装への注意点

- **Collector が購読すべきチャネル名の生成規則**: tracing channel は `` `tracing:plt:kafka:${section}:${phase}` ``(phase は `start`/`end`/`asyncStart`/`asyncEnd`/`error`)、simple channel(`instances` / `consumer:lag`)は `` `plt:kafka:${name}` `` のまま。この 2 系統を取り違えないこと
- **actorId の取り出し方**: diagnostics_channel payload の `client`(Consumer/Producer/Admin)または `instance`(MessagesStream 等生成通知)フィールドから `.clientId` を読む。`unknown` で受けて Zod でスキーマ化する際、`client` フィールドは「クラスインスタンス」であり JSON.stringify できないため、Collector 境界で `clientId`/`groupId`/`memberId` 等の必要なプリミティブ値だけを抽出してから GlassboxEvent に詰める(インスタンスそのものを payload に含めない)
- **リバランス edge の合成**: メンバー単位の edge(`consumer:group:rebalance` → `syncGroup` start → `consumer:group:join`)を gateway 側で `groupId` ごとに集約し、group 全体の 4 状態機械に変換するロジックが world-reducer とは別に(または collector 内で前処理として)必要
- **`consumer:commits` / `consumer:lag` は今回発火せず**: autocommit のタイミングと lag monitoring の既定インターバルが spike の実行時間(十数秒)より長かったためと考えられる。Phase 1 実装時に produce-burst 中の実挙動を追加確認すること
- **listConsumerGroupOffsets は topics を明示しないと空配列が返る**落とし穴があるため、admin-poller 実装時は必ず `topics: [{ name, partitionIndexes }]` を渡す
