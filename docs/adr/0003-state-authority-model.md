# 0003: 状態の権威モデル(admin snapshot を authoritative baseline とする)

## Status

Accepted

## Context

glassbox の UI は 2 種類の時計が異なる情報源から状態を受け取る。

1. **クライアントイベント**(`consumer.group.*` / `consumer.heartbeat.*` / `group.state.changed` 等): consumer actor の EventEmitter・`diagnostics_channel` から edge-triggered に発火する、レイテンシの低い一次情報源。ただし個々の consumer/メンバー単位の視点でしか観測できず、group 全体・topic 全体の整合した断面を保証しない。
2. **admin snapshot**(`admin.snapshot`): `admin-poller` が `listGroups` → `describeGroups` → `listConsumerGroupOffsets` を定期実行して得る、ブローカーに問い合わせた断面。呼び出しごとに最大 `ADMIN_POLL_INTERVAL_MS`(既定 1s)古い可能性があり、かつ `describeGroups` と `listConsumerGroupOffsets` は別々の RPC なので単一トランザクションのスナップショットではない(非原子)。

この 2 つを reducer(`world-reducer.ts`)に無秩序に混ぜると、次の問題が起きる。

- admin snapshot が届いた瞬間、その直前にクライアントイベントで得ていた新しいメンバーシップ・オフセット情報を「古い admin snapshot」が上書きしてしまい、UI 上の状態がちらつく(反証レビューで指摘済み)。
- `describeGroups` と `listConsumerGroupOffsets` が非原子であるため、両者から計算した派生値(lag = end offset − committed offset)が理論上負になりうる。

## Decision

フィールドごとに「どちらが権威を持つか」を明文化し、reducer 実装前にこの ADR で固定する。

### 1. admin snapshot = authoritative baseline(メンバーシップ・committed/end offset)

- `admin.snapshot` イベントは取得時刻(`fetchedAt`、gateway が publish 時に付与する `ts` を流用)を持つ。
- reducer は、admin snapshot 由来の値(group のメンバー一覧、各パーティションの committed offset・end offset)を **その `fetchedAt` より古いタイムスタンプのイベントでは上書きしない**。つまり admin snapshot は「その時点までに観測された最も確実な断面」であり、reducer 内部で保持する `lastSnapshotAt` より前のタイムスタンプを持つクライアントイベントの再送・遅延到着があっても、snapshot の値を退行させない。
- 新しい admin snapshot が届いたら、その内容(メンバーシップ・オフセット)を無条件で正として採用する(次の snapshot まで基準は変わらない)。

### 2. クライアントイベント = optimistic overlay

- `consumer.group.*` / `group.state.changed` 等のクライアントイベントは、admin snapshot が届くまでの間、baseline の上に重ねる楽観的な差分として扱う。
  - 例: `consumer.group.joined` を受けてメンバーを画面に即座に反映するが、これは次の admin snapshot が届いた時点で reconcile(snapshot の内容に置き換え)される。
  - 例: `group.state.changed` による 4 状態(Empty/PreparingRebalance/CompletingRebalance/Stable)の遷移表示は、admin snapshot の `state`(ブローカー報告値をそのまま 4 状態に写像したもの)と食い違うことがあり得る。当初は「直近に観測されたどちらか新しい方(イベントの `ts` 比較)」で優先度を決める設計だったが、Phase 3 の目視レビューで実際に矛盾が再現した(下記「単純な ts 比較が破綻する理由」を参照)ため、**猶予期間(grace window)方式**に改めた。
- overlay は永続的な信頼源ではない。次の snapshot 到着で必ず reconcile される前提のため、overlay 側にバグがあっても最大 1 ポーリング間隔+猶予期間で自己修復する。

### 1.5 `state` フィールド専用の再解決規則(`STATE_RECONCILE_GRACE_MS`)

**単純な ts 比較が破綻する理由**: admin snapshot の `state` は `describeGroups` の 1 ポーリング tick の結果であり、そのイベント自体の `ts`(gateway が publish 時に付与)は「新しい」ものになる。しかし `describeGroups` が返す `state` の実値は、そのポーリング tick が発行された**瞬間のブローカー側の実際の状態**を反映しているとは限らない(ネットワーク遅延・ブローカー側の内部伝播ラグにより、直前に起きた遷移をまだ拾えていないことがある)。つまり「イベントの `ts` は新しいが、運んでいる `state` の値は古い」という状況が起こり得るため、`ts` の大小比較だけでは admin snapshot 側が優先すべきか判定できない(Phase 3 の目視レビューでこの矛盾が実際に再現した: `group.state.changed` で `PreparingRebalance` に遷移した直後の admin snapshot がまだ `Stable` を報告し、ハイライトが遷移履歴と食い違って見えた)。

そこで `state` フィールドに限り、次の規則を採用する(`apps/web/src/themes/kafka/world-reducer.ts` の `groupStateFromTransition` / `STATE_RECONCILE_GRACE_MS`):

- group ごとに、直近の `group.state.changed` の `to` と、そのイベントの `ts` を保持する。
- 次に admin snapshot が届いたとき、その snapshot 自身の `ts` と直近の `group.state.changed` の `ts` の差が **`STATE_RECONCILE_GRACE_MS`(2000ms、`ADMIN_POLL_INTERVAL_MS` 既定値の 2 倍)未満**であれば、admin snapshot の `state` を無視し、`group.state.changed` の `to` を表示に用い続ける(ポーリング tick 側がまだ追いついていないとみなす)。
- 差が `STATE_RECONCILE_GRACE_MS` 以上経過していれば、admin snapshot の `state` を正として採用する(reconcile)。`group.state.changed` が(collector 側の不具合等で)何らかの理由で二度と届かなかった場合でも、この規則により admin snapshot 側の実測値へ必ず収束し、UI が古い状態のまま永久に固まることはない。
- メンバーシップ・offsets には grace window を適用しない(引き続き無条件で admin snapshot 側が baseline)。`state` フィールドだけがこの特別扱いの対象である。

### 3. 派生値は防御的に clamp する

- lag(= end offset − committed offset)は `describeGroups` と `listConsumerGroupOffsets` が非原子であることに起因して理論上負になりうるため、**常に `Math.max(0, end - committed)` で計算する**(gateway 側の admin-poller で計算し、`admin.snapshot.groups[].offsets[].lag` として配布する。クライアント側で再計算しない)。
- clamp は「情報を隠す」のではなく「非原子な 2 回の RPC 由来の負値というノイズを、学習者に見せる意味のない数値として除去する」ためのものである。

### 4. group 全体の 4 状態はメンバー単位 edge の合成で導出する

spike([`docs/spikes/0001-platformatic-kafka-diagnostics.md`](../spikes/0001-platformatic-kafka-diagnostics.md))で実測した通り、クライアント側で直接観測できるのは「メンバー単位のローカル edge」であり、「group 全体が今どの状態か」は 1 つの状態機械として別途合成する必要がある。対応表:

| UI 状態遷移 | 観測される edge |
|---|---|
| `Empty → PreparingRebalance` | 新規メンバーの `consumer:group` チャネル `operation: 'joinGroup'` の `start`(= `consumer.group.joining`)。既存メンバーがいる場合は、その既存メンバーの `consumer:heartbeat:error` → EventEmitter `consumer:group:rebalance`(= `consumer.group.rebalancing`)が先行して発火する |
| `PreparingRebalance → CompletingRebalance` | 参加中の各メンバーの `consumer:group` チャネル `operation: 'syncGroup'` の `start`(= `consumer.group.syncing`) |
| `CompletingRebalance → Stable` | 参加中の**全メンバー**の EventEmitter `consumer:group:join`(= `consumer.group.joined`、`generationId` を伴う)が、同一 `generationId` で出揃った時点 |
| メンバー離脱 | `consumer:group` チャネル `operation: 'leaveGroup'` および EventEmitter `consumer:group:leave`(= `consumer.group.left`) |

この合成は `group-state-tracker.ts` の純関数(`groupId` ごとにメンバー集合とメンバー単位 edge を受け取り、group 全体の状態と遷移前後を返す)として実装し、edge 到達順序が入れ替わるケース(spike では未検証と明記されている)も含めて TDD する。合成結果は `group.state.changed`(`from`/`to`/`generationId`/導出元 `edge`)としてイベント化し、reducer はこれをクライアントイベント側の overlay として扱う。

## Consequences

- reducer(`world-reducer.ts`)は「admin snapshot の `fetchedAt` より古いクライアントイベントで baseline を退行させない」という順序不変条件を持つため、テストケースとして「snapshot 到着後に、その snapshot より前の `ts` を持つクライアントイベントが遅延到着する」ケースを明示的に用意する。
- `state` フィールドは `STATE_RECONCILE_GRACE_MS`(2000ms)の猶予期間内、`group.state.changed` の `to` が admin snapshot の `state` より優先される。**client イベントが何らかの理由で欠落した場合(collector のバグ・ネットワーク瞬断等)は、猶予期間経過後に admin snapshot 側の実測値へ自動的に reconcile される**(自己修復)。この結果、`state` は「最大で `STATE_RECONCILE_GRACE_MS` 分だけ古い表示が許容される」代わりに、「client イベントが永久に欠落しても UI が古い状態のまま固まり続けることはない」という保証を得る。テストケースとして「猶予期間内は admin snapshot の(stale な)`state` に上書きされない」「猶予期間経過後は admin snapshot の `state` に reconcile される」の両方を `world-reducer.test.ts` に用意する(Phase 3 で追加済み)。
- lag が clamp される結果、UI 上は「常に 0 以上」の値しか表示されない。学習者向けに、これは意図的な防御的挙動であることを `docs/themes/kafka.md`(Phase 4)で説明する。
- group 全体の 4 状態は gateway 側(`group-state-tracker.ts`)で合成してから配布するため、web 側の `world-reducer.ts` は「合成済みの `group.state.changed`」と「admin snapshot の `groups[].state`」の 2 つの情報源を突き合わせるだけでよく、メンバー単位 edge を web 側で再合成する必要はない。
- 多数メンバー構成での edge 到達順序のばらつきは spike で未検証(注記済み)のため、`group-state-tracker.ts` の TDD で意図的に順序をシャッフルしたテストケースを追加し、実装のロバスト性を先に固める。
