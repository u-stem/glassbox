# 0004: gateway プロセスの耐障害性(`uncaughtException` 最終防衛)

## Status

Accepted

## Context

Phase 2 のフォローアップ調査で、add-consumer/remove-consumer(kill)を繰り返すと gateway プロセスが不定期にクラッシュする現象が見つかった(タスク #7「gateway の joining スタック現象を調査・解消する」で解消済み)。調査の結果、2 つの異なる種類のクラッシュ原因が判明した。

### 原因 1: `EventEmitter` の `'error'` に listener がゼロのまま emit される

`@platformatic/kafka` の各クライアント(`Admin`/`Producer`/`Consumer`)は Node の `EventEmitter` を継承しており、Node は `'error'` イベントが listener 0 個の状態で emit されると、そのプロセス全体を無条件でクラッシュさせる(Node 自身の仕様)。これは kill シナリオに限らず、この gateway プロセスが生成するすべてのクライアントインスタンスに当てはまる一般的なリスクである。

対策として、`server.ts`・`producer-actor.ts`・`consumer-actor.ts`・`admin-poller.ts` の生成箇所すべてで、コールバックの有無に関わらず必ず `.on("error", ...)` を登録している(コールバック未指定でもリスナー自体は常に貼る、というのがこの listener 登録の設計)。これは通常の `try/catch` で捕捉できるエラーではなく、EventEmitter の `'error'` セマンティクス特有の対処である。

### 原因 2: ライブラリ内部の同期 throw(`@platformatic/kafka@2.8.0` の `kafkaError.findBy` バグ)

上記の listener 登録だけでは防ぎきれない、別種のクラッシュが repeated kill シナリオで再現した。

`@platformatic/kafka@2.8.0` の `Consumer#handleError` は、少なくとも 1 つのコードパス(この gateway が kill シナリオのために意図的に切断したままにしている接続に対する、繰り返しのメタデータ再取得失敗)で `kafkaError.findBy(...)` を呼び出すが、そのエラーオブジェクトには `findBy` メソッドが存在せず、素の同期 `TypeError` を送出する。この呼び出しは:

- `EventEmitter` の `'error'` emission **ではない**ため、上記の `.on('error', ...)` では捕捉できない。
- どの呼び出しスタックの try/catch にも囲まれていない、真の同期例外である。

Node がこの種の「どこの try/catch にも囲まれていない同期例外」を捕捉できる唯一のフックは、プロセスグローバルな `process.on('uncaughtException', ...)` である。

## Decision

`server.ts` に、次の 2 つのプロセスグローバルハンドラを最終防衛として登録する。

```ts
process.on("uncaughtException", (error) => {
  app.log.error(error, "uncaughtException (see server.ts's doc on this handler)");
});
process.on("unhandledRejection", (reason) => {
  app.log.error(reason, "unhandledRejection (see server.ts's doc on this handler)");
});
```

これは「あらゆるクラッシュを握りつぶす」一般ポリシーでは **ない**。次の condition に限定される、狭いセーフティネットである。

- 自前のコードで発生源を特定でき、通常の try/catch や `.on('error', ...)` で処理できるエラーは、引き続きそこで処理する(このハンドラに丸投げしない)。
- このハンドラは、**この gateway 自身の kill シナリオ設計が誘発しやすい**、サードパーティライブラリ側の既知のバグ(繰り返し接続を破棄するという、ライブラリの自前テストがおそらくカバーしていないエッジケース)を対象にした、意図的かつ狭い網である。
- ログを出してプロセスを生かし続けるだけで、他の箇所で処理可能なエラーを意図的に隠蔽するものではない。

## Consequences

- gateway プロセスは、この既知のライブラリバグに起因する同期例外では停止しなくなった。add-consumer/remove-consumer(kill)を繰り返す運用(学習用途では頻発する操作パターン)でもプロセスが生存し続ける。
- この防御は `@platformatic/kafka@2.8.0` 固有のバグに対する回避策であり、ライブラリのバージョンアップで `findBy` 呼び出し自体が修正されれば不要になる可能性がある。バージョンを上げる際は、このハンドラのログに実際に何か記録され続けているか確認し、記録がなくなっていれば `docs/adr/0004-process-resilience.md`(本 ADR)ごと見直す。
- `uncaughtException`/`unhandledRejection` はあくまで最終防衛であり、恒久的な修正の代替ではない。将来的に `@platformatic/kafka` のバージョンを上げる際は、この特定のバグ(`kafkaError.findBy` の `TypeError`)が解消されているか確認し、解消されていれば本 ADR とハンドラの必要性を再評価する。
- 実装本体は `apps/gateway/src/server.ts` の `process.on("uncaughtException", ...)` 呼び出し箇所のコメントを参照(本 ADR はその設計判断を独立した文書として記録したもの)。
