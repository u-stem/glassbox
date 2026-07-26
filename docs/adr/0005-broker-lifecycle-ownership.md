# 0005: ブローカーのライフサイクルを gateway が所有する

## Status

Accepted

## Context

Kafka ブローカーの起動は CLI(`docker compose -f docker/compose.yaml up -d`)でしか行えず、ホーム画面はブローカー未起動時にそのコマンドを表示して手入力を促すだけだった。学習プラットフォームとしては、ブローカーを落として何が起きるかを観察する操作自体が教材になるため、この操作を UI から行えるようにしたい。

しかし単にボタンを足すだけでは目的を達成できず、その過程で既存の設計上の問題が 2 つ表面化した。

### 問題 1: gateway が Kafka 不在では起動できない

`server.ts` の `start()` は `app.listen()` より前に `ensureDemoTopic()` を `await` しており、失敗すれば `process.exit(1)` していた。つまり Kafka が落ちている状態では gateway 自体が起動できない。

この制約下では「Kafka だけが落ちている」状態は *gateway 起動後に Kafka が落ちた場合* にしか到達せず、ホーム画面が `kafka-down` を表示する余地はほとんどない(通常は `gateway-down` になる)。ここに Kafka 起動ボタンだけを足しても、gateway は結局 CLI で起動することになり、「CLI を使わずに立ち上げる」という目的は満たせない。

### 問題 2: トピックが消えると gateway が生きていても復活しない

`docker/compose.yaml` は named volume を宣言しておらず(ブローカーのデータは匿名ボリュームにある)、`KAFKA_AUTO_CREATE_TOPICS_ENABLE` も `"false"` である。一方 `ensureDemoTopic()` は起動時 1 回しか呼ばれない。

したがって `up -d` がコンテナを再作成すると `glassbox.demo` トピックが消え、gateway プロセスは生きているのにダッシュボードは永久に空のまま、produce も失敗し続ける。原因は利用者からは全く見えない。ブローカーの起動停止を一級の UI 操作にするなら、押しやすくなるこの経路を先に塞ぐ必要がある。

## Decision

### 1. gateway の起動を Kafka 非依存にする(トピック作成の reconcile 化)

`start()` から `ensureDemoTopic()` の呼び出しを外し、admin-poller が「ブローカーへ到達できた」ことを検出した時点で実行する。admin-poller には `onReachable` フックを追加し、到達性が `connected` へ **遷移した瞬間だけ** 呼ぶ(`connected` が続く間は呼ばない)。

`ensureDemoTopic()` は `listTopics()` で存在確認してから作るので既に冪等であり、reconcile として繰り返し呼んで安全である。これにより:

- Kafka 不在でも gateway は起動して `listen` する
- 停止 → 再開でも、コンテナ再作成でトピックが消えた場合でも、`unreachable` → `connected` の遷移でトピックが再作成される

トピック作成というドメイン知識は admin-poller に持ち込まず、poller はあくまで「到達性の遷移を通知する」責務に留める。

### 2. ブローカー制御 API は gateway に置く

`GET` / `POST /api/themes/kafka/broker` を gateway に追加し、`docker compose` の実行主体を gateway とする。web の Next.js Route Handler に置く案も検討したが、次の理由で採らなかった。

- gateway は既に「常駐して副作用を所有する」プロセスであり(README のアーキテクチャ概略)、web はブラウザに描画する層である。docker の実行は前者の責務に属する。
- gateway には `HOST` 既定 `127.0.0.1`(ADR 外の設計判断だが `env.ts` に明記)と `@fastify/cors` の `WEB_ORIGIN` 限定という、ループバック限定・同一オリジン限定のガードが既にある。既存のシナリオ実行 API と同じ脅威モデルで守られるため、新しいプロセス境界とその周辺のガードを web 側に作り直さずに済む。
- 「gateway が落ちていても Kafka を起動できるように web へ置く」という動機は、決定 1 によって消滅する。gateway が Kafka の不在で落ちることがなくなるため。

### 3. 「停止」は `docker compose stop`。`down` と named volume は採らない

- 停止は `stop`(コンテナ保持)。`down` はコンテナごと削除するためトピック再作成が入って起動が遅く、学習の途中経過も消える。`stop`/`start` なら同じブローカーに戻るので gateway 側クライアントの再接続も効きやすい。
- 起動は `up -d --no-recreate`。`docker compose start` は未作成のコンテナに対して失敗するため、「未作成」と「作成済み・停止中」を 1 コマンドで扱うには `up` が必要である。`--no-recreate` は、データを持つ匿名ボリュームごとコンテナが作り直されるのを明示的に禁じる。
- `--wait` は付けない。healthcheck の `start_period` が 20 秒あるためレスポンスが長時間ブロックし、「起動中 → 稼働中」の遷移を UI で見せられなくなる。収束はクライアント側のポーリングに任せる。
- `-p`(プロジェクト名)は渡さない。compose はプロジェクト名を compose ファイルの親ディレクトリから導出しており、`-f <絶対パス>` だけを渡せば cwd に関係なく README の CLI 手順と同じプロジェクトを操作できる。`-p` を付けると UI と CLI が別々の Kafka を見ることになる。
- named volume によるデータ永続化は行わない。決定 1 によりトピックは自動で再作成されるため、コンテナが作り直されても壊れた状態は残らない。オフセット履歴は失われるが、学習ツールとしてはクリーンな状態から始まるほうが都合がよい。

### 4. `docker` は shell を介さず固定引数配列で実行する

`execFile` を `shell: false` で使い、引数は組み立て済みの固定配列のみを渡す。利用者の入力が引数に流れ込む経路を作らない。この決定はテストでも担保する(生成される引数に `down` / `-v` / `--volumes` / `-p` が現れないことを assert する)。

## Consequences

- gateway・web・Kafka の起動順序の制約が消え、`bun run dev` で gateway と web を上げてからブラウザのボタンで Kafka を起動する流れが成立する。
- `/healthz` の `kafka` フィールドの値域は変わらないが、意味の重心が変わる: 「gateway が起動している」ことと「Kafka に繋がっている」ことが常に別事実になる。web 側の `toEnvironmentStatus` は既に 3 状態を扱っているためロジック変更は不要。
- 環境の状態源が 2 つ(gateway の `/healthz` と docker の `compose ps`)になる。両者は正当に食い違う(例: コンテナは `running` だが healthcheck 待ちで gateway はまだ繋がれない)。ADR-0003 が確立した「フィールドごとに権威を明文化する」規律をここにも適用し、UI では 1 つのラベルに丸めず「gateway 接続」と「ブローカー(コンテナ)」を別の行として併記する。
- ブローカー停止時、healthz 側のヒステリシス(2 連続失敗、5 秒間隔)により最大 10 秒ほど gateway 行が「接続 OK」のまま残る。ラベルが分離されていれば「gateway がまだ接続断を検知していない」という事実の正確な表示であり、ヒステリシスは変更しない。
- `docker` が使えない環境(未インストール・デーモン停止)では、ボタンを出さず従来のコマンド案内へ degrade する。この経路は壊さない。
- ブローカー停止 → 再開後、consumer の一部は消費ストリームが reject したまま復帰しない(実測)。復旧は手動(consumer を削除して追加し直す)であり、自動化は**試したうえで見送った**。`onConsumeError` を受けて consumer を自動撤去する実装を入れたところ、同じ reject が通常のリバランス中にも `TimeoutError: Request timed out` として発生し(レッスン B の E2E で毎回再現)、追加したばかりの健全な consumer が消えるようになった。「消費していない member が残る」より「操作した直後に consumer が消える」ほうが体験として悪いため、レジストリはエラーを記録するだけに留めている。実測の詳細は [`docs/themes/kafka.md`](../themes/kafka.md) の「既知の限界」に記録した。
- したがってブローカーの停止/再開は、consumer を観察している最中に行うと手動の作り直しが必要になる。producer と admin poller については自動で復帰するため、パーティションとオフセットの観察に限れば停止ボタンはそのまま使える。
