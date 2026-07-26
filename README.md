# glassbox

ミドルウェア・プロトコルを実際に動かし、「今どの状態からどの状態に進んでいるか」をリアルタイムに可視化する学習プラットフォーム。名前の glassbox は「ブラックボックスの中身を見せる」の意。

本物のミドルウェアを Docker Compose で動かし、自作の計装クライアント層(gateway)で全イベントを捕捉して可視化する、ハイブリッド方式を採る。最初のテーマは Kafka(produce/consume、パーティション、consumer group リバランス、オフセット、ラグ)。

## アーキテクチャ概略

```
[ブラウザ (Next.js, apps/web)]
        │ SSE (/api/events) ←→ POST (/api/themes/kafka/scenarios/:id)
        ▼
[gateway (Fastify, apps/gateway, Node 実行)]
        │ 計装クライアント(@platformatic/kafka)+ diagnostics_channel + admin poller
        ▼
[Apache Kafka (docker/compose.yaml, KRaft シングルノード)]
```

- `apps/web`: Next.js(App Router)。SSE で受け取ったイベントを Zod で parse し、純関数の reducer(`world-reducer.ts`)で状態を組み立て、zustand ストアに反映してパネル群を描画する。
- `apps/gateway`: 常駐 Fastify プロセス。producer/consumer/admin クライアントを保持し、クライアントイベントと `diagnostics_channel` を購読して正規化したイベントを SSE で配信する。シナリオ実行 API(`POST /api/themes/kafka/scenarios/:id`)もここが持つ。
- `packages/schema`: web/gateway が共有する Zod スキーマ(`GlassboxEvent` エンベロープ + Kafka イベント union)。
- gateway が別プロセスなのは、常駐する consumer/producer が Next.js の route handler のライフサイクル(HMR での破棄)と相容れず、`diagnostics_channel` の購読がプロセスグローバルなため(詳細は各 ADR を参照)。

設計判断の詳細は [`docs/adr/`](docs/adr/)(クライアント選定・SSE 設計・状態権威モデル・プロセス耐障害性・ブローカーのライフサイクル所有)、Kafka テーマ固有の学習内容・既知の限界は [`docs/themes/kafka.md`](docs/themes/kafka.md) を参照。

## セットアップ

前提: Node `>=24.6.0`、[bun](https://bun.com/)、Docker。

```bash
# 1. 依存関係のインストール(bun workspaces: apps/*, packages/*)
bun install

# 2. gateway(Fastify、既定ポート 4000)と web(Next.js、既定ポート 3000)を並列起動
bun run dev
```

ブラウザで `http://localhost:3000` を開き、Kafka カードの「起動」ボタンでブローカーを起動する。初回はイメージの取得とコンテナ作成が走るため数分かかることがある。起動後、「ダッシュボードを開く」から `/themes/kafka` に進む。

gateway は Kafka が起動していなくても立ち上がり、ブローカーに到達できた時点でデモトピックを作成する([ADR-0005](docs/adr/0005-broker-lifecycle-ownership.md))。したがって起動順序を気にする必要はない。

個別に起動したい場合、または UI を使わずブローカーを操作したい場合は次のコマンドを使う。

```bash
docker compose -f docker/compose.yaml up -d   # ブローカーの起動
docker compose -f docker/compose.yaml stop    # ブローカーの停止
bun run --cwd apps/gateway dev                # gateway だけを起動
bun run --cwd apps/web dev                    # web だけを起動
```

`bun run dev` は両プロセスに同じ環境変数を渡すため、`PORT` を指定すると gateway と web の両方がそのポートを使おうとする。ポートを変えたいときは個別起動を使う。

### 環境変数(いずれも既定値あり、`apps/gateway/src/env.ts` 参照)

| 変数 | 既定値 | 用途 |
|---|---|---|
| `PORT` | `4000` | gateway の待受ポート |
| `KAFKA_BROKERS` | `localhost:9092` | ブローカーの bootstrap アドレス |
| `EVENT_BUFFER_CAPACITY` | `1000` | SSE リングバッファの保持件数 |
| `ADMIN_POLL_INTERVAL_MS` | `1000` | admin snapshot のポーリング間隔 |
| `WEB_ORIGIN` | `http://localhost:3000` | CORS で許可する web の origin |
| `GLASSBOX_COMPOSE_FILE` | リポジトリ同梱の `docker/compose.yaml` | ブローカー制御 API が操作する compose ファイル |

web 側は `NEXT_PUBLIC_GATEWAY_URL`(既定 `http://localhost:4000`)で gateway の URL を指定する。

### 状態の確認とブローカー制御 API

gateway の `GET /healthz` は `{ ok: true, kafka: "connected" | "unreachable" | "unknown" }` を返す。これは「gateway がいまブローカーと話せているか」であり、コンテナが動いているかとは別の問いである(コンテナが up でもヘルスチェック通過前は接続できない)。

コンテナ側の状態と操作は `/api/themes/kafka/broker` が担う。

| メソッド | 内容 |
|---|---|
| `GET` | `{ broker: { kind: "running" \| "starting" \| "stopped" \| "absent" \| "unavailable" } }` を返す |
| `POST` | `{ "action": "start" \| "stop" }` を受け取り、実行後に観測した状態を返す |

ホーム画面(`/`)はこの 2 つをポーリングし、「Gateway 接続」と「ブローカー(コンテナ)」を別々の行として表示したうえで、起動/停止ボタンを出す。停止は `docker compose stop` であり、コンテナもデータも残る(`down` は行わない)。実行中に再度 `POST` すると 409 を返し、`docker compose up` が多重に走ることはない。

このエンドポイントは gateway の他の API と同じくループバック(`HOST` 既定 `127.0.0.1`)と `WEB_ORIGIN` からのみ到達できる。

## 開発コマンド

```bash
bun test              # 単体テスト(全ワークスペース)
bun run typecheck     # tsc --noEmit(全ワークスペース)
bun run lint          # biome check
bun run --cwd apps/gateway test:integration  # 実 broker に対する統合テスト
bun run --cwd apps/web test:e2e              # Playwright E2E スモーク(要 gateway/web 起動中)
```

## 配色トークン

配色は [`apps/web/src/app/globals.css`](apps/web/src/app/globals.css) の `.viz-root` に CSS カスタムプロパティとして集約している。UI は必ずトークン参照(`bg-(--surface-1)` や `var(--text-primary)`)で書き、素の Tailwind パレット色(`bg-blue-600` など)は使わない。

- **新しいページを作るときは、そのページの `<main>` に `viz-root` を付ける。** トークンは `.viz-root` の内側でしか定義されないため、付け忘れるとそのページだけライト固定になる。地色(`bg-(--page-plane)`)は幅を絞る前の全幅要素に置き、内側の `div` で `max-w-*` を掛ける。
- `--series-1`〜`--series-8` はデータ(パーティション)の色で、`partitionColorVar()` 経由でのみ使う。UI の chrome に流用しない。
- `--status-*` はマーク・枠線・透過した帯に使い、文字色には使わない(`--status-warning` はライト面に対して 1.74:1)。エラーは赤い文字ではなく `--status-critical` のドット + `--text-primary` の本文で示す。
- `--text-muted` も文字色に使わない(ライト面で 3.5:1)。本文・補助テキストは `--text-secondary`。`--text-muted` は操作できる要素(ボタン・入力欄)の枠線に使い、静的な面の枠線は `--border`。
- ボタンは「塗り(`--accent` / `--on-accent`)= 主アクション、枠線 = 副アクション」で階層を表す。色で機能を区別しない。
- **面の上に重ねる面**(ポップオーバー等)の枠線は `--border` ではなく `--axis` を使う。サーフェスは `--surface-1` / `--page-plane` の 2 段しかなく、`--surface-1` のカードの上に `--surface-1` を重ねると alpha 0.1 の `--border` では輪郭が消えるため(トポロジーのノードが同じ理由で `--axis` を使っている)。
- **z 階層は 3 つだけ**: `10` = データツールチップ(パーティション盤面)、`40` = 共通ヘッダー、`50` = 用語ポップオーバー。サイドドロワーはフロー内に置いてあるので z-index を持たない。`position: fixed` のポップオーバーが正しく置かれるよう、パネルやドロワーに `transform` / `filter` / `will-change` / `contain` を付けない(`sticky` は包含ブロックを作らないので可)。
- ライト/ダークは `<html>` の `data-theme`(`light` / `dark`、未設定なら OS 設定)で切り替わる。`globals.css` は既定・OS ダーク・強制ダークの 3 経路を宣言しており、スクロールバー用に `:root` へも `color-scheme` を置いている。

## ドキュメント

- [`docs/adr/`](docs/adr/): 設計判断(ADR)
  - [0001: Kafka クライアント選定](docs/adr/0001-kafka-client-selection.md)
  - [0002: SSE リアルタイム配信](docs/adr/0002-sse-realtime-transport.md)
  - [0003: 状態の権威モデル](docs/adr/0003-state-authority-model.md)
  - [0004: gateway プロセスの耐障害性](docs/adr/0004-process-resilience.md)
  - [0005: ブローカーのライフサイクル所有](docs/adr/0005-broker-lifecycle-ownership.md)
- [`docs/spikes/`](docs/spikes/): 使い捨て spike の記録
- [`docs/themes/kafka.md`](docs/themes/kafka.md): Kafka テーマの学習内容・4 パネルの読み方・レッスン・用語集・既知の限界
