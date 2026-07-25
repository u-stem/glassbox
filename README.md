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

設計判断の詳細は [`docs/adr/`](docs/adr/)(クライアント選定・SSE 設計・状態権威モデル・プロセス耐障害性)、Kafka テーマ固有の学習内容・既知の限界は [`docs/themes/kafka.md`](docs/themes/kafka.md) を参照。

## セットアップ

前提: Node `>=24.6.0`、[bun](https://bun.com/)、Docker。

```bash
# 1. 依存関係のインストール(bun workspaces: apps/*, packages/*)
bun install

# 2. Kafka ブローカーを起動(KRaft シングルノード、ポート 9092)
docker compose -f docker/compose.yaml up -d

# 3. gateway を起動(Fastify、既定ポート 4000)
bun --cwd apps/gateway run dev

# 4. web を起動(Next.js、既定ポート 3000)
bun --cwd apps/web run dev
```

ブラウザで `http://localhost:3000/themes/kafka` を開く。

### 環境変数(いずれも既定値あり、`apps/gateway/src/env.ts` 参照)

| 変数 | 既定値 | 用途 |
|---|---|---|
| `PORT` | `4000` | gateway の待受ポート |
| `KAFKA_BROKERS` | `localhost:9092` | ブローカーの bootstrap アドレス |
| `EVENT_BUFFER_CAPACITY` | `1000` | SSE リングバッファの保持件数 |
| `ADMIN_POLL_INTERVAL_MS` | `1000` | admin snapshot のポーリング間隔 |
| `WEB_ORIGIN` | `http://localhost:3000` | CORS で許可する web の origin |

web 側は `NEXT_PUBLIC_GATEWAY_URL`(既定 `http://localhost:4000`)で gateway の URL を指定する。

## 開発コマンド

```bash
bun test              # 単体テスト(全ワークスペース)
bun run typecheck     # tsc --noEmit(全ワークスペース)
bun run lint          # biome check
bun --cwd apps/gateway run test:integration  # 実 broker に対する統合テスト
bun --cwd apps/web run test:e2e              # Playwright E2E スモーク(要 gateway/web 起動中)
```

## ドキュメント

- [`docs/adr/`](docs/adr/): 設計判断(ADR)
  - [0001: Kafka クライアント選定](docs/adr/0001-kafka-client-selection.md)
  - [0002: SSE リアルタイム配信](docs/adr/0002-sse-realtime-transport.md)
  - [0003: 状態の権威モデル](docs/adr/0003-state-authority-model.md)
  - [0004: gateway プロセスの耐障害性](docs/adr/0004-process-resilience.md)
- [`docs/spikes/`](docs/spikes/): 使い捨て spike の記録
- [`docs/themes/kafka.md`](docs/themes/kafka.md): Kafka テーマの学習内容・4 パネルの読み方・レッスン・既知の限界
