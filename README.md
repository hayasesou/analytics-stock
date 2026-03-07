# analytics-stock

株式分析・提案・対話型リサーチツール（要件 v1.1 凍結版）の実装ベースラインです。

## はじめて触る方向け（先にここだけ読めばOK）

このプロジェクトは、次の3つを1つの画面群で扱います。

- 研究: 戦略アイデアを作る・検証する
- 監視: いま「チャンス（エッジ）」があるかを見る
- 執行: 注文の状態やリスク停止を監視する

まずは「取引はせず、画面を確認するだけ」で始めるのが安全です。

## 初心者向けクイックスタート（Docker版）

### 0. 事前準備

- Docker Desktop をインストールして起動しておく
- このリポジトリをローカルに用意する
- ターミナルでこのディレクトリに移動する

### 1. 設定ファイルを作る

```bash
cp .env.example .env
```

最低限、Web のログイン用に `.env` の以下を確認してください。

- `BASIC_AUTH_USER`
- `BASIC_AUTH_PASS`

### 2. DBスキーマを適用する

```bash
docker compose --profile setup run --rm db-bootstrap
```

### 3. 画面とバックエンドを起動する

```bash
docker compose up -d web worker executor agents research-chat gateway-crypto gateway-jp gateway-us
```

Discord連携も使う場合だけ、`DISCORD_BOT_TOKEN` を `.env` に設定してから次を追加します。

```bash
docker compose up -d discord-listener discord-research-listener
```

### 4. ブラウザで開く

- URL: `http://localhost:3000`
- Basic認証: `.env` の `BASIC_AUTH_USER` / `BASIC_AUTH_PASS`

### 5. 止める

```bash
docker compose down
```

## 画面の見方（最初に見る3ページ）

- `/edge`（Edge監視）
  - 何がどれだけ「有利な機会」かを一覧で確認する画面です。
  - `Edge Score` が高く、`Net Edge` がプラスのものを優先的に見ます。
- `/execution`（執行監視）
  - 注文が `proposed/approved/done/failed` など、どの状態かを確認する画面です。
  - `halted` が出ていれば、リスク条件で停止中です。
- `/research`（研究管理）
  - 戦略の進捗（new → analyzing → candidate → paper → live）を確認します。
  - `Paperへ` や `Live承認` の手動操作ができます。

## 画面別READMEへのリンク

ルートREADMEは全体概要と実行手順のみを持ち、各画面の詳細は以下に分離しています。

- 画面別READMEインデックス: `docs/screens/README.md`
- `/top50`: `docs/screens/top50.md`
- `/reports/weekly`: `docs/screens/weekly-summary.md`
- `/reports/[securityId]`: `docs/screens/report-detail.md`
- `/backtest`: `docs/screens/backtest.md`
- `/edge`: `docs/screens/edge.md`
- `/execution`: `docs/screens/execution.md`
- `/research`: `docs/screens/research.md`
- `/events`: `docs/screens/events.md`
- `/chat`: `docs/screens/chat.md`

## 構成

- `worker/`: Lightsail 常駐の Python Worker（収集・解析・レポート・通知）
- `web/`: Vercel 配備の Next.js（Top50/詳細/バックテスト/イベント/チャット）
- `infra/sql/schema.sql`: Neon Postgres スキーマ
- `config.yaml`: 凍結要件を反映した設定
- `docs/checklist.md`: 付録A 実行順チェックリスト

## 最短起動

1. Neon に `infra/sql/schema.sql` を適用
2. `cp .env.example .env` を実行し、値を設定
3. Worker:

```bash
cd worker
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
python -m src.main weekly
python -m src.main daily
```

4. Web:

```bash
cd web
npm install
npm run dev
```

## Docker 実行（web + worker 完全コンテナ）

1. `.env` を作成して値を設定

```bash
cp .env.example .env
```

2. スキーマ適用（Neon）

```bash
# 推奨: docker compose 経由 (psql のローカル導入不要)
docker compose --profile setup run --rm db-bootstrap

# 代替: ラッパースクリプト（psql があればローカル実行、なければ compose にフォールバック）
bash scripts/bootstrap.sh
```

3. 常駐起動（Web + Worker Scheduler + Executor + Agents + Discord Listener + Crypto/JP/US Gateway）

```bash
docker compose up -d web worker executor agents research-chat discord-listener discord-research-listener gateway-crypto gateway-jp gateway-us
```

4. 手動ジョブ実行（必要時）

```bash
docker compose --profile jobs run --rm worker-daily
docker compose --profile jobs run --rm worker-weekly
docker compose --profile jobs run --rm worker-research
docker compose --profile jobs run --rm worker-research-chat
# YouTube URL 直接投入（URL指定）
docker compose --profile jobs run --rm worker python -m src.main ingest_youtube --url "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
# Discordコマンド文字列をそのまま投入
docker compose --profile jobs run --rm worker python -m src.main ingest_youtube --command "/ingest_youtube https://youtu.be/dQw4w9WgXcQ"
# OpenClaw PoC 評価メモ生成（docs/openclaw-evaluation.md）
docker compose --profile jobs run --rm worker python -m src.main openclaw_eval
```

`discord-listener` を起動している場合、Discord の `#inbox` に YouTube URL を貼るだけで自動 ingest されます。

`discord-research-listener` を起動している場合、Bot へのメンション付きで自然文や URL を送ると Research Chat session が作成され、`research-chat` worker が後続 task を処理します。

5. Deep Research レポート取込（任意）

```bash
# 例: GUIの deep research レポートを保存したテキストを取り込む
export DEEP_RESEARCH_REPORT_PATH=/app/source/deep_research_3513.txt
export DEEP_RESEARCH_SECURITY_ID=JP:3513
docker compose --profile jobs run --rm worker-research
```

### Crypto Gateway（鍵分離）

- `gateway-crypto` が Binance/Hyperliquid の注文実行を担当します。
- `executor` は `CRYPTO_GATEWAY_URL` / `CRYPTO_GATEWAY_AUTH_TOKEN` のみ使用し、取引鍵は参照しません。
- `docker-compose.yml` で worker系コンテナには `GATEWAY_BINANCE_API_KEY` / `GATEWAY_BINANCE_API_SECRET` を空で上書きしています。

### JP Gateway（kabu station 分離）

- `gateway-jp` が kabu station 向け注文実行を担当します（レート制限 + 差分発注）。
- `executor` は `JP_GATEWAY_URL` / `JP_GATEWAY_AUTH_TOKEN` でのみ接続します。
- `docker-compose.yml` で worker系コンテナには `KABU_STATION_API_PASSWORD` / `KABU_STATION_API_TOKEN` を空で上書きしています。

### US Gateway（IBKR 分離）

- `gateway-us` が IBKR 向け注文実行を担当します（接続再試行 + 注文状態再同期）。
- `executor` は `US_GATEWAY_URL` / `US_GATEWAY_AUTH_TOKEN` でのみ接続します。
- `docker-compose.yml` で worker系コンテナには `IBKR_ACCOUNT_ID` を空で上書きしています。

## バッチスケジュール（JST）

- 日次: 20:00 (`jobs.daily`)
- 週次: 土曜 06:30 (`jobs.weekly`)

`scripts/` に cron 用ラッパーを配置しています。

## 銘柄マスタ（実企業名）

- `worker` は銘柄マスタを live 優先で取得します。
  - JP: J-Quants V2 `equities/master`（`JQUANTS_API_KEY` 必須。未設定時のみ `JQUANTS_EMAIL` / `JQUANTS_PASSWORD` の旧認証にフォールバック）
  - US: Massive `reference/tickers`（`MASSIVE_API_KEY` 任意）または SEC `company_tickers_exchange.json`
- live 取得に失敗した場合は従来の mock ユニバースにフォールバックします。
- `security_id` 形式:
  - JP: `JP:####`（実在4桁コード）
  - US: `US:<ticker>`（例: `US:AAPL`）
  - 互換性のため旧 `US:###` 形式も API では許容しています。

## 日次イベント（live 優先）

- `daily` ジョブのイベントは live 優先で取得します。
  - SEC: `getcurrent` Atom フィード（`SEC_USER_AGENT` 必須）
  - EDINET: documents list API（`EDINET_API_KEY` 任意）
- live で1件も取得できない場合はイベントを0件として扱います（mock イベントは生成しません）。

## 価格データ品質ゲート（実運用向け）

- `weekly`:
  - `data_quality.weekly` で市場ごとの最低カバレッジ比率を強制します。
  - 例: `JP/US` の 14日内カバレッジが閾値未満なら `weekly` を `failed` にします。
- `executor`:
  - `execution.data_quality` で発注時の価格鮮度をチェックします。
  - `reject_on_missing_price: true` かつ stale の場合、対象ポジションは自動除外され、意図注文は `rejected` になります。

## LLM 生成（任意）

- 週次の `security_full` レポート本文生成を OpenAI で有効化できます。
- 週次の `weekly_summary` 生成も OpenAI で有効化できます。
- 既定モデルは `gpt-5-mini`（`OPENAI_MODEL`）です。
- 有効化フラグ:
  - `LLM_SECURITY_REPORTS_ENABLED=1`
  - `LLM_WEEKLY_SUMMARY_ENABLED=1`
- 研究ループで Deep Research 構造化を使う場合:
  - `DEEP_RESEARCH_REPORT_PATH`
  - `DEEP_RESEARCH_SECURITY_ID`
- YouTube URL起点の idea ingest を使う場合:
  - `YOUTUBE_API_KEY`
  - `DISCORD_BOT_TOKEN`（Discord受信を使う場合）
  - `config.yaml` の `discord_ingest.inbox_channel_name`（既定: `inbox`）
- 安全装置（任意・既定値あり）:
  - `LLM_SECURITY_REPORT_MAX_CALLS=20`
  - `LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES=3`
  - `LLM_SECURITY_REPORT_BUDGET_SEC=180`
  - `LLM_SECURITY_REPORT_TIMEOUT_SEC=12`
  - `LLM_WEEKLY_SUMMARY_TIMEOUT_SEC=12`
- 未設定/0 の場合は既存テンプレ生成です。

## LV4 研究ループ / 実行ループ

- `worker-research`:
  - 最新 weekly 候補から戦略候補 (`strategies`, `strategy_versions`, `strategy_evaluations`) を生成
  - `strategy_factory.validation` の固定ルールで Walk-forward 検証を実行し、`robust_backtest` として保存
  - A/B/C ファンダ判断 (`fundamental_snapshots`) を作成
  - 5つの自律エージェントタスク (`agent_tasks`) を起票
- `agents`:
  - キューを並列処理し、戦略設計/コード/特徴量/リスク評価/オーケストレーション結果を記録
  - `agents.adapter.mode=openclaw_poc` で `strategy_design` のみ OpenClaw PoC アダプタで処理可能
  - `openclaw_eval` ジョブで builtin と OpenClaw PoC を比較し、Go/No-Go 判定を `docs/openclaw-evaluation.md` に保存
- `executor`:
  - 承認済み `order_intents` のみ実行
  - DD 3% / rolling SR(20d) のリスクゲート
  - `execution.strategy_risk_gate` による戦略単位 warning/halt/cooldown と panic close
  - A/B/C を使った最終発注ゲート（C除外、B縮小、A通常）
  - `execution.order_reconcile` で target と現在ポジション/未約定注文の差分注文のみ発行

### 戦略検証ゲート（固定ルール + 動的評価）

- ルールは `config.yaml` の `strategy_factory.validation` で固定:
  - `train_days / test_days / step_days`
  - `momentum_quantile / max_volatility_20d`
  - `gates`（Sharpe・DD・取引数など）
- 評価は動的:
  - 最新価格履歴を使った Walk-forward を `worker-research` 実行時に再計算
  - `strategy_evaluations.eval_type = robust_backtest` に保存
- 候補戦略は「ファンダ判定 + 検証ゲート」の両方を通過したもののみ `candidate` 扱いになります。

### 戦略検証ゲート（固定ルール + 動的評価）

- ルールは `config.yaml` の `strategy_factory.validation` で固定:
  - `train_days / test_days / step_days`
  - `momentum_quantile / max_volatility_20d`
  - `gates`（Sharpe・DD・取引数など）
- 評価は動的:
  - 最新価格履歴を使った Walk-forward を `worker-research` 実行時に再計算
  - `strategy_evaluations.eval_type = robust_backtest` に保存
- 候補戦略は「ファンダ判定 + 検証ゲート」の両方を通過したもののみ `candidate` 扱いになります。

### LLM テスト

```bash
cd worker
pytest -q -k "llm_reporting_unit or llm_weekly_summary_unit or llm_reporting_golden or llm_weekly_summary_golden or openai_client"
RUN_LLM_LIVE=1 OPENAI_API_KEY=... OPENAI_MODEL=gpt-5-mini pytest -q -m llm_live
```

## 用語集（初心者向け）

- Edge（エッジ）
  - 売買コストを引いたあとでも利益が期待できる「優位性」です。
- Edge Score
  - エッジを 0〜100 で見やすくした点数です。高いほど有利です。
- Net Edge
  - 手数料やスリッページ（想定より不利な約定）を引いた後の期待値です。
- アービトラージ（Arbitrage）
  - 市場間の価格差など、歪みを狙う手法です。方向予想より中立運用しやすいのが特徴です。
- Δ中立（デルタ中立）
  - 相場全体が上がる/下がる影響をできるだけ打ち消す建て方です。
- Gateway
  - 実際の発注だけを担当する専用サービスです。鍵（APIキー）を分離して安全性を上げます。
- Paper運用
  - 本番資金を使わず、実運用と同じ条件で検証する段階です。
- Live運用
  - 実資金で運用する段階です。このプロジェクトでは手動承認で移行できます。
- DD（ドローダウン）
  - 直近ピークからどれだけ下がったかの指標です。損失管理に使います。
- Sharpe（シャープレシオ）
  - リスクに対してどれだけ効率よく利益を出せたかの指標です。

## よくあるエラーと対処

- `web` が再起動を繰り返す
  - まず `docker compose logs --tail=100 web` を確認してください。
  - イメージが古い可能性があるため、`docker compose up -d --build --force-recreate web` を実行します。
- `discord-listener` が再起動を繰り返す
  - `.env` に `DISCORD_BOT_TOKEN` が未設定だと起動できません。
  - Discordを使わない場合は `discord-listener` を起動対象から外してください。
- `401 Unauthorized` が出る
  - Basic認証です。`.env` の `BASIC_AUTH_USER` / `BASIC_AUTH_PASS` でログインしてください。

## PR マージ運用（CI通過後）

- `ci` workflow が PR ごとに `worker-tests` と `web-build` を実行します。
- 本リポジトリは GitHub プラン制約で branch protection の必須チェックを強制できないため、マージ時は以下スクリプトを使ってください。

```bash
bash scripts/merge_after_ci.sh <PR番号> --merge
```

- スクリプトは `worker-tests` / `web-build` が `success` になるまで待機し、失敗ならマージせず終了します。
