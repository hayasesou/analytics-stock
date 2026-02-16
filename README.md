# analytics-stock

株式分析・提案・対話型リサーチツール（要件 v1.1 凍結版）の実装ベースラインです。

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

3. 常駐起動（Web + Worker Scheduler + Executor + Agents）

```bash
docker compose up -d web worker executor agents
```

4. 手動ジョブ実行（必要時）

```bash
docker compose --profile jobs run --rm worker-daily
docker compose --profile jobs run --rm worker-weekly
docker compose --profile jobs run --rm worker-research
```

5. Deep Research レポート取込（任意）

```bash
# 例: GUIの deep research レポートを保存したテキストを取り込む
export DEEP_RESEARCH_REPORT_PATH=/app/source/deep_research_3513.txt
export DEEP_RESEARCH_SECURITY_ID=JP:3513
docker compose --profile jobs run --rm worker-research
```

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
  - A/B/C ファンダ判断 (`fundamental_snapshots`) を作成
  - 5つの自律エージェントタスク (`agent_tasks`) を起票
- `agents`:
  - キューを並列処理し、戦略設計/コード/特徴量/リスク評価/オーケストレーション結果を記録
- `executor`:
  - 承認済み `order_intents` のみ実行
  - DD 3% / rolling SR(20d) のリスクゲート
  - A/B/C を使った最終発注ゲート（C除外、B縮小、A通常）

### LLM テスト

```bash
cd worker
pytest -q -k "llm_reporting_unit or llm_weekly_summary_unit or llm_reporting_golden or llm_weekly_summary_golden or openai_client"
RUN_LLM_LIVE=1 OPENAI_API_KEY=... OPENAI_MODEL=gpt-5-mini pytest -q -m llm_live
```

## PR マージ運用（CI通過後）

- `ci` workflow が PR ごとに `worker-tests` と `web-build` を実行します。
- 本リポジトリは GitHub プラン制約で branch protection の必須チェックを強制できないため、マージ時は以下スクリプトを使ってください。

```bash
bash scripts/merge_after_ci.sh <PR番号> --merge
```

- スクリプトは `worker-tests` / `web-build` が `success` になるまで待機し、失敗ならマージせず終了します。
