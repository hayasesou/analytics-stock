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

3. 常駐起動（Web + Worker Scheduler）

```bash
docker compose up -d web worker
```

4. 手動ジョブ実行（必要時）

```bash
docker compose --profile jobs run --rm worker-daily
docker compose --profile jobs run --rm worker-weekly
```

## バッチスケジュール（JST）

- 日次: 20:00 (`jobs.daily`)
- 週次: 土曜 06:30 (`jobs.weekly`)

`scripts/` に cron 用ラッパーを配置しています。

## LLM 生成（任意）

- 週次の `security_full` レポート本文生成を OpenAI で有効化できます。
- 週次の `weekly_summary` 生成も OpenAI で有効化できます。
- 既定モデルは `gpt-5-mini`（`OPENAI_MODEL`）です。
- 有効化フラグ:
  - `LLM_SECURITY_REPORTS_ENABLED=1`
  - `LLM_WEEKLY_SUMMARY_ENABLED=1`
- 安全装置（任意・既定値あり）:
  - `LLM_SECURITY_REPORT_MAX_CALLS=20`
  - `LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES=3`
  - `LLM_SECURITY_REPORT_BUDGET_SEC=180`
  - `LLM_SECURITY_REPORT_TIMEOUT_SEC=12`
  - `LLM_WEEKLY_SUMMARY_TIMEOUT_SEC=12`
- 未設定/0 の場合は既存テンプレ生成です。

### LLM テスト

```bash
cd worker
pytest -q -k "llm_reporting_unit or llm_weekly_summary_unit or llm_reporting_golden or llm_weekly_summary_golden or openai_client"
RUN_LLM_LIVE=1 OPENAI_API_KEY=... OPENAI_MODEL=gpt-5-mini pytest -q -m llm_live
```
