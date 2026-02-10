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
