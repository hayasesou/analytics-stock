# 付録A 実行順チェックリスト

## 1) 通知を先に通す

- [ ] Discord に株分析用チャンネルを作成
- [ ] Discord Webhook URL を発行して保存 (`DISCORD_WEBHOOK_URL`)

## 2) LLM キー

- [ ] OpenAI API キーを発行 (`OPENAI_API_KEY`)

## 3) クラウド永続（Neon/R2）

- [ ] Neon プロジェクト作成
- [ ] `NEON_DATABASE_URL` を控える
- [ ] 拡張を有効化 (`vector`, `pgcrypto`)
- [ ] `infra/sql/schema.sql` を投入
- [ ] R2 バケット作成 (`stock-analysis-evidence`, `stock-analysis-data`)
- [ ] R2 S3 互換キーを発行し保存
  - [ ] `R2_ACCOUNT_ID`
  - [ ] `R2_ACCESS_KEY_ID`
  - [ ] `R2_SECRET_ACCESS_KEY`
  - [ ] `R2_BUCKET_EVIDENCE`
  - [ ] `R2_BUCKET_DATA`

## 4) データソースキー

- [ ] J-Quants Light 登録 (`JQUANTS_EMAIL`, `JQUANTS_PASSWORD`)
- [ ] Massive Stocks Starter 登録 (`MASSIVE_API_KEY`)
- [ ] EDINET API キー発行 (`EDINET_API_KEY`)
- [ ] SEC User-Agent を設定 (`SEC_USER_AGENT="stock-analysis (contact: hayasesou3@gmail.com)"`)

## 5) Lightsail ワーカー

- [ ] 東京リージョンで Lightsail インスタンス作成
- [ ] SSH 接続確認
- [ ] Docker / docker compose インストール
- [ ] `/opt/stock-analysis/` にコード配置
- [ ] `.env` に必須キーを設定
  - [ ] `NEON_DATABASE_URL`
  - [ ] `R2_*`
  - [ ] `OPENAI_API_KEY`
  - [ ] `DISCORD_WEBHOOK_URL`
  - [ ] `JQUANTS_*`
  - [ ] `MASSIVE_API_KEY`
  - [ ] `EDINET_API_KEY`
  - [ ] `SEC_USER_AGENT`
- [ ] `jobs.daily` 手動実行で Discord 通知確認
- [ ] `jobs.weekly` 手動実行で Neon/R2 成果物確認
- [ ] cron 設定
  - [ ] 日次 20:00 JST
  - [ ] 週次 土曜 06:30 JST

## 6) Vercel Web

- [ ] Next.js プロジェクトを Vercel Hobby へデプロイ
- [ ] Env Vars 設定
  - [ ] `BASIC_AUTH_USER`, `BASIC_AUTH_PASS`
  - [ ] `NEON_DATABASE_URL`
  - [ ] `R2_*`
  - [ ] （必要時）`OPENAI_API_KEY`
- [ ] Top50 一覧表示確認
- [ ] 銘柄詳細と引用表示確認
- [ ] バックテスト 3 コスト比較確認
- [ ] チャット（引用 + 差分）確認

## 7) 最終確認

- [ ] PC OFF でも Web 閲覧継続（Vercel）
- [ ] Lightsail 停止時も閲覧継続（更新のみ停止）
- [ ] Secrets が Git に含まれていない
- [ ] 予算ガード（資料上限・LLM上限）が有効
