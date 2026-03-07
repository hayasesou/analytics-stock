# Research Chat / Discord Research 引き継ぎ仕様書

最終更新日: 2026-03-08

## 1. 文書の目的

この文書は、`analytics-stock` リポジトリ内で進めている `Research Chat / Discord Research` 機能について、次担当者がそのまま実装・改善・運用を再開できるようにするための詳細な引き継ぎ仕様書である。

この文書を読めば、少なくとも次が分かることを目的とする。

- 何を作ろうとしているか
- 現在どこまで実装済みか
- 何が今回の会話で追加実装されたか
- 実際に今どう動くか
- まだ何が残っているか
- 次担当者は何をどの順番でやるべきか
- どのファイルを見るべきか
- どのコマンドで確認すべきか

---

## 2. ゴールの再整理

### 2.1 この機能の目的

Discord または Web から、URL と自然文を投げるだけで以下を一気通貫で行う research 支援基盤を作ることが目的である。

- session 作成
- 入力保存
- URL 抽出
- 仮説生成
- 反証条件生成
- validation plan 生成
- SQL / Python artifact 生成
- artifact 実行
- chart 生成
- Discord thread への follow-up
- Web での追跡

### 2.2 現在の到達点

現時点では、PoC / 内部運用としてはかなり高い完成度に達している。

既にできること:

- Discord で Bot にメンションして research session を起票できる
- 初回返信に `session URL` を含められる
- worker 完了後に同じ Discord thread へ詳細 follow-up を返せる
- hypotheses / artifacts / validation を Web で追える
- SQL / Python artifact を実行できる
- chart を agent が動的に提案して生成できる
- chart を Web で見られる
- chart を Discord thread に PNG 画像で送れる
- chart だけを再生成する task を実行できる
- chart 生成時に `プリセット指定 + 自然文 instruction` の両方を渡せる

ただし、本番品質としてはまだ改善余地が残る。

---

## 3. 今回の会話で追加した主な実装

この節は、今回のやり取りの中で追加・変更した内容だけを整理したもの。

### 3.1 Discord UX 改善

追加したこと:

- Discord 初回受付返信に `WEB_BASE_URL` ベースの `session URL` を含めた
- worker 完了後に同じ Discord thread へ詳細 follow-up を返すようにした
- follow-up には session URL, summary, hypotheses, artifacts 概要を含めるようにした

対象ファイル:

- `worker/src/jobs/discord_research_listener.py`
- `worker/src/jobs/research_chat.py`
- `worker/src/integrations/discord.py`

### 3.2 Artifacts 画面改善

追加したこと:

- `latestRun` の run status を見やすく表示
- `stdout` / `stderr` / `result_json` / `output_r2_key` / 実行時刻を表示
- `sessionId` フィルタに対応
- chart artifact を表示可能にした

対象ファイル:

- `web/app/research/artifacts/page.tsx`
- `web/components/ResearchArtifactChart.tsx`

### 3.3 research prompt 改善

追加したこと:

- 全モードで「要約」ではなく「検証可能な主張」に寄せた
- 業績接続、競合比較、市場コンテキスト、織り込み済み論点を prompt に明示した
- validation_plan と key_metrics の重みを上げた

対象ファイル:

- `worker/src/llm/research_prompts.py`

### 3.4 chart artifact 追加

追加したこと:

- SQL / Python run 結果から chart artifact を自動生成するようにした
- 最初は固定 chart だったが、その後 agent が動的に chart を考える方式に変更した
- chart は DB に artifact として保存される
- Web では ECharts で描画する
- Discord では PNG として添付送信する

対象ファイル:

- `worker/src/jobs/research_chat.py`
- `web/components/ResearchArtifactChart.tsx`
- `web/app/research/artifacts/page.tsx`

### 3.5 chart only 再生成追加

追加したこと:

- `research.chart_generate` task を追加
- SQL / Python artifact の `latest successful run` から chart だけ再生成できるようにした
- Web の `Research Chat` から `Chart Task` を叩けるようにした

対象ファイル:

- `worker/src/jobs/research_chat.py`
- `worker/src/storage/db.py`
- `web/app/api/research/artifacts/[artifactId]/chart/route.ts`
- `web/components/ResearchChatClient.tsx`

### 3.6 chart 指定機能追加

追加したこと:

- `Chart Task` 実行時に `chartType` を指定できるようにした
- `chartInstruction` という自然文補足も同時に渡せるようにした
- worker 側では両方を chart planning prompt に反映する
- fallback でも一部の指定は尊重する

対象ファイル:

- `web/components/ResearchChatClient.tsx`
- `web/app/api/research/artifacts/[artifactId]/chart/route.ts`
- `worker/src/jobs/research_chat.py`

---

## 4. 現在のシステム挙動

### 4.1 Discord 受信時

現在の流れ:

1. Discord で Bot をメンションする
2. URL / 自然文を送る
3. listener が thread を使って受け付ける
4. session / user message / external_input / bootstrap hypotheses / note artifact / tasks を作る
5. 初回受付返信を返す
6. 受付返信には `session=<uuid>` と `session URL` が含まれる
7. 裏で `research-chat` worker が task を処理する

### 4.2 worker 完了後

現在の流れ:

1. `portfolio_build` 完了時に summary を assistant message として保存
2. Discord 由来 session であれば同じ thread に follow-up を送る
3. follow-up には次が含まれる
   - session URL
   - summary
   - hypotheses の要約
   - artifact 一覧

### 4.3 artifact run 時

現在の流れ:

1. SQL または Python artifact を実行
2. `research_artifact_runs` に run 結果を保存
3. run 結果を見て chart を作れるなら chart artifact を 1-3 個作る
4. chart artifact は DB に保存される
5. chart 要約を Discord thread に送る
6. 各 chart を PNG 画像として Discord thread に送る

### 4.4 chart only 再生成時

現在の流れ:

1. Web から `Chart Task` を押す
2. `research.chart_generate` task を enqueue
3. worker が元 artifact の `latest successful run` を読む
4. chart だけ再生成する
5. Web と Discord に反映される

---

## 5. chart 生成の現在仕様

### 5.1 基本方針

chart は固定ロジックで 1 本だけ作るのではなく、agent が run 結果を見て 1-3 本提案する。

### 5.2 LLM 経路

OpenAI が利用可能な場合:

- artifact の `artifact_type`
- artifact title
- body
- code
- result preview
- chartType
- chartInstruction

を見て、JSON schema に従って chart spec を返す。

### 5.3 fallback 経路

OpenAI が使えない場合:

- SQL の `[x, y]` 形結果なら line / bar / scatter などを fallback 生成
- Python の `chart` payload があればそこから chart spec を再構成
- `chartType` や `instruction` に応じて kind を変える

### 5.4 現在対応している chart kind

- `line`
- `bar`
- `scatter`
- `area`

### 5.5 Web 表示

Web は ECharts で描画する。

対象ファイル:

- `web/components/ResearchArtifactChart.tsx`

### 5.6 Discord 表示

Discord には PNG を添付する。

PNG は worker 側で pure Python で生成している。

理由:

- Pillow / matplotlib が現環境に入っていなかった
- 依存を増やさずに PNG を直接送る必要があった

現状の注意:

- PNG 品質は機能重視であり、高度な描画品質ではない
- タイトルやラベル描画は簡易
- area chart の塗りも簡易

---

## 6. 現在の主要 task 一覧

research 系で現在使う task:

- `research.extract_input`
- `research.generate_hypothesis`
- `research.critic_review`
- `research.quant_plan`
- `research.code_generate`
- `research.portfolio_build`
- `research.artifact_run`
- `research.chart_generate`
- `research.validate_outcome`
- `research.session_summarize`

### 6.1 `research.chart_generate`

役割:

- 既存 artifact の最新 successful run から chart だけ再生成する

想定入力 payload:

```json
{
  "session_id": "uuid",
  "artifact_id": "uuid",
  "requested_by": "web",
  "chart_type": "scatter",
  "chart_instruction": "イベント後累積リターンを優先して"
}
```

完了時:

- chart artifact を 1-3 個作る
- Discord 由来なら同じ thread に PNG を送る

---

## 7. 現在の Web UI 仕様

### 7.1 `/research/chat`

現在できること:

- 自然文 / URL を送信
- summarize task enqueue
- validate task enqueue
- artifact run enqueue
- chart task enqueue

Artifacts セクションには現在以下がある:

- `Run Task`
- `Chart Task`
- `Chart Type` セレクタ
- `Chart Instruction` 入力欄

### 7.2 `/research/artifacts`

現在できること:

- artifact 一覧表示
- `sessionId` 絞り込み
- latestRun の内容確認
- chart artifact の描画

まだ弱い点:

- artifacts page 自体には `Chart Task` ボタンをまだ出していない
- `Chart Type` / `Instruction` の指定 UI は現時点では `ResearchChatClient` 側中心

---

## 8. 現在の主要ファイルマップ

### 8.1 Worker

- `worker/src/jobs/discord_research_listener.py`
- `worker/src/jobs/research_chat.py`
- `worker/src/integrations/discord.py`
- `worker/src/storage/db.py`
- `worker/src/llm/research_prompts.py`
- `worker/src/llm/openai_client.py`

### 8.2 Web

- `web/app/research/artifacts/page.tsx`
- `web/components/ResearchArtifactChart.tsx`
- `web/components/ResearchChatClient.tsx`
- `web/app/api/research/artifacts/[artifactId]/run/route.ts`
- `web/app/api/research/artifacts/[artifactId]/chart/route.ts`

### 8.3 テスト

- `worker/tests/test_research_chat_job.py`
- `worker/tests/test_discord_research_listener_job.py`
- `worker/tests/test_research_prompts.py`

---

## 9. 現在の確認済みテスト状態

今回の作業完了時点で以下を確認済み。

### 9.1 pytest

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache ./worker/.venv/bin/python -m pytest -q \
  worker/tests/test_research_chat_job.py \
  worker/tests/test_discord_research_listener_job.py \
  worker/tests/test_research_prompts.py
```

結果:

- `21 passed`

### 9.2 py_compile

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache ./worker/.venv/bin/python -m py_compile \
  worker/src/jobs/research_chat.py \
  worker/src/storage/db.py \
  worker/src/integrations/discord.py
```

### 9.3 TypeScript

```bash
./node_modules/.bin/tsc --noEmit --incremental false
```

---

## 10. まだ残っている改善タスク

ここから先が、次担当者がやるべきこと。

重要なのは、もう大きな基盤実装ではなく `品質改善フェーズ` に入っていること。

### P1. Discord 投稿の retry / 重複防止

現状の課題:

- Discord API 送信失敗時の retry が弱い
- 同じ chart / same run の重複送信抑止が弱い
- Discord 投稿結果を DB に記録していない

やること:

- `send_bot_message` / `send_bot_file` に retry を入れる
- exponential backoff か短い retry を導入する
- artifact metadata または task result に `discord_sent_at` / `discord_message_hash` を持たせる
- 同じ run に対して複数回投稿しない仕組みを入れる

対象ファイル:

- `worker/src/integrations/discord.py`
- `worker/src/jobs/research_chat.py`

### P1. PNG 品質改善

現状の課題:

- pure Python の手描き PNG のため、見た目は簡易
- 軸ラベル / legend / annotation の品質は低い
- 複雑 chart に弱い

やること:

- PNG レイアウト改善
- legend 位置改善
- タイトル / summary の描画改善
- scatter / bar / area の見た目改善
- 可能なら将来的にライブラリ導入を検討

対象ファイル:

- `worker/src/jobs/research_chat.py`

### P1. chart planner の賢さ向上

現状の課題:

- chart は動的に決まるが、金融ドメイン特化の賢さはまだ弱い
- `relative_return`, `event_window`, `benchmark comparison` を自動で強く出すには改善余地あり

やること:

- chart planning prompt を別ファイルに切り出してもよい
- SQL columns の意味推定を強化する
- hypothesis metadata を chart planning にもっと使う
- event 分析モード時は `event後累積リターン` を優先するなどのルールを強化する

対象ファイル:

- `worker/src/jobs/research_chat.py`

### P1. `/research/artifacts` からも Chart Task を打てるようにする

現状の課題:

- `ResearchChatClient` では Chart Task がある
- `/research/artifacts` ページ単体にはまだ chart 再生成 UI がない

やること:

- artifacts page に `Chart Task` ボタン追加
- `chartType` セレクタ追加
- `instruction` 入力欄追加

対象ファイル:

- `web/app/research/artifacts/page.tsx`

### P1. URL 抽出品質改善

現状の課題:

- `requests + title/excerpt` ベースで軽い
- JS-heavy ページやニュース / SNS 系で弱い

やること:

- readability 系ライブラリ検討
- source type 別 extractor 実装
- YouTube / X / ニュースサイト分岐
- extracted_text の品質評価改善

対象ファイル:

- `worker/src/jobs/research_chat.py`
- `worker/src/jobs/discord_research_listener.py`

### P1. 仮説品質のさらなる改善

現状の課題:

- 以前より良くなったが、情報が薄い入力だと仮説も浅くなる
- 要約寄りになるケースはまだある

やること:

- prompt 改善を継続
- mode ごとの要求をさらに具体化
- evidence gap を明示させる
- validation plan を artifact により直接つなげる

対象ファイル:

- `worker/src/llm/research_prompts.py`
- `worker/src/jobs/research_chat.py`

---

## 11. 次担当者がまず読むべき順番

1. `worker/src/jobs/discord_research_listener.py`
2. `worker/src/jobs/research_chat.py`
3. `worker/src/integrations/discord.py`
4. `worker/src/storage/db.py`
5. `web/components/ResearchChatClient.tsx`
6. `web/app/research/artifacts/page.tsx`
7. `web/components/ResearchArtifactChart.tsx`
8. `worker/src/llm/research_prompts.py`

---

## 12. 次担当者が最初にやるべき確認

### 12.1 ローカル確認

```bash
docker compose ps web research-chat discord-research-listener
```

```bash
docker compose logs --tail=100 research-chat
```

```bash
docker compose logs --tail=100 discord-research-listener
```

### 12.2 テスト再確認

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache ./worker/.venv/bin/python -m pytest -q \
  worker/tests/test_research_chat_job.py \
  worker/tests/test_discord_research_listener_job.py \
  worker/tests/test_research_prompts.py
```

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache ./worker/.venv/bin/python -m py_compile \
  worker/src/jobs/research_chat.py \
  worker/src/storage/db.py \
  worker/src/integrations/discord.py
```

```bash
./node_modules/.bin/tsc --noEmit --incremental false
```

### 12.3 実際の動作確認シナリオ

Discord に次のような文を送る。

```text
@BotName US:NVDA https://www.nvidia.com/en-us/data-center/ai-factories/ この内容で投資仮説を作って
```

確認観点:

- 受付返信に session URL が出るか
- 数十秒後に follow-up が出るか
- chart 要約が出るか
- chart PNG が出るか
- `/research/chat?sessionId=...` で session が見えるか
- `/research/artifacts?sessionId=...` で chart が見えるか
- `Chart Task` で chart only 再生成できるか
- `chartType + instruction` が効いているか

---

## 13. 環境変数前提

最低限必要:

- `NEON_DATABASE_URL`
- `OPENAI_API_KEY`
- `DISCORD_BOT_TOKEN`
- `WEB_BASE_URL`

Discord 側前提:

- Bot 招待済み
- `Message Content Intent` ON
- thread 作成権限
- thread 投稿権限

---

## 14. 現時点の完成度評価

### 14.1 完成しているとみなしてよい部分

- Discord 起票
- session URL 返信
- worker follow-up
- artifact run
- chart 動的生成
- chart Web 表示
- chart Discord PNG 送信
- chart only 再生成
- chartType + instruction 指定

### 14.2 まだ未完成とみなすべき部分

- Discord retry / idempotency
- PNG の見た目改善
- URL 抽出品質
- 仮説品質のさらなる改善
- 本番監視 / エラー観測

---

## 15. 最後の短い結論

現時点の `Research Chat / Discord Research` は、PoC / 内部運用としてはかなり完成度が高い。  
ここから先は大規模な基盤構築よりも、`品質改善・運用品質向上` が中心になる。

次担当者がまずやるべきことは次の3つ。

1. Discord 投稿の retry / 重複防止
2. PNG 品質改善
3. URL 抽出と仮説品質の改善

逆に言うと、主要導線はすでに成立している。  
これからは「作る」より「磨く」フェーズである。
