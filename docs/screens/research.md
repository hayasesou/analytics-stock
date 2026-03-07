# /research（研究管理）

## この画面の目的

- 戦略の育成状況を管理する画面です。
- 「候補作成 → 検証 → paper → live」を一元管理します。

## できること

- Kanbanで進捗管理
- 戦略評価（Sharpe/DD/CAGR等）の比較
- `Paperへ` / `Live承認` / `却下` の手動操作

## Kanbanの意味

- `new`: 新規アイデア
- `analyzing`: 検証中
- `candidate`: 検証通過候補
- `paper`: 検証運用中（実資金なし）
- `live`: 本番運用
- `rejected`: 不採用

## 主要項目の意味

- `Sharpe`: リスクあたり効率（高いほど良い）
- `Max DD`: 最大下落幅（小さいほど良い）
- `CAGR`: 年率成長率（高いほど良い）
- `validation`: fold検証の通過可否

## 操作ルール（推奨）

1. `candidate` を `Paperへ` 昇格
2. 一定期間観察後、問題なければ `Live承認`
3. 問題があれば `却下`（理由を必ず記録）

## 判断の目安

- `validation = pass` を優先
- `Max DD` が大きい戦略は保留
- 指標が良くても、実運用（paper）の安定性を優先

## 毎週の運用手順

1. `candidate` の新規追加を確認
2. `paper` の結果確認
3. 昇格/却下を手動で判断

