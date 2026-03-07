# /execution（執行監視）

## この画面の目的

- 注文が正しく流れているかを確認する画面です。
- 失敗や停止を早く見つけるために使います。

## できること

- `order_intents` の状態を監視
- リスク状態（normal/risk_alert/halted）を監視
- 失敗注文の傾向を確認

## 項目の意味

- `proposed`
  - 注文案が作られた状態
- `approved`
  - 実行許可済み
- `sent/executing`
  - 発注済み/処理中
- `done`
  - 完了
- `rejected`
  - 条件に合わず除外
- `failed`
  - 実行失敗
- `canceled`
  - 取消

## フィルタの使い方

- `Intent Status`
  - 状態で絞り込み
- `Portfolio`
  - ポートフォリオ名で絞り込み
- `Limit`
  - 件数

## 判断の目安

- 通常: `done` が増え、`failed` が低い
- 要注意: `rejected` が急増
- 危険: `halted` が発生

## 異常時の対処フロー

1. `failed` の直近レコードを確認
2. `risk_alert/halted` の件数を確認
3. `/edge` で対象戦略の `Risk State` を確認
4. 必要なら運用を止め、原因（データ欠損/接続/API制限）を調査

## 毎日の確認手順（3分）

1. `failed` 件数
2. `halted` 件数
3. 直近1時間の `rejected` 増加有無

