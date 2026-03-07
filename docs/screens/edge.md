# /edge（Edge監視）

## この画面の目的

- 「いまどれが有利か」を連続値で監視する画面です。
- シグナルが出ない日でも、機会の近さを把握できます。

## できること

- 戦略ごとの `Edge Score` と `Net Edge` を確認
- トレンド（時間推移）を確認
- リスク状態（normal/halted/cooldown）を確認

## 項目の意味

- `Edge Score`
  - 優位性を0〜100でスコア化した値。
- `Net Edge`
  - 手数料・スリッページ等を引いた後の期待値。
- `Dist`（Distance to Entry）
  - エントリー条件までの距離。小さいほど条件に近いです。
- `Conf`（Confidence）
  - 推定の信頼度。
- `Risk State`
  - `normal`: 稼働可
  - `cooldown`: 一時停止中
  - `halted`: リスク停止中

## フィルタの使い方

- `Market Scope`
  - `JP_EQ` / `US_EQ` / `CRYPTO` / `MIXED`
- `Strategy Name`
  - 戦略名で絞り込み
- `Symbol`
  - 銘柄で絞り込み
- `Rows Limit` / `Trend Limit`
  - 表示件数

## 判断の目安

- 優先確認: `Net Edge > 0`
- 注意: `Risk State = halted/cooldown`
- 監視: `Dist` が縮小している戦略（機会が近い）

## 異常時の確認順

1. `Risk State` が `halted` か確認
2. `/execution` で `failed/rejected` が増えていないか確認
3. `/research` で戦略の状態（paper/live）を確認
4. 必要ならその戦略を一時停止

## 毎日の確認手順（5分）

1. `/edge` で `Net Edge > 0` を抽出
2. `Risk State = normal` の戦略だけ残す
3. 上位を `/execution` で実行状態確認

