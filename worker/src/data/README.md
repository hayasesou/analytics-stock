# Data Module Map

- `provider.py`: `HybridDataProvider` facade。外部 import の入口。
- `provider_master.py`: JP/US 銘柄マスタ取得と mock universe。
- `provider_prices.py`: 価格履歴取得、Stooq/J-Quants/Massive の切り替え、USDJPY。
- `provider_events.py`: SEC / EDINET のイベント取得と正規化。

読む順番:
1. `provider.py`
2. 必要な責務の helper module だけ開く

主要 public API:
- `HybridDataProvider.load_securities(as_of_date)`
- `HybridDataProvider.load_price_history(securities, start_date, end_date)`
- `HybridDataProvider.load_usdjpy(start_date, end_date)`
- `HybridDataProvider.load_recent_events(now, hours=24)`
