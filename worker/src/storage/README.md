# Storage Map

- `db.py`: public facade。外部は `NeonRepository` だけを使う。
- `db_ingest.py`: facade。実処理は `db_ingest_lifecycle.py`, `db_ingest_market.py`, `db_ingest_crypto.py` に分割済み。
- `db_scores.py`: score/top50/signal diagnostics。
- `db_reports.py`: facade。実処理は `db_reports_documents.py`, `db_reports_read.py`, `db_reports_write.py`, `db_reports_backtest.py` に分割済み。
- `db_strategy.py`: strategy lifecycle と edge state。
- `db_ideas.py`: ideas/experiments/lessons。
- `db_trading.py`: facade。実処理は `db_trading_risk.py`, `db_trading_execution.py`, `db_trading_fundamentals.py` に分割済み。
- `db_research_write.py`: agent queue と research write API。
- `db_chat.py`: chat/external input/hypothesis/artifact read-write。
- `db_portfolio_queries.py`, `db_read_queries.py`: executor/edge/read model 系 query。
