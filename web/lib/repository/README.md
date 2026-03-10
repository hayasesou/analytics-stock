# Repository Map

- `shared.ts`: clamp / decode / fold validation helper。
- `core.ts`: run lookup, top50, security resolve。
- `reports.ts`: facade。実処理は `reports_timeline.ts`, `reports_queries.ts`, `reports_events.ts` に分割済み。
- `backtest.ts`: backtest runs / metrics / points。
- `execution.ts`: intents, risk snapshots, edge states/trend。
- `research_lifecycle.ts`: facade。実処理は `research_lifecycle_kanban.ts`, `research_lifecycle_queries.ts`, `research_lifecycle_actions.ts` に分割済み。
- `research_write.ts`: chat append, evidence search, create APIs。
- `research_read.ts`: facade。実処理は `research_read_records.ts`, `research_read_sessions.ts` に分割済み。
- `../repository.ts`: barrel export only。
