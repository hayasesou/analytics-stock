# Architecture (MVP v1.1 baseline)

## Components

- `worker` (Python, Lightsail):
  - Daily job (`jobs.daily`): 24h event aggregation, report generation, Discord notification
  - Weekly job (`jobs.weekly`): Layer0/1/2 scoring, Top50, B-signal, DCF Top10, backtest(3 costs), persistence
- `web` (Next.js, Vercel):
  - Top50 list with filter/sort/signal visibility
  - Security reports with claim/evidence view
  - Weekly summary, backtest dashboard, daily event list
  - Q&A chat endpoint with citation-first answer and diff output
- `storage`:
  - Neon Postgres: run metadata, scores, reports, citations, events, backtest, chat logs
  - R2: artifacts (parquet/json/text), evidence cache

## Data flow

1. `weekly` run starts and records `runs` row
2. Universe + prices + FX are collected (mock provider fallback is enabled)
3. Features -> style scores -> market percentile normalization
4. Top50 mixed ranking with JP/US minimum constraints
5. Signals (High confidence and Top10 + weekly entry cap)
6. Top50 reports and DCF reports are generated with claim IDs and citations
7. Backtest runs with 3 cost profiles and ATR rules
8. Results are written to Neon and artifacts to R2
9. Discord weekly links are sent

## Security

- Web/API protected by HTTP Basic auth middleware
- Secrets are expected only via environment variables
