CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_type TEXT NOT NULL CHECK (run_type IN ('daily', 'weekly', 'research')),
  status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  config_version TEXT,
  model_name TEXT,
  prompt_version TEXT,
  temperature NUMERIC(4, 3),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS securities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  security_id TEXT NOT NULL UNIQUE,
  market TEXT NOT NULL CHECK (market IN ('JP', 'US')),
  ticker TEXT NOT NULL,
  name TEXT,
  sector TEXT,
  industry TEXT,
  currency TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_securities_market_ticker ON securities (market, ticker);

CREATE TABLE IF NOT EXISTS universe_membership (
  id BIGSERIAL PRIMARY KEY,
  security_id UUID NOT NULL REFERENCES securities(id),
  universe TEXT NOT NULL,
  as_of_date DATE NOT NULL,
  is_member BOOLEAN NOT NULL DEFAULT TRUE,
  source TEXT,
  retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (security_id, universe, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_universe_membership_asof ON universe_membership (universe, as_of_date);

CREATE TABLE IF NOT EXISTS prices_daily (
  security_id UUID NOT NULL REFERENCES securities(id),
  trade_date DATE NOT NULL,
  open_raw NUMERIC(18, 6) NOT NULL,
  high_raw NUMERIC(18, 6) NOT NULL,
  low_raw NUMERIC(18, 6) NOT NULL,
  close_raw NUMERIC(18, 6) NOT NULL,
  volume BIGINT,
  adjusted_close NUMERIC(18, 6),
  adjustment_factor NUMERIC(18, 8) NOT NULL DEFAULT 1.0,
  source TEXT NOT NULL,
  retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (security_id, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_prices_daily_trade_date ON prices_daily (trade_date);

CREATE TABLE IF NOT EXISTS fx_rates_daily (
  pair TEXT NOT NULL,
  trade_date DATE NOT NULL,
  rate NUMERIC(18, 6) NOT NULL,
  source TEXT NOT NULL,
  retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (pair, trade_date)
);

CREATE TABLE IF NOT EXISTS documents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  external_doc_id TEXT NOT NULL,
  source_system TEXT NOT NULL,
  source_url TEXT NOT NULL,
  title TEXT,
  published_at TIMESTAMPTZ,
  security_id UUID REFERENCES securities(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (source_system, external_doc_id)
);

CREATE TABLE IF NOT EXISTS document_versions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id UUID NOT NULL REFERENCES documents(id),
  retrieved_at TIMESTAMPTZ NOT NULL,
  sha256 CHAR(64) NOT NULL,
  mime_type TEXT NOT NULL,
  r2_object_key TEXT NOT NULL,
  r2_text_key TEXT,
  page_count INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (document_id, sha256)
);

CREATE INDEX IF NOT EXISTS idx_document_versions_retrieved_at ON document_versions (retrieved_at DESC);

CREATE TABLE IF NOT EXISTS evidence_chunks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  doc_version_id UUID NOT NULL REFERENCES document_versions(id),
  chunk_index INTEGER NOT NULL,
  chunk_text TEXT NOT NULL,
  embedding VECTOR(1536),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (doc_version_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_evidence_chunks_doc ON evidence_chunks (doc_version_id);

CREATE TABLE IF NOT EXISTS events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID REFERENCES runs(id),
  security_id UUID REFERENCES securities(id),
  event_type TEXT NOT NULL,
  importance TEXT NOT NULL CHECK (importance IN ('high', 'medium', 'low')),
  event_time TIMESTAMPTZ NOT NULL,
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  source_url TEXT,
  doc_version_id UUID REFERENCES document_versions(id),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_event_time ON events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_importance_time ON events (importance, event_time DESC);

CREATE TABLE IF NOT EXISTS score_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES runs(id),
  security_id UUID NOT NULL REFERENCES securities(id),
  as_of_date DATE NOT NULL,
  quality NUMERIC(6, 3) NOT NULL,
  growth NUMERIC(6, 3) NOT NULL,
  value NUMERIC(6, 3) NOT NULL,
  momentum NUMERIC(6, 3) NOT NULL,
  catalyst NUMERIC(6, 3) NOT NULL,
  combined_score NUMERIC(6, 3) NOT NULL,
  missing_ratio NUMERIC(6, 3) NOT NULL DEFAULT 0,
  liquidity_flag BOOLEAN NOT NULL DEFAULT FALSE,
  exclusion_flag BOOLEAN NOT NULL DEFAULT FALSE,
  confidence TEXT NOT NULL CHECK (confidence IN ('High', 'Medium', 'Low')),
  market_rank INTEGER,
  mixed_rank INTEGER,
  flags JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_id, security_id)
);

CREATE INDEX IF NOT EXISTS idx_score_snapshots_asof ON score_snapshots (as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_score_snapshots_combined ON score_snapshots (combined_score DESC);

CREATE TABLE IF NOT EXISTS top50_membership (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES runs(id),
  security_id UUID NOT NULL REFERENCES securities(id),
  rank INTEGER NOT NULL,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_id, security_id),
  UNIQUE (run_id, rank)
);

CREATE TABLE IF NOT EXISTS signals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES runs(id),
  security_id UUID NOT NULL REFERENCES securities(id),
  as_of_date DATE NOT NULL,
  is_signal BOOLEAN NOT NULL,
  entry_allowed BOOLEAN NOT NULL DEFAULT FALSE,
  reason TEXT,
  rank INTEGER,
  confidence TEXT NOT NULL CHECK (confidence IN ('High', 'Medium', 'Low')),
  valid_until DATE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_id, security_id)
);

CREATE TABLE IF NOT EXISTS reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID REFERENCES runs(id),
  security_id UUID REFERENCES securities(id),
  report_type TEXT NOT NULL CHECK (report_type IN ('weekly_summary', 'security_full', 'dcf', 'event_digest', 'chat_answer')),
  title TEXT NOT NULL,
  body_md TEXT NOT NULL,
  conclusion TEXT,
  falsification_conditions TEXT,
  confidence TEXT CHECK (confidence IN ('High', 'Medium', 'Low')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reports_security_type ON reports (security_id, report_type, created_at DESC);

CREATE TABLE IF NOT EXISTS report_claims (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  report_id UUID NOT NULL REFERENCES reports(id),
  claim_id TEXT NOT NULL,
  claim_text TEXT NOT NULL,
  claim_type TEXT NOT NULL DEFAULT 'important',
  status TEXT NOT NULL CHECK (status IN ('supported', 'hypothesis')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (report_id, claim_id)
);

CREATE TABLE IF NOT EXISTS citations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  report_id UUID NOT NULL REFERENCES reports(id),
  claim_id TEXT NOT NULL,
  doc_version_id UUID NOT NULL REFERENCES document_versions(id),
  page_ref TEXT,
  quote_text TEXT NOT NULL,
  locator JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_citations_report_claim ON citations (report_id, claim_id);

CREATE TABLE IF NOT EXISTS backtest_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES runs(id),
  as_of_date DATE NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  common_period_start DATE,
  common_period_end DATE,
  strategy_name TEXT NOT NULL DEFAULT 'B_MODE',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backtest_metrics (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  backtest_run_id UUID NOT NULL REFERENCES backtest_runs(id),
  cost_profile TEXT NOT NULL CHECK (cost_profile IN ('zero', 'standard', 'strict')),
  market_scope TEXT NOT NULL CHECK (market_scope IN ('JP', 'US', 'MIXED', 'COMMON')),
  cagr NUMERIC(10, 6),
  max_dd NUMERIC(10, 6),
  sharpe NUMERIC(10, 6),
  sortino NUMERIC(10, 6),
  volatility NUMERIC(10, 6),
  win_rate NUMERIC(10, 6),
  avg_win NUMERIC(18, 6),
  avg_loss NUMERIC(18, 6),
  alpha_simple NUMERIC(10, 6),
  information_ratio_simple NUMERIC(10, 6),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (backtest_run_id, cost_profile, market_scope)
);

CREATE TABLE IF NOT EXISTS backtest_equity_curve (
  id BIGSERIAL PRIMARY KEY,
  backtest_run_id UUID NOT NULL REFERENCES backtest_runs(id),
  cost_profile TEXT NOT NULL CHECK (cost_profile IN ('zero', 'standard', 'strict')),
  trade_date DATE NOT NULL,
  equity NUMERIC(18, 6) NOT NULL,
  benchmark_equity NUMERIC(18, 6),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (backtest_run_id, cost_profile, trade_date)
);

CREATE TABLE IF NOT EXISTS backtest_trades (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  backtest_run_id UUID NOT NULL REFERENCES backtest_runs(id),
  cost_profile TEXT NOT NULL CHECK (cost_profile IN ('zero', 'standard', 'strict')),
  security_id UUID REFERENCES securities(id),
  market TEXT NOT NULL CHECK (market IN ('JP', 'US')),
  entry_date DATE NOT NULL,
  entry_price NUMERIC(18, 6) NOT NULL,
  exit_date DATE,
  exit_price NUMERIC(18, 6),
  quantity NUMERIC(18, 6) NOT NULL,
  gross_pnl NUMERIC(18, 6),
  net_pnl NUMERIC(18, 6),
  cost NUMERIC(18, 6) NOT NULL DEFAULT 0,
  exit_reason TEXT,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_backtest_trades_dates ON backtest_trades (entry_date, exit_date);

CREATE TABLE IF NOT EXISTS chat_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  title TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_diagnostics_weekly (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES runs(id),
  horizon_days INTEGER NOT NULL CHECK (horizon_days IN (5, 20, 60)),
  hit_rate NUMERIC(10, 6) NOT NULL,
  median_return NUMERIC(10, 6),
  p10_return NUMERIC(10, 6),
  p90_return NUMERIC(10, 6),
  sample_size INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_id, horizon_days)
);

CREATE INDEX IF NOT EXISTS idx_signal_diag_run ON signal_diagnostics_weekly (run_id, horizon_days);

CREATE TABLE IF NOT EXISTS chat_messages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id UUID NOT NULL REFERENCES chat_sessions(id),
  run_id UUID REFERENCES runs(id),
  role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
  content TEXT NOT NULL,
  answer_before TEXT,
  answer_after TEXT,
  change_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_session_time ON chat_messages (session_id, created_at);

CREATE TABLE IF NOT EXISTS chat_message_citations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  message_id UUID NOT NULL REFERENCES chat_messages(id),
  doc_version_id UUID NOT NULL REFERENCES document_versions(id),
  page_ref TEXT,
  quote_text TEXT NOT NULL,
  claim_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_message_citations_msg ON chat_message_citations (message_id);
