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

CREATE TABLE IF NOT EXISTS crypto_market_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  exchange TEXT NOT NULL CHECK (exchange IN ('binance', 'hyperliquid')),
  symbol TEXT NOT NULL,
  market_type TEXT NOT NULL CHECK (market_type IN ('spot', 'perp')),
  observed_at TIMESTAMPTZ NOT NULL,
  best_bid NUMERIC(24, 10),
  best_ask NUMERIC(24, 10),
  mid NUMERIC(24, 10),
  spread_bps NUMERIC(18, 8),
  funding_rate NUMERIC(18, 10),
  open_interest NUMERIC(24, 10),
  mark_price NUMERIC(24, 10),
  index_price NUMERIC(24, 10),
  basis_bps NUMERIC(18, 8),
  source_mode TEXT NOT NULL CHECK (source_mode IN ('ws', 'rest')),
  latency_ms NUMERIC(12, 3),
  data_quality JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (exchange, symbol, market_type, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_crypto_market_snapshots_symbol_time
  ON crypto_market_snapshots (symbol, market_type, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_crypto_market_snapshots_exchange_time
  ON crypto_market_snapshots (exchange, observed_at DESC);

CREATE TABLE IF NOT EXISTS crypto_data_quality (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  exchange TEXT NOT NULL CHECK (exchange IN ('binance', 'hyperliquid')),
  symbol TEXT NOT NULL,
  market_type TEXT NOT NULL CHECK (market_type IN ('spot', 'perp')),
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  sample_count INTEGER NOT NULL DEFAULT 0 CHECK (sample_count >= 0),
  missing_count INTEGER NOT NULL DEFAULT 0 CHECK (missing_count >= 0),
  missing_ratio NUMERIC(10, 6) NOT NULL DEFAULT 0,
  latency_p95_ms NUMERIC(12, 3),
  ws_failover_count INTEGER NOT NULL DEFAULT 0 CHECK (ws_failover_count >= 0),
  eligible_for_edge BOOLEAN NOT NULL DEFAULT TRUE,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (exchange, symbol, market_type, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_crypto_data_quality_symbol_window
  ON crypto_data_quality (exchange, symbol, market_type, window_end DESC);

CREATE INDEX IF NOT EXISTS idx_crypto_data_quality_edge_gate
  ON crypto_data_quality (eligible_for_edge, window_end DESC);

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

CREATE TABLE IF NOT EXISTS external_inputs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id UUID NOT NULL REFERENCES chat_sessions(id),
  message_id UUID REFERENCES chat_messages(id),
  source_type TEXT NOT NULL CHECK (source_type IN ('discord', 'web', 'x', 'youtube', 'web_url', 'text')),
  source_url TEXT,
  raw_text TEXT,
  extracted_text TEXT,
  quality_grade TEXT CHECK (quality_grade IN ('A', 'B', 'C')),
  extraction_status TEXT NOT NULL DEFAULT 'queued' CHECK (extraction_status IN ('queued', 'success', 'partial', 'failed')),
  user_comment TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_external_inputs_session_time ON external_inputs (session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_external_inputs_source_type ON external_inputs (source_type, created_at DESC);

CREATE TABLE IF NOT EXISTS research_hypotheses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id UUID NOT NULL REFERENCES chat_sessions(id),
  external_input_id UUID REFERENCES external_inputs(id),
  parent_message_id UUID REFERENCES chat_messages(id),
  stance TEXT NOT NULL CHECK (stance IN ('bullish', 'bearish', 'neutral', 'watch')),
  horizon_days INTEGER NOT NULL CHECK (horizon_days IN (1, 5, 20, 60, 120)),
  thesis_md TEXT NOT NULL,
  falsification_md TEXT NOT NULL,
  confidence NUMERIC(4, 3),
  status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'watch', 'validate', 'passed', 'failed', 'archived')),
  is_favorite BOOLEAN NOT NULL DEFAULT FALSE,
  version INTEGER NOT NULL DEFAULT 1,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_hypotheses_session_time ON research_hypotheses (session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_research_hypotheses_status ON research_hypotheses (status, created_at DESC);

CREATE TABLE IF NOT EXISTS research_hypothesis_assets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  hypothesis_id UUID NOT NULL REFERENCES research_hypotheses(id) ON DELETE CASCADE,
  asset_class TEXT NOT NULL CHECK (asset_class IN ('JP_EQ', 'US_EQ', 'CRYPTO')),
  security_id UUID REFERENCES securities(id),
  symbol_text TEXT,
  weight_hint NUMERIC(6, 4),
  confidence NUMERIC(4, 3),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_hypothesis_assets_hypothesis ON research_hypothesis_assets (hypothesis_id);

CREATE TABLE IF NOT EXISTS research_hypothesis_outcomes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  hypothesis_id UUID NOT NULL REFERENCES research_hypotheses(id) ON DELETE CASCADE,
  checked_at TIMESTAMPTZ NOT NULL,
  ret_1d NUMERIC(10, 4),
  ret_5d NUMERIC(10, 4),
  ret_20d NUMERIC(10, 4),
  mfe NUMERIC(10, 4),
  mae NUMERIC(10, 4),
  outcome_label TEXT NOT NULL CHECK (outcome_label IN ('hit', 'miss', 'partial', 'open')),
  summary_md TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_hypothesis_outcomes_hypothesis_time
  ON research_hypothesis_outcomes (hypothesis_id, checked_at DESC);

CREATE TABLE IF NOT EXISTS research_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id UUID NOT NULL REFERENCES chat_sessions(id),
  hypothesis_id UUID REFERENCES research_hypotheses(id) ON DELETE SET NULL,
  artifact_type TEXT NOT NULL CHECK (artifact_type IN ('sql', 'python', 'chart', 'table', 'note', 'report')),
  title TEXT NOT NULL,
  body_md TEXT,
  code_text TEXT,
  language TEXT,
  is_favorite BOOLEAN NOT NULL DEFAULT FALSE,
  created_by_task_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_artifacts_session_time ON research_artifacts (session_id, created_at DESC);

CREATE TABLE IF NOT EXISTS research_artifact_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  artifact_id UUID NOT NULL REFERENCES research_artifacts(id) ON DELETE CASCADE,
  run_status TEXT NOT NULL CHECK (run_status IN ('pending', 'running', 'success', 'failed')),
  stdout_text TEXT,
  stderr_text TEXT,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  output_r2_key TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_artifact_runs_artifact_time
  ON research_artifact_runs (artifact_id, created_at DESC);

CREATE TABLE IF NOT EXISTS strategies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL UNIQUE,
  description TEXT,
  asset_scope TEXT NOT NULL CHECK (asset_scope IN ('JP_EQ', 'US_EQ', 'CRYPTO', 'MIXED')),
  status TEXT NOT NULL CHECK (status IN ('draft', 'candidate', 'approved', 'paper', 'live', 'paused', 'retired')),
  live_candidate BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE strategies
  ADD COLUMN IF NOT EXISTS live_candidate BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_strategies_status ON strategies (status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategies_live_candidate
  ON strategies (live_candidate, updated_at DESC);

CREATE TABLE IF NOT EXISTS strategy_versions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  strategy_id UUID NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
  version INTEGER NOT NULL CHECK (version >= 1),
  spec JSONB NOT NULL,
  code_artifact_key TEXT,
  sha256 CHAR(64),
  created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  approved_by TEXT,
  approved_at TIMESTAMPTZ,
  is_active BOOLEAN NOT NULL DEFAULT FALSE,
  UNIQUE (strategy_id, version)
);

CREATE INDEX IF NOT EXISTS idx_strategy_versions_active
  ON strategy_versions (strategy_id, is_active, created_at DESC);

CREATE TABLE IF NOT EXISTS strategy_lifecycle_reviews (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  strategy_id UUID NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
  strategy_version_id UUID REFERENCES strategy_versions(id) ON DELETE SET NULL,
  action TEXT NOT NULL CHECK (action IN (
    'promote_paper',
    'mark_live_candidate',
    'approve_live',
    'reject_live',
    'demote_live_candidate',
    'manual_status_update'
  )),
  from_status TEXT NOT NULL,
  to_status TEXT NOT NULL,
  live_candidate BOOLEAN NOT NULL DEFAULT FALSE,
  reason TEXT,
  recheck_condition TEXT,
  recheck_after DATE,
  acted_by TEXT NOT NULL,
  acted_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_lifecycle_reviews_strategy_time
  ON strategy_lifecycle_reviews (strategy_id, acted_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_lifecycle_reviews_action_time
  ON strategy_lifecycle_reviews (action, acted_at DESC);

CREATE TABLE IF NOT EXISTS strategy_evaluations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  strategy_version_id UUID NOT NULL REFERENCES strategy_versions(id) ON DELETE CASCADE,
  eval_type TEXT NOT NULL CHECK (eval_type IN ('quick_backtest', 'robust_backtest', 'paper', 'live')),
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  metrics JSONB NOT NULL,
  artifacts JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_evaluations_type_time
  ON strategy_evaluations (eval_type, created_at DESC);

CREATE TABLE IF NOT EXISTS edge_states (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  strategy_name TEXT NOT NULL,
  strategy_version_id UUID REFERENCES strategy_versions(id) ON DELETE SET NULL,
  market_scope TEXT NOT NULL CHECK (market_scope IN ('JP_EQ', 'US_EQ', 'CRYPTO', 'MIXED')),
  symbol TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  edge_score NUMERIC(8, 4) NOT NULL,
  expected_net_edge NUMERIC(12, 6),
  distance_to_entry NUMERIC(12, 6),
  expected_net_edge_bps NUMERIC(12, 6),
  distance_to_entry_bps NUMERIC(12, 6),
  confidence NUMERIC(8, 4),
  risk_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  risk JSONB NOT NULL DEFAULT '{}'::jsonb,
  explain TEXT,
  market_regime TEXT,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (strategy_name, market_scope, symbol, observed_at)
);

ALTER TABLE edge_states
  ADD COLUMN IF NOT EXISTS expected_net_edge NUMERIC(12, 6);

ALTER TABLE edge_states
  ADD COLUMN IF NOT EXISTS distance_to_entry NUMERIC(12, 6);

ALTER TABLE edge_states
  ADD COLUMN IF NOT EXISTS risk_json JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE edge_states
  ADD COLUMN IF NOT EXISTS market_regime TEXT;

UPDATE edge_states
SET expected_net_edge = COALESCE(expected_net_edge, expected_net_edge_bps),
    distance_to_entry = COALESCE(distance_to_entry, distance_to_entry_bps),
    risk_json = CASE
        WHEN risk_json = '{}'::jsonb AND COALESCE(risk, '{}'::jsonb) <> '{}'::jsonb THEN risk
        ELSE risk_json
    END,
    market_regime = COALESCE(market_regime, market_scope)
WHERE (expected_net_edge IS NULL AND expected_net_edge_bps IS NOT NULL)
   OR (distance_to_entry IS NULL AND distance_to_entry_bps IS NOT NULL)
   OR market_regime IS NULL
   OR (risk_json = '{}'::jsonb AND COALESCE(risk, '{}'::jsonb) <> '{}'::jsonb);

CREATE INDEX IF NOT EXISTS idx_edge_states_scope_time
  ON edge_states (market_scope, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_edge_states_strategy_time
  ON edge_states (strategy_name, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_edge_states_strategy_version_time
  ON edge_states (strategy_version_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_edge_states_observed_at
  ON edge_states (observed_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_edge_states_strategy_version_symbol_observed
  ON edge_states (strategy_version_id, symbol, observed_at)
  WHERE strategy_version_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS ideas (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_type TEXT NOT NULL,
  source_url TEXT,
  title TEXT NOT NULL,
  raw_text TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'new',
  priority INTEGER NOT NULL DEFAULT 100 CHECK (priority >= 0 AND priority <= 1000),
  created_by TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ideas_status_priority_created
  ON ideas (status, priority, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ideas_source_url
  ON ideas (source_type, source_url)
  WHERE source_url IS NOT NULL;

CREATE TABLE IF NOT EXISTS idea_evidence (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idea_id UUID NOT NULL REFERENCES ideas(id) ON DELETE CASCADE,
  doc_version_id UUID NOT NULL REFERENCES document_versions(id) ON DELETE RESTRICT,
  excerpt TEXT NOT NULL,
  locator JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_idea_evidence_idea_time
  ON idea_evidence (idea_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_idea_evidence_doc
  ON idea_evidence (doc_version_id);

CREATE TABLE IF NOT EXISTS experiments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idea_id UUID NOT NULL REFERENCES ideas(id) ON DELETE CASCADE,
  strategy_version_id UUID REFERENCES strategy_versions(id) ON DELETE SET NULL,
  hypothesis TEXT NOT NULL,
  eval_status TEXT NOT NULL DEFAULT 'queued',
  metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
  artifacts JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_experiments_idea_status_time
  ON experiments (idea_id, eval_status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_experiments_strategy_time
  ON experiments (strategy_version_id, created_at DESC);

CREATE TABLE IF NOT EXISTS lessons (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idea_id UUID NOT NULL REFERENCES ideas(id) ON DELETE CASCADE,
  experiment_id UUID REFERENCES experiments(id) ON DELETE SET NULL,
  lesson_type TEXT NOT NULL,
  summary TEXT NOT NULL,
  reusable_checklist JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lessons_idea_time
  ON lessons (idea_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_lessons_experiment
  ON lessons (experiment_id);

CREATE TABLE IF NOT EXISTS strategy_risk_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  strategy_version_id UUID NOT NULL REFERENCES strategy_versions(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  triggered_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_risk_events_strategy_time
  ON strategy_risk_events (strategy_version_id, triggered_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_risk_events_type_time
  ON strategy_risk_events (event_type, triggered_at DESC);

CREATE TABLE IF NOT EXISTS strategy_risk_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  strategy_version_id UUID NOT NULL REFERENCES strategy_versions(id) ON DELETE CASCADE,
  as_of TIMESTAMPTZ NOT NULL,
  as_of_date DATE NOT NULL,
  drawdown NUMERIC(10, 6),
  sharpe_20d NUMERIC(10, 6),
  state TEXT NOT NULL CHECK (state IN ('normal', 'warning', 'halted', 'cooldown')),
  trigger_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
  cooldown_until TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (strategy_version_id, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_strategy_risk_snapshots_strategy_time
  ON strategy_risk_snapshots (strategy_version_id, as_of DESC);

CREATE TABLE IF NOT EXISTS portfolios (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL UNIQUE,
  base_currency TEXT NOT NULL,
  broker_map JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_intents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  portfolio_id UUID NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
  strategy_version_id UUID REFERENCES strategy_versions(id) ON DELETE SET NULL,
  as_of TIMESTAMPTZ NOT NULL,
  target_positions JSONB NOT NULL,
  reason TEXT,
  risk_checks JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL CHECK (status IN ('proposed', 'approved', 'rejected', 'sent', 'executing', 'done', 'failed', 'canceled')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  approved_at TIMESTAMPTZ,
  approved_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_order_intents_status_time
  ON order_intents (status, created_at DESC);

CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  intent_id UUID REFERENCES order_intents(id) ON DELETE SET NULL,
  broker TEXT NOT NULL,
  account_id TEXT,
  symbol TEXT NOT NULL,
  instrument_type TEXT NOT NULL CHECK (instrument_type IN ('JP_EQ', 'US_EQ', 'CRYPTO', 'FUT', 'FX', 'ETF')),
  side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL', 'SELL_SHORT', 'BUY_TO_COVER')),
  order_type TEXT NOT NULL CHECK (order_type IN ('MKT', 'LMT', 'STOP', 'STP_LMT')),
  qty NUMERIC(18, 8) NOT NULL,
  limit_price NUMERIC(18, 8),
  stop_price NUMERIC(18, 8),
  time_in_force TEXT NOT NULL DEFAULT 'DAY',
  status TEXT NOT NULL CHECK (status IN ('new', 'sent', 'ack', 'partially_filled', 'filled', 'canceled', 'rejected', 'expired', 'error')),
  broker_order_id TEXT,
  idempotency_key TEXT NOT NULL,
  submitted_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (broker, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_orders_status_time ON orders (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS order_fills (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  fill_time TIMESTAMPTZ NOT NULL,
  qty NUMERIC(18, 8) NOT NULL,
  price NUMERIC(18, 8) NOT NULL,
  fee NUMERIC(18, 8) NOT NULL DEFAULT 0,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_order_fills_time ON order_fills (fill_time DESC);

CREATE TABLE IF NOT EXISTS positions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  portfolio_id UUID NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  instrument_type TEXT NOT NULL CHECK (instrument_type IN ('JP_EQ', 'US_EQ', 'CRYPTO', 'FUT', 'FX', 'ETF')),
  qty NUMERIC(18, 8) NOT NULL,
  avg_price NUMERIC(18, 8),
  last_price NUMERIC(18, 8),
  market_value NUMERIC(18, 8),
  unrealized_pnl NUMERIC(18, 8),
  realized_pnl NUMERIC(18, 8),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (portfolio_id, symbol, instrument_type)
);

CREATE INDEX IF NOT EXISTS idx_positions_portfolio_symbol ON positions (portfolio_id, symbol);

CREATE TABLE IF NOT EXISTS risk_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  portfolio_id UUID NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
  as_of TIMESTAMPTZ NOT NULL,
  equity NUMERIC(18, 8) NOT NULL,
  drawdown NUMERIC(10, 6) NOT NULL,
  sharpe_20d NUMERIC(10, 6),
  gross_exposure NUMERIC(18, 8),
  net_exposure NUMERIC(18, 8),
  state TEXT NOT NULL CHECK (state IN ('normal', 'risk_alert', 'halted')),
  triggers JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (portfolio_id, as_of)
);

CREATE INDEX IF NOT EXISTS idx_risk_snapshots_portfolio_time
  ON risk_snapshots (portfolio_id, as_of DESC);

CREATE TABLE IF NOT EXISTS agent_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  task_type TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 100,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'success', 'failed', 'canceled')),
  payload JSONB NOT NULL,
  result JSONB NOT NULL DEFAULT '{}'::jsonb,
  cost_usd NUMERIC(12, 4),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_tasks_queue ON agent_tasks (status, priority, created_at);

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS session_id UUID REFERENCES chat_sessions(id);

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS parent_task_id UUID REFERENCES agent_tasks(id);

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS lease_owner TEXT;

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 3;

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS dedupe_key TEXT;

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS error_text TEXT;

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS assigned_role TEXT;

ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS assigned_node TEXT;

CREATE INDEX IF NOT EXISTS idx_agent_tasks_session ON agent_tasks (session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_tasks_lease ON agent_tasks (status, available_at, lease_expires_at);

CREATE TABLE IF NOT EXISTS fundamental_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  security_id UUID NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  as_of_date DATE NOT NULL,
  source TEXT NOT NULL DEFAULT 'llm',
  rating TEXT NOT NULL CHECK (rating IN ('A', 'B', 'C')),
  confidence TEXT CHECK (confidence IN ('High', 'Medium', 'Low')),
  summary TEXT NOT NULL,
  snapshot JSONB NOT NULL,
  created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (security_id, as_of_date, source)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_snapshots_time
  ON fundamental_snapshots (as_of_date DESC, rating);
