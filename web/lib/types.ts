export type Top50Row = {
  rank: number;
  rankPrev: number | null;
  rankDelta: number | null;
  securityId: string;
  market: "JP" | "US";
  ticker: string;
  name: string;
  sector: string | null;
  score: number;
  scoreDelta: number | null;
  edgeScore: number;
  quality: number;
  growth: number;
  value: number;
  momentum: number;
  catalyst: number;
  missingRatio: number;
  liquidityFlag: boolean;
  selectionReason: string | null;
  signalReason: string | null;
  confidence: "High" | "Medium" | "Low";
  isSignal: boolean;
  entryAllowed: boolean;
  validUntil: string | null;
};

export type SecurityIdentity = {
  securityId: string;
  market: "JP" | "US";
  ticker: string;
  name: string;
};

export type ReportRecord = {
  id: string;
  securityId: string | null;
  reportType: string;
  title: string;
  bodyMd: string;
  conclusion: string | null;
  falsificationConditions: string | null;
  confidence: string | null;
  createdAt: string;
};

export type CitationRecord = {
  claimId: string;
  docVersionId: string;
  pageRef: string | null;
  quoteText: string;
};

export type EventRecord = {
  id: string;
  importance: "high" | "medium" | "low";
  eventType: string;
  eventTime: string;
  title: string;
  summary: string;
  sourceUrl: string | null;
};

export type BacktestMetric = {
  costProfile: "zero" | "standard" | "strict";
  cagr: number;
  maxDd: number;
  sharpe: number;
  sortino: number;
  volatility: number;
  winRate: number;
  avgWin: number;
  avgLoss: number;
  alphaSimple: number;
  informationRatioSimple: number;
};

export type BacktestPoint = {
  costProfile: string;
  tradeDate: string;
  equity: number;
  drawdown: number;
  benchmarkEquity: number | null;
};

export type BacktestReasonCode =
  | "ok"
  | "no_signals"
  | "no_weekly_run"
  | "requested_run_not_found"
  | "requested_run_has_no_backtest"
  | "latest_weekly_has_no_backtest"
  | "no_backtest_run"
  | "no_metrics"
  | "no_curve";

export type BacktestRunOption = {
  runId: string;
  status: string;
  startedAt: string;
  finishedAt: string | null;
  signals: number | null;
  backtestProfiles: number | null;
  hasBacktestRun: boolean;
};

export type BacktestMeta = {
  requestedRunId: string | null;
  resolvedRunId: string | null;
  latestWeeklyRunId: string | null;
  latestWithBacktestRunId: string | null;
  resolvedSource: "requested" | "latest_weekly" | "latest_with_backtest" | "none";
  reasonCode: BacktestReasonCode;
  resolvedRunStatus: string | null;
  resolvedRunStartedAt: string | null;
  resolvedRunFinishedAt: string | null;
  resolvedRunSignals: number | null;
  resolvedRunBacktestProfiles: number | null;
};

export type BacktestData = {
  metrics: BacktestMetric[];
  curve: BacktestPoint[];
  meta: BacktestMeta;
};

export type SecurityTimelinePrice = {
  date: string;
  close: number;
};

export type SecurityTimelineSignal = {
  date: string;
  isSignal: boolean;
  entryAllowed: boolean;
  reason: string | null;
  rank: number | null;
  confidence: "High" | "Medium" | "Low" | null;
  validUntil: string | null;
};

export type SecurityTimelineEvent = {
  date: string;
  eventTime: string;
  title: string;
  summary: string;
  importance: "high" | "medium" | "low";
  eventType: string;
  sourceUrl: string | null;
};

export type SecurityTimelineData = {
  securityId: string;
  days: number;
  prices: SecurityTimelinePrice[];
  signals: SecurityTimelineSignal[];
  events: SecurityTimelineEvent[];
};

export type WeeklyHighConfidenceTop10Item = {
  rank: number;
  securityId: string;
  ticker: string;
  name: string;
  market: "JP" | "US";
};

export type WeeklyLiquidityChange = {
  securityId: string;
  ticker: string;
  name: string;
  market: "JP" | "US";
  previousLiquidityFlag: boolean;
  currentLiquidityFlag: boolean;
};

export type WeeklyStrictMetric = {
  cagr: number;
  maxDd: number;
  sharpe: number;
} | null;

export type WeeklySignalDiagnostic = {
  horizonDays: 5 | 20 | 60;
  hitRate: number;
  medianReturn: number | null;
  p10Return: number | null;
  p90Return: number | null;
  sampleSize: number;
};

export type WeeklyActionData = {
  latestRunId: string | null;
  previousRunId: string | null;
  highConfidenceTop10: WeeklyHighConfidenceTop10Item[];
  liquidityChanges: WeeklyLiquidityChange[];
  strictMetric: WeeklyStrictMetric;
  signalDiagnostics: WeeklySignalDiagnostic[];
};

export type ExecutionOrderIntent = {
  intentId: string;
  portfolioId: string;
  portfolioName: string;
  strategyVersionId: string | null;
  asOf: string;
  status: "proposed" | "approved" | "rejected" | "sent" | "executing" | "done" | "failed" | "canceled";
  reason: string | null;
  riskChecks: Record<string, unknown>;
  targetPositions: unknown[];
  createdAt: string;
  approvedAt: string | null;
  approvedBy: string | null;
};

export type ExecutionRiskSnapshot = {
  portfolioId: string;
  portfolioName: string;
  asOf: string;
  equity: number;
  drawdown: number;
  sharpe20d: number | null;
  grossExposure: number | null;
  netExposure: number | null;
  state: "normal" | "risk_alert" | "halted";
  triggers: Record<string, unknown>;
  createdAt: string;
};

export type ResearchStrategy = {
  strategyId: string;
  strategyName: string;
  assetScope: "JP_EQ" | "US_EQ" | "CRYPTO" | "MIXED";
  status: "draft" | "candidate" | "approved" | "paper" | "live" | "paused" | "retired";
  updatedAt: string;
  versionId: string | null;
  version: number | null;
  evalRunId: string | null;
  evalType: "quick_backtest" | "robust_backtest" | "paper" | "live" | null;
  sharpe: number | null;
  maxDd: number | null;
  cagr: number | null;
  validationPassed: boolean | null;
  validationFoldCount: number | null;
  validationPrimaryProfile: string | null;
  foldSharpeFirst: number | null;
  foldSharpeLast: number | null;
  foldSharpeDelta: number | null;
  foldSharpeMin: number | null;
  foldSharpeMax: number | null;
  validationFailReasons: string[];
  validationFolds: ResearchValidationFold[];
  validationGates: ResearchValidationGates | null;
};

export type ResearchValidationGates = {
  minFoldCount: number | null;
  minTradesPerFold: number | null;
  minSharpe: number | null;
  minCagr: number | null;
  minMaxDd: number | null;
};

export type ResearchValidationFoldProfile = {
  sharpe: number | null;
  cagr: number | null;
  maxDd: number | null;
  tradeCount: number | null;
};

export type ResearchValidationFold = {
  fold: number;
  trainStart: string;
  trainEnd: string;
  testStart: string;
  testEnd: string;
  signalCount: number;
  momentumThreshold: number | null;
  skipped: boolean;
  skipReason: string | null;
  profiles: Record<string, ResearchValidationFoldProfile>;
};

export type ResearchFundamentalSnapshot = {
  securityId: string;
  ticker: string;
  name: string;
  market: "JP" | "US";
  source: string;
  asOfDate: string;
  rating: "A" | "B" | "C";
  confidence: "High" | "Medium" | "Low" | null;
  summary: string;
  createdAt: string;
};

export type ResearchAgentTask = {
  id: string;
  taskType: string;
  priority: number;
  status: "queued" | "running" | "success" | "failed" | "canceled";
  strategyName: string | null;
  securityId: string | null;
  costUsd: number | null;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
};
