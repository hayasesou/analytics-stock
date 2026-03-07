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

export type EdgeStateRow = {
  strategyName: string;
  strategyVersionId: string | null;
  strategyStatus: "draft" | "candidate" | "approved" | "paper" | "live" | "paused" | "retired" | null;
  marketScope: "JP_EQ" | "US_EQ" | "CRYPTO" | "MIXED";
  symbol: string;
  observedAt: string;
  edgeScore: number;
  expectedNetEdgeBps: number | null;
  distanceToEntryBps: number | null;
  confidence: number | null;
  marketRegime: string | null;
  explain: string | null;
  riskState: "normal" | "warning" | "halted" | "cooldown" | null;
  riskDrawdown: number | null;
  riskSharpe20d: number | null;
  cooldownUntil: string | null;
  meta: Record<string, unknown>;
};

export type EdgeTrendPoint = {
  strategyName: string;
  strategyVersionId: string | null;
  symbol: string;
  observedAt: string;
  edgeScore: number;
  expectedNetEdgeBps: number | null;
  distanceToEntryBps: number | null;
  confidence: number | null;
  riskState: "normal" | "warning" | "halted" | "cooldown" | null;
};

export type ResearchInputRecord = {
  id: string;
  sessionId: string;
  messageId: string | null;
  sourceType: "discord" | "web" | "x" | "youtube" | "web_url" | "text";
  sourceUrl: string | null;
  rawText: string | null;
  extractedText: string | null;
  qualityGrade: "A" | "B" | "C" | null;
  extractionStatus: "queued" | "success" | "partial" | "failed";
  userComment: string | null;
  metadata: Record<string, unknown>;
  createdAt: string;
};

export type ResearchHypothesisAsset = {
  id: string;
  assetClass: "JP_EQ" | "US_EQ" | "CRYPTO";
  securityId: string | null;
  symbolText: string | null;
  ticker: string | null;
  name: string | null;
  market: "JP" | "US" | null;
  weightHint: number | null;
  confidence: number | null;
};

export type ResearchHypothesisRecord = {
  id: string;
  sessionId: string;
  externalInputId: string | null;
  parentMessageId: string | null;
  stance: "bullish" | "bearish" | "neutral" | "watch";
  horizonDays: number;
  thesisMd: string;
  falsificationMd: string;
  confidence: number | null;
  status: "draft" | "watch" | "validate" | "passed" | "failed" | "archived";
  isFavorite: boolean;
  version: number;
  metadata: Record<string, unknown>;
  createdAt: string;
  assets: ResearchHypothesisAsset[];
};

export type ResearchArtifactRecord = {
  id: string;
  sessionId: string;
  hypothesisId: string | null;
  artifactType: "sql" | "python" | "chart" | "table" | "note" | "report";
  title: string;
  bodyMd: string | null;
  codeText: string | null;
  language: string | null;
  isFavorite: boolean;
  createdByTaskId: string | null;
  metadata: Record<string, unknown>;
  createdAt: string;
  latestRun: {
    id: string;
    runStatus: ResearchArtifactRunStatus;
    stdoutText: string | null;
    stderrText: string | null;
    resultJson: Record<string, unknown>;
    outputR2Key: string | null;
    createdAt: string;
  } | null;
};

export type ResearchArtifactRunStatus = "pending" | "running" | "success" | "failed";

export type ResearchHypothesisOutcomeRecord = {
  id: string;
  hypothesisId: string;
  checkedAt: string;
  ret1d: number | null;
  ret5d: number | null;
  ret20d: number | null;
  mfe: number | null;
  mae: number | null;
  outcomeLabel: "hit" | "miss" | "partial" | "open";
  summaryMd: string | null;
  metadata: Record<string, unknown>;
  hypothesis: Pick<ResearchHypothesisRecord, "stance" | "horizonDays" | "thesisMd" | "confidence" | "status" | "sessionId"> | null;
};

export type ResearchChatMessageRecord = {
  id: string;
  sessionId: string;
  role: "user" | "assistant" | "system";
  content: string;
  answerBefore: string | null;
  answerAfter: string | null;
  changeReason: string | null;
  createdAt: string;
};

export type ResearchChatSessionDetail = {
  sessionId: string;
  title: string | null;
  messages: ResearchChatMessageRecord[];
  inputs: ResearchInputRecord[];
  hypotheses: ResearchHypothesisRecord[];
  artifacts: ResearchArtifactRecord[];
};

export type ResearchSessionListItem = {
  sessionId: string;
  title: string | null;
  createdAt: string;
  messageCount: number;
  inputCount: number;
  hypothesisCount: number;
  latestAssistantMessage: string | null;
};

export type ResearchKanbanStatus = "new" | "analyzing" | "rejected" | "candidate" | "paper" | "live";

export type ResearchKanbanItem = {
  lane: ResearchKanbanStatus;
  itemType: "idea" | "strategy";
  id: string;
  title: string;
  subtitle: string | null;
  updatedAt: string;
};

export type ResearchKanbanLane = {
  lane: ResearchKanbanStatus;
  count: number;
  items: ResearchKanbanItem[];
};

export type ResearchStrategy = {
  strategyId: string;
  strategyName: string;
  assetScope: "JP_EQ" | "US_EQ" | "CRYPTO" | "MIXED";
  status: "draft" | "candidate" | "approved" | "paper" | "live" | "paused" | "retired";
  liveCandidate: boolean;
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
  paperDays: number | null;
  paperRoundTrips: number | null;
  paperSharpe20d: number | null;
  paperMaxDrawdown: number | null;
  paperGateDaysOk: boolean | null;
  paperGateRoundTripsOk: boolean | null;
  paperGateRiskOk: boolean | null;
  lastLifecycleAction: string | null;
  lastLifecycleReason: string | null;
  lastLifecycleBy: string | null;
  lastLifecycleAt: string | null;
  lastLifecycleRecheckAfter: string | null;
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

export type ResearchLifecycleReview = {
  id: string;
  strategyId: string;
  strategyName: string;
  strategyVersionId: string | null;
  action: string;
  fromStatus: string;
  toStatus: string;
  liveCandidate: boolean;
  reason: string | null;
  recheckCondition: string | null;
  recheckAfter: string | null;
  actedBy: string;
  actedAt: string;
};
