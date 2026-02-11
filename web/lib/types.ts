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
