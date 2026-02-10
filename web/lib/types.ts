export type Top50Row = {
  rank: number;
  securityId: string;
  market: "JP" | "US";
  ticker: string;
  name: string;
  sector: string | null;
  score: number;
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
  benchmarkEquity: number | null;
};
