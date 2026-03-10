import type { ResearchStrategy, SecurityIdentity } from "@/lib/types";

export function isUndefinedRelationError(error: unknown, relation: string): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }
  const candidate = error as { code?: string; message?: string };
  if (candidate.code === "42P01") {
    return true;
  }
  return typeof candidate.message === "string" && candidate.message.includes(`relation "${relation}" does not exist`);
}

export function decodeSecurityId(raw: string): string {
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

export function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

export function clampLookbackDays(days: number): number {
  if (!Number.isFinite(days)) {
    return 180;
  }
  return Math.min(3650, Math.max(1, Math.trunc(days)));
}

export function clampLookupLimit(limit: number): number {
  if (!Number.isFinite(limit)) {
    return 5;
  }
  return Math.min(20, Math.max(1, Math.trunc(limit)));
}

export function clampExecutionLimit(limit: number): number {
  if (!Number.isFinite(limit)) {
    return 50;
  }
  return Math.min(200, Math.max(1, Math.trunc(limit)));
}

export function clampBacktestRunLimit(limit: number): number {
  if (!Number.isFinite(limit)) {
    return 20;
  }
  return Math.min(200, Math.max(20, Math.trunc(limit)));
}

export function clampEdgeLimit(limit: number): number {
  if (!Number.isFinite(limit)) {
    return 120;
  }
  return Math.min(500, Math.max(1, Math.trunc(limit)));
}
export function mapSecurityIdentityRow(row: {
  security_id: string;
  market: "JP" | "US";
  ticker: string;
  name: string;
}): SecurityIdentity {
  return {
    securityId: row.security_id,
    market: row.market,
    ticker: row.ticker,
    name: row.name
  };
}

export function isLegacyMockSecurity(row: { market: "JP" | "US"; name: string }): boolean {
  const name = (row.name ?? "").trim();
  if (row.market === "JP") {
    return /^JP Corp \d{4}$/.test(name);
  }
  return /^US Holdings \d+$/.test(name);
}

export function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

export function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

export function asBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

export function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((item): item is string => typeof item === "string");
}

export function extractFoldValidationSummary(
  metricsValue: unknown,
  artifactsValue: unknown
): Pick<
  ResearchStrategy,
  | "validationPassed"
  | "validationFoldCount"
  | "validationPrimaryProfile"
  | "foldSharpeFirst"
  | "foldSharpeLast"
  | "foldSharpeDelta"
  | "foldSharpeMin"
  | "foldSharpeMax"
  | "validationFailReasons"
  | "validationFolds"
  | "validationGates"
> {
  const metrics = asRecord(metricsValue);
  const artifacts = asRecord(artifactsValue);
  const validation = asRecord(artifacts?.validation);
  const gate = asRecord(validation?.gate);
  const summary = asRecord(validation?.summary);
  const policy = asRecord(validation?.policy);
  const gates = asRecord(policy?.gates);

  const primaryProfile =
    asString(metrics?.validation_primary_profile)
    ?? asString(gate?.primary_cost_profile)
    ?? "standard";

  const primarySummary = summary ? asRecord(summary[primaryProfile]) : null;
  const profileFoldCount = asNumber(primarySummary?.fold_count);
  const foldCountFromMetrics = asNumber(metrics?.validation_fold_count);

  const foldsRaw = Array.isArray(validation?.folds) ? validation.folds : [];
  const normalizedFolds: ResearchStrategy["validationFolds"] = [];
  const sharpeSeries: number[] = [];

  for (const foldRaw of foldsRaw) {
    const fold = asRecord(foldRaw);
    if (!fold) {
      continue;
    }
    const profiles = asRecord(fold.profiles);
    const normalizedProfiles: Record<string, { sharpe: number | null; cagr: number | null; maxDd: number | null; tradeCount: number | null }> = {};
    if (profiles) {
      for (const [profileName, rawProfileMetrics] of Object.entries(profiles)) {
        const profileMetrics = asRecord(rawProfileMetrics);
        if (!profileMetrics) {
          continue;
        }
        normalizedProfiles[profileName] = {
          sharpe: asNumber(profileMetrics.sharpe),
          cagr: asNumber(profileMetrics.cagr),
          maxDd: asNumber(profileMetrics.max_dd),
          tradeCount: asNumber(profileMetrics.trade_count)
        };
      }
    }

    const skipped = asBoolean(fold.skipped) ?? false;
    const primarySharpe = normalizedProfiles[primaryProfile]?.sharpe ?? null;
    if (!skipped && primarySharpe != null) {
      sharpeSeries.push(primarySharpe);
    }

    normalizedFolds.push({
      fold: asNumber(fold.fold) ?? normalizedFolds.length,
      trainStart: asString(fold.train_start) ?? "",
      trainEnd: asString(fold.train_end) ?? "",
      testStart: asString(fold.test_start) ?? "",
      testEnd: asString(fold.test_end) ?? "",
      signalCount: asNumber(fold.signal_count) ?? 0,
      momentumThreshold: asNumber(fold.momentum_threshold),
      skipped,
      skipReason: asString(fold.skip_reason),
      profiles: normalizedProfiles
    });
  }

  const foldSharpeFirst = sharpeSeries.length > 0 ? sharpeSeries[0] : null;
  const foldSharpeLast = sharpeSeries.length > 0 ? sharpeSeries[sharpeSeries.length - 1] : null;
  const foldSharpeMin = sharpeSeries.length > 0 ? Math.min(...sharpeSeries) : null;
  const foldSharpeMax = sharpeSeries.length > 0 ? Math.max(...sharpeSeries) : null;
  const foldSharpeDelta = (foldSharpeFirst != null && foldSharpeLast != null)
    ? foldSharpeLast - foldSharpeFirst
    : null;

  const failReasons = asStringArray(metrics?.validation_fail_reasons);
  const gateReasons = asStringArray(gate?.reasons);
  const validationGates = gates
    ? {
        minFoldCount: asNumber(gates.min_fold_count),
        minTradesPerFold: asNumber(gates.min_trades_per_fold),
        minSharpe: asNumber(gates.min_sharpe),
        minCagr: asNumber(gates.min_cagr),
        minMaxDd: asNumber(gates.min_max_dd)
      }
    : null;

  return {
    validationPassed: asBoolean(metrics?.validation_passed) ?? asBoolean(gate?.passed),
    validationFoldCount: foldCountFromMetrics ?? profileFoldCount ?? (sharpeSeries.length || null),
    validationPrimaryProfile: primaryProfile,
    foldSharpeFirst,
    foldSharpeLast,
    foldSharpeDelta,
    foldSharpeMin,
    foldSharpeMax,
    validationFailReasons: failReasons.length > 0 ? failReasons : gateReasons,
    validationFolds: normalizedFolds,
    validationGates
  };
}


export function asObjectRecord(value: unknown): Record<string, unknown> {
  return asRecord(value) ?? {};
}

