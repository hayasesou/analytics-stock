"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import * as echarts from "echarts/core";
import { BarChart, LineChart } from "echarts/charts";
import {
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

import { ResearchStrategy, ResearchValidationFold } from "@/lib/types";
import { TermHelp } from "@/components/TermHelp";

echarts.use([
  BarChart,
  LineChart,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  CanvasRenderer
]);

type MetricKey = "sharpe" | "cagr" | "maxDd" | "tradeCount";
type ViewMode = "all_profiles" | "fixed_profile";

type Props = {
  strategies: ResearchStrategy[];
};

const METRIC_OPTIONS: { value: MetricKey; label: string }[] = [
  { value: "sharpe", label: "Sharpe" },
  { value: "cagr", label: "CAGR" },
  { value: "maxDd", label: "MaxDD" },
  { value: "tradeCount", label: "TradeCount" }
];

const PANEL_HELP = {
  fold: [
    { label: "定義", text: "学習期間(train)と評価期間(test)の1区切りです。" },
    { label: "計算元", text: "validation policy の train_days / test_days / step_days。" },
    { label: "解釈", text: "時系列の複数区間で頑健性を確認します。" },
    { label: "注意点", text: "日次価格の点ではなく fold 単位の点です。" }
  ],
  signalCount: [
    { label: "定義", text: "各 fold test 区間で生成されたシグナル件数です。" },
    { label: "計算元", text: "validation.folds[].signal_count。" },
    { label: "解釈", text: "少なすぎると統計的に不安定です。" },
    { label: "注意点", text: "閾値未満の fold は skipped/fail になります。" }
  ],
  skipped: [
    { label: "定義", text: "その fold を評価対象から除外した状態です。" },
    { label: "計算元", text: "validation.folds[].skipped / skip_reason。" },
    { label: "解釈", text: "データ不足やシグナル不足で発生します。" },
    { label: "注意点", text: "グラフ上は null 扱いで線が途切れます。" }
  ]
} as const;

function metricValue(fold: ResearchValidationFold, profile: string, metric: MetricKey): number | null {
  const profileMetrics = fold.profiles[profile];
  if (!profileMetrics) {
    return null;
  }
  if (metric === "sharpe") {
    return profileMetrics.sharpe;
  }
  if (metric === "cagr") {
    return profileMetrics.cagr;
  }
  if (metric === "maxDd") {
    return profileMetrics.maxDd;
  }
  return profileMetrics.tradeCount;
}

function formatMetric(v: number, metric: MetricKey): string {
  if (metric === "cagr" || metric === "maxDd") {
    return `${(v * 100).toFixed(2)}%`;
  }
  if (metric === "tradeCount") {
    return `${Math.round(v)}`;
  }
  return v.toFixed(4);
}

function profileOrderKey(profile: string): number {
  if (profile === "strict") {
    return 0;
  }
  if (profile === "standard") {
    return 1;
  }
  if (profile === "zero") {
    return 2;
  }
  return 10;
}

function seriesColor(profile: string, metric: MetricKey): string {
  if (metric === "maxDd") {
    if (profile === "strict") {
      return "#9b1c1c";
    }
    if (profile === "standard") {
      return "#b63b3b";
    }
    if (profile === "zero") {
      return "#d45f5f";
    }
    return "#c73f3f";
  }
  if (profile === "strict") {
    return "#2f7d59";
  }
  if (profile === "standard") {
    return "#21689e";
  }
  if (profile === "zero") {
    return "#8b6f1f";
  }
  return "#5c6570";
}

function isFailFold(
  fold: ResearchValidationFold,
  profile: string,
  metric: MetricKey,
  gates: ResearchStrategy["validationGates"]
): boolean {
  if (fold.skipped) {
    return true;
  }
  const v = metricValue(fold, profile, metric);
  if (v == null) {
    return true;
  }
  if (!gates) {
    return false;
  }
  if (metric === "sharpe" && gates.minSharpe != null) {
    return v < gates.minSharpe;
  }
  if (metric === "cagr" && gates.minCagr != null) {
    return v < gates.minCagr;
  }
  if (metric === "maxDd" && gates.minMaxDd != null) {
    return v < gates.minMaxDd;
  }
  if (metric === "tradeCount" && gates.minTradesPerFold != null) {
    return v < gates.minTradesPerFold;
  }
  return false;
}

export function ResearchFoldValidationPanel({ strategies }: Props) {
  const chartRef = useRef<echarts.ECharts | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const available = useMemo(
    () => strategies.filter((s) => s.validationFolds.length > 0),
    [strategies]
  );

  const defaultStrategyId = available[0]?.strategyId ?? "";
  const [selectedStrategyId, setSelectedStrategyId] = useState(defaultStrategyId);
  const [selectedMetric, setSelectedMetric] = useState<MetricKey>("sharpe");
  const [viewMode, setViewMode] = useState<ViewMode>("all_profiles");
  const [selectedProfile, setSelectedProfile] = useState<string>("");

  useEffect(() => {
    if (!available.some((s) => s.strategyId === selectedStrategyId)) {
      setSelectedStrategyId(defaultStrategyId);
    }
  }, [available, defaultStrategyId, selectedStrategyId]);

  const selectedStrategy = useMemo(
    () => available.find((s) => s.strategyId === selectedStrategyId) ?? null,
    [available, selectedStrategyId]
  );

  const folds = selectedStrategy?.validationFolds ?? [];

  const profileNames = useMemo(() => {
    const names = new Set<string>();
    for (const fold of folds) {
      for (const profile of Object.keys(fold.profiles)) {
        names.add(profile);
      }
    }
    return Array.from(names).sort((a, b) => {
      const ka = profileOrderKey(a);
      const kb = profileOrderKey(b);
      if (ka !== kb) {
        return ka - kb;
      }
      return a.localeCompare(b);
    });
  }, [folds]);

  useEffect(() => {
    const preferred = selectedStrategy?.validationPrimaryProfile || "";
    if ((!selectedProfile || !profileNames.includes(selectedProfile)) && preferred && profileNames.includes(preferred)) {
      setSelectedProfile(preferred);
      return;
    }
    if (!profileNames.includes(selectedProfile)) {
      setSelectedProfile(profileNames[0] ?? "");
    }
  }, [profileNames, selectedProfile, selectedStrategy?.validationPrimaryProfile]);

  const failProfile = selectedProfile || selectedStrategy?.validationPrimaryProfile || "standard";
  const failFlags = useMemo(
    () => folds.map((fold) => isFailFold(fold, failProfile, selectedMetric, selectedStrategy?.validationGates ?? null)),
    [failProfile, folds, selectedMetric, selectedStrategy?.validationGates]
  );
  const failCount = failFlags.filter(Boolean).length;

  const profilesForSeries = useMemo(() => {
    if (viewMode === "fixed_profile") {
      return selectedProfile ? [selectedProfile] : [];
    }
    return profileNames;
  }, [profileNames, selectedProfile, viewMode]);

  useEffect(() => {
    if (!containerRef.current || !selectedStrategy || folds.length === 0) {
      return;
    }
    const chart = chartRef.current ?? echarts.init(containerRef.current);
    chartRef.current = chart;

    const xData = folds.map((f) => `F${f.fold}`);
    const lineSeries = profilesForSeries.map((profile) => ({
      name: `${profile} ${selectedMetric}`,
      type: "line",
      yAxisIndex: 0,
      showSymbol: true,
      connectNulls: false,
      lineStyle: {
        width: 2,
        color: seriesColor(profile, selectedMetric)
      },
      data: folds.map((fold) => {
        if (fold.skipped) {
          return null;
        }
        return metricValue(fold, profile, selectedMetric);
      })
    }));

    const signalSeries = {
      name: "signal_count",
      type: "bar",
      yAxisIndex: 1,
      data: folds.map((f) => f.signalCount),
      itemStyle: {
        color: (params: { dataIndex: number }) =>
          failFlags[params.dataIndex] ? "rgba(185, 41, 41, 0.52)" : "rgba(45, 123, 164, 0.35)"
      },
      barMaxWidth: 28
    };

    chart.setOption(
      {
        grid: { left: 42, right: 56, top: 44, bottom: 56 },
        legend: { top: 8 },
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "cross" },
          formatter: (params: unknown) => {
            const rows = Array.isArray(params) ? params : [params];
            const idx = Number(rows[0]?.dataIndex ?? 0);
            const fold = folds[idx];
            if (!fold) {
              return "";
            }
            const lines: string[] = [
              `<strong>Fold ${fold.fold}</strong>`,
              `${fold.testStart} → ${fold.testEnd}`,
              `signals: ${fold.signalCount}`,
              `status: ${failFlags[idx] ? "fail" : "pass"}`,
              fold.skipped ? `skipped: ${fold.skipReason ?? "true"}` : "skipped: false"
            ];
            for (const profile of profilesForSeries) {
              const v = metricValue(fold, profile, selectedMetric);
              if (v == null) {
                continue;
              }
              lines.push(`${profile} ${selectedMetric}: ${formatMetric(v, selectedMetric)}`);
            }
            return lines.join("<br/>");
          }
        },
        xAxis: {
          type: "category",
          data: xData,
          boundaryGap: true
        },
        yAxis: [
          {
            type: "value",
            scale: true,
            name: selectedMetric,
            axisLabel: selectedMetric === "cagr" || selectedMetric === "maxDd"
              ? { formatter: (v: number) => `${(v * 100).toFixed(0)}%` }
              : undefined
          },
          {
            type: "value",
            scale: true,
            name: "signals",
            position: "right"
          }
        ],
        dataZoom: [
          { type: "inside", xAxisIndex: 0 },
          { type: "slider", xAxisIndex: 0, height: 18, bottom: 10 }
        ],
        series: [...lineSeries, signalSeries]
      },
      { notMerge: true }
    );

    const onResize = () => chart.resize();
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
    };
  }, [selectedStrategy, folds, profilesForSeries, selectedMetric, failFlags]);

  useEffect(() => {
    return () => {
      chartRef.current?.dispose();
      chartRef.current = null;
    };
  }, []);

  if (available.length === 0) {
    return <p>fold検証データがありません。</p>;
  }

  return (
    <div className="grid" style={{ gap: 10 }}>
      <div className="grid three" style={{ alignItems: "end", gap: 8 }}>
        <div className="grid" style={{ gap: 6 }}>
          <label htmlFor="validationStrategy">Strategy</label>
          <select
            id="validationStrategy"
            value={selectedStrategyId}
            onChange={(e) => setSelectedStrategyId(e.target.value)}
          >
            {available.map((s) => (
              <option key={s.strategyId} value={s.strategyId}>
                {s.strategyName}
              </option>
            ))}
          </select>
        </div>
        <div className="grid" style={{ gap: 6 }}>
          <label htmlFor="validationMetric">Metric</label>
          <select
            id="validationMetric"
            value={selectedMetric}
            onChange={(e) => setSelectedMetric(e.target.value as MetricKey)}
          >
            {METRIC_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </div>
        <div className="grid" style={{ gap: 6 }}>
          <label htmlFor="validationViewMode">View</label>
          <select
            id="validationViewMode"
            value={viewMode}
            onChange={(e) => setViewMode(e.target.value as ViewMode)}
          >
            <option value="all_profiles">All profiles</option>
            <option value="fixed_profile">Fixed profile</option>
          </select>
        </div>
        <div className="grid" style={{ gap: 6 }}>
          <label htmlFor="validationProfile">Profile</label>
          <select
            id="validationProfile"
            value={selectedProfile}
            onChange={(e) => setSelectedProfile(e.target.value)}
            disabled={profileNames.length === 0}
          >
            {profileNames.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="hint-line">
        このグラフは日次価格の折れ線ではなく、walk-forward の fold 単位評価です。
        <TermHelp
          term="Fold Graph"
          sections={[
            { label: "定義", text: "横軸は時系列日付ではなく Fold 番号（F0, F1...）です。" },
            { label: "計算元", text: "validation.folds の各区間メトリクスを表示しています。" },
            { label: "解釈", text: "モデルの頑健性を区間ごとに比較できます。" },
            { label: "注意点", text: "細かい価格推移を見る用途ではありません。" }
          ]}
        />
      </div>
      <div className="mono hint-line" style={{ fontSize: 12 }}>
        fail folds: {failCount} / {folds.length} (profile={failProfile}, metric={selectedMetric})
        <TermHelp
          term="fail folds"
          sections={[
            { label: "定義", text: "ゲート未達または skipped の fold 数です。" },
            { label: "計算元", text: "選択profile/metric と validation_gates を比較して判定します。" },
            { label: "解釈", text: "多いほどその条件で安定性が低い可能性があります。" },
            { label: "注意点", text: "metric を切り替えると fail 数も変化します。" }
          ]}
        />
      </div>
      <div className="timeline-chart-wrap" ref={containerRef} style={{ height: 420 }} />

      {selectedStrategy ? (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>
                  <span className="term-head">
                    Fold
                    <TermHelp term="Fold" sections={PANEL_HELP.fold} />
                  </span>
                </th>
                <th>Train</th>
                <th>Test</th>
                <th>
                  <span className="term-head">
                    Signals
                    <TermHelp term="Signals" sections={PANEL_HELP.signalCount} />
                  </span>
                </th>
                <th>Status</th>
                <th>
                  <span className="term-head">
                    Skipped
                    <TermHelp term="Skipped" sections={PANEL_HELP.skipped} />
                  </span>
                </th>
                {profileNames.map((profile) => (
                  <th key={profile}>{profile}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {folds.map((fold, idx) => (
                <tr
                  key={`${selectedStrategy.strategyId}-${fold.fold}`}
                  style={failFlags[idx] ? { background: "rgba(185, 41, 41, 0.08)" } : undefined}
                >
                  <td>{fold.fold}</td>
                  <td>{fold.trainStart} → {fold.trainEnd}</td>
                  <td>{fold.testStart} → {fold.testEnd}</td>
                  <td>{fold.signalCount}</td>
                  <td>{failFlags[idx] ? "fail" : "pass"}</td>
                  <td>{fold.skipped ? (fold.skipReason ?? "true") : "false"}</td>
                  {profileNames.map((profile) => {
                    const v = metricValue(fold, profile, selectedMetric);
                    return <td key={`${fold.fold}-${profile}`}>{v == null ? "-" : formatMetric(v, selectedMetric)}</td>;
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}
