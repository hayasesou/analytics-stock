"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import * as echarts from "echarts/core";
import { LineChart, ScatterChart } from "echarts/charts";
import {
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

import type { ExecutionOrderIntent, ExecutionRiskSnapshot } from "@/lib/types";

echarts.use([
  LineChart,
  ScatterChart,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  CanvasRenderer
]);

type Props = {
  intents: ExecutionOrderIntent[];
  risks: ExecutionRiskSnapshot[];
};

function fmtPct(value: number): string {
  return `${(value * 100).toFixed(2)}%`;
}

function fmtNum(value: number | null): string {
  if (value == null || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(4);
}

function formatJst(ts: string): string {
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) {
    return ts;
  }
  return d.toLocaleString("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
}

function statusClass(status: string): "high" | "medium" | "low" {
  if (status === "done") {
    return "high";
  }
  if (status === "approved" || status === "executing" || status === "sent" || status === "proposed") {
    return "medium";
  }
  return "low";
}

function riskClass(state: string): "high" | "medium" | "low" {
  if (state === "normal") {
    return "high";
  }
  if (state === "risk_alert") {
    return "medium";
  }
  return "low";
}

export function ExecutionDashboard({ intents, risks }: Props) {
  const chartRef = useRef<HTMLDivElement | null>(null);
  const echartsRef = useRef<echarts.ECharts | null>(null);
  const [selectedIntentId, setSelectedIntentId] = useState<string | null>(null);

  const selectedIntent = useMemo(
    () => intents.find((intent) => intent.intentId === selectedIntentId) ?? null,
    [intents, selectedIntentId]
  );

  const portfolios = useMemo(
    () => Array.from(new Set(risks.map((r) => r.portfolioName))).sort(),
    [risks]
  );
  const [selectedPortfolio, setSelectedPortfolio] = useState<string>(portfolios[0] ?? "");

  useEffect(() => {
    if (!portfolios.includes(selectedPortfolio)) {
      setSelectedPortfolio(portfolios[0] ?? "");
    }
  }, [portfolios, selectedPortfolio]);

  const filteredRisks = useMemo(
    () =>
      risks
        .filter((r) => (selectedPortfolio ? r.portfolioName === selectedPortfolio : true))
        .slice()
        .sort((a, b) => a.asOf.localeCompare(b.asOf)),
    [risks, selectedPortfolio]
  );

  useEffect(() => {
    if (!chartRef.current || filteredRisks.length === 0) {
      return;
    }
    const chart = echartsRef.current ?? echarts.init(chartRef.current);
    echartsRef.current = chart;

    const haltedData = filteredRisks
      .filter((r) => r.state === "halted")
      .map((r) => [r.asOf, r.drawdown]);
    const alertData = filteredRisks
      .filter((r) => r.state === "risk_alert")
      .map((r) => [r.asOf, r.drawdown]);

    chart.setOption(
      {
        grid: { left: 40, right: 48, top: 40, bottom: 56 },
        legend: { top: 8 },
        tooltip: {
          trigger: "axis",
          formatter: (params: unknown) => {
            const rows = Array.isArray(params) ? params : [params];
            const ts = String(rows[0]?.axisValueLabel ?? rows[0]?.axisValue ?? "");
            const lines = [`<strong>${formatJst(ts)} JST</strong>`];
            for (const row of rows) {
              const name = String(row.seriesName ?? "");
              const value = Array.isArray(row.value) ? Number(row.value[1]) : Number(row.value);
              if (Number.isNaN(value)) {
                continue;
              }
              if (name.includes("Drawdown")) {
                lines.push(`${name}: ${fmtPct(value)}`);
              } else {
                lines.push(`${name}: ${value.toFixed(2)}`);
              }
            }
            return lines.join("<br/>");
          }
        },
        xAxis: {
          type: "category",
          boundaryGap: false,
          data: filteredRisks.map((r) => r.asOf)
        },
        yAxis: [
          {
            type: "value",
            name: "Drawdown",
            min: -1,
            max: 0,
            axisLabel: {
              formatter: (v: number) => `${(v * 100).toFixed(0)}%`
            }
          },
          {
            type: "value",
            name: "Sharpe20d",
            position: "right"
          }
        ],
        dataZoom: [
          { type: "inside", xAxisIndex: 0 },
          { type: "slider", xAxisIndex: 0, height: 18, bottom: 10 }
        ],
        series: [
          {
            name: "Drawdown",
            type: "line",
            yAxisIndex: 0,
            data: filteredRisks.map((r) => r.drawdown),
            showSymbol: false,
            lineStyle: { width: 2, color: "#a93131" },
            areaStyle: { color: "rgba(169, 49, 49, 0.16)" }
          },
          {
            name: "Sharpe20d",
            type: "line",
            yAxisIndex: 1,
            data: filteredRisks.map((r) => r.sharpe20d),
            showSymbol: false,
            lineStyle: { width: 1.7, color: "#0f7f6d" }
          },
          {
            name: "Risk Alert",
            type: "scatter",
            yAxisIndex: 0,
            data: alertData,
            symbolSize: 9,
            itemStyle: { color: "#c95f2b" }
          },
          {
            name: "Halted",
            type: "scatter",
            yAxisIndex: 0,
            data: haltedData,
            symbolSize: 10,
            itemStyle: { color: "#a93131" }
          }
        ]
      },
      { notMerge: true }
    );

    const onResize = () => {
      chart.resize();
    };
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
    };
  }, [filteredRisks]);

  useEffect(
    () => () => {
      echartsRef.current?.dispose();
      echartsRef.current = null;
    },
    []
  );

  return (
    <>
      <div className="card">
        <h2>Risk時系列チャート（Drawdown / Sharpe20d）</h2>
        {portfolios.length > 1 ? (
          <div className="grid two" style={{ alignItems: "center", marginBottom: 8 }}>
            <div className="mono">Portfolio</div>
            <div>
              <select
                value={selectedPortfolio}
                onChange={(e) => setSelectedPortfolio(e.target.value)}
                aria-label="portfolio"
              >
                {portfolios.map((name) => (
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </select>
            </div>
          </div>
        ) : null}
        {filteredRisks.length === 0 ? (
          <p>チャート表示に必要なriskデータがありません。</p>
        ) : (
          <div className="timeline-chart-wrap" style={{ height: 360 }} ref={chartRef} />
        )}
      </div>

      <div className="card">
        <h2>Intent一覧</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Status</th>
                <th>AsOf</th>
                <th>Portfolio</th>
                <th>StrategyVersion</th>
                <th>Reason</th>
                <th>TargetCount</th>
                <th>Approved</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody>
              {intents.length === 0 ? (
                <tr>
                  <td colSpan={8}>Intentデータがありません。</td>
                </tr>
              ) : (
                intents.map((intent) => (
                  <tr key={intent.intentId}>
                    <td>
                      <span className={`pill ${statusClass(intent.status)}`}>{intent.status}</span>
                    </td>
                    <td>{formatJst(intent.asOf)} JST</td>
                    <td>{intent.portfolioName}</td>
                    <td className="mono">{intent.strategyVersionId ?? "-"}</td>
                    <td>{intent.reason ?? "-"}</td>
                    <td>{intent.targetPositions.length}</td>
                    <td>
                      {intent.approvedAt ? `${formatJst(intent.approvedAt)} / ${intent.approvedBy ?? "-"}` : "-"}
                    </td>
                    <td>
                      <button type="button" onClick={() => setSelectedIntentId(intent.intentId)}>
                        詳細
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card">
        <h2>Risk時系列テーブル</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>State</th>
                <th>AsOf</th>
                <th>Portfolio</th>
                <th>Equity</th>
                <th>Drawdown</th>
                <th>Sharpe20d</th>
                <th>Gross</th>
                <th>Net</th>
                <th>Triggers</th>
              </tr>
            </thead>
            <tbody>
              {risks.length === 0 ? (
                <tr>
                  <td colSpan={9}>Riskスナップショットがありません。</td>
                </tr>
              ) : (
                risks.map((risk, idx) => (
                  <tr key={`${risk.portfolioId}-${risk.asOf}-${idx}`}>
                    <td>
                      <span className={`pill ${riskClass(risk.state)}`}>{risk.state}</span>
                    </td>
                    <td>{formatJst(risk.asOf)} JST</td>
                    <td>{risk.portfolioName}</td>
                    <td>{fmtNum(risk.equity)}</td>
                    <td>{fmtPct(risk.drawdown)}</td>
                    <td>{fmtNum(risk.sharpe20d)}</td>
                    <td>{fmtNum(risk.grossExposure)}</td>
                    <td>{fmtNum(risk.netExposure)}</td>
                    <td className="mono">{JSON.stringify(risk.triggers)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {selectedIntent ? (
        <div className="modal-backdrop" onClick={() => setSelectedIntentId(null)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Intent詳細</h3>
              <button type="button" onClick={() => setSelectedIntentId(null)}>
                閉じる
              </button>
            </div>
            <div className="grid" style={{ gap: 8 }}>
              <div className="mono">intentId: {selectedIntent.intentId}</div>
              <div>status: {selectedIntent.status}</div>
              <div>portfolio: {selectedIntent.portfolioName}</div>
              <div>asOf: {formatJst(selectedIntent.asOf)} JST</div>
              <div>reason: {selectedIntent.reason ?? "-"}</div>
              <div className="modal-section">
                <h4>riskChecks</h4>
                <pre>{JSON.stringify(selectedIntent.riskChecks, null, 2)}</pre>
              </div>
              <div className="modal-section">
                <h4>targetPositions</h4>
                <pre>{JSON.stringify(selectedIntent.targetPositions, null, 2)}</pre>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}
