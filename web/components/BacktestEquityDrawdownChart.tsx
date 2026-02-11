"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import * as echarts from "echarts/core";
import { LineChart } from "echarts/charts";
import {
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

import { BacktestPoint } from "@/lib/types";

echarts.use([
  LineChart,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  CanvasRenderer
]);

type Props = {
  curve: BacktestPoint[];
};

function pct(v: number): string {
  return `${(v * 100).toFixed(2)}%`;
}

export function BacktestEquityDrawdownChart({ curve }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<echarts.ECharts | null>(null);

  const profiles = useMemo(
    () => Array.from(new Set(curve.map((p) => p.costProfile))).sort(),
    [curve]
  );
  const defaultProfile = profiles.includes("strict") ? "strict" : profiles[0] ?? "";
  const [selected, setSelected] = useState(defaultProfile);

  useEffect(() => {
    if (!profiles.includes(selected)) {
      setSelected(defaultProfile);
    }
  }, [profiles, selected, defaultProfile]);

  const selectedCurve = useMemo(
    () => curve.filter((p) => p.costProfile === selected),
    [curve, selected]
  );

  useEffect(() => {
    if (!containerRef.current || selectedCurve.length === 0) {
      return;
    }

    const chart = chartRef.current ?? echarts.init(containerRef.current);
    chartRef.current = chart;

    chart.setOption(
      {
        grid: { left: 40, right: 46, top: 40, bottom: 56 },
        legend: { top: 8 },
        tooltip: {
          trigger: "axis",
          formatter: (params: unknown) => {
            const rows = Array.isArray(params) ? params : [params];
            const date = String(rows[0]?.axisValueLabel ?? rows[0]?.axisValue ?? "");
            const lines = [`<strong>${date}</strong>`];
            for (const row of rows) {
              const seriesName = String(row.seriesName ?? "");
              const val = Array.isArray(row.value) ? Number(row.value[1]) : Number(row.value);
              if (Number.isNaN(val)) {
                continue;
              }
              if (seriesName === "Drawdown") {
                lines.push(`${seriesName}: ${pct(val)}`);
              } else {
                lines.push(`${seriesName}: ${val.toFixed(4)}`);
              }
            }
            return lines.join("<br/>");
          }
        },
        xAxis: {
          type: "category",
          data: selectedCurve.map((p) => p.tradeDate),
          boundaryGap: false
        },
        yAxis: [
          {
            type: "value",
            scale: true,
            name: "Equity",
            position: "left"
          },
          {
            type: "value",
            name: "DD",
            position: "right",
            min: -1,
            max: 0,
            axisLabel: {
              formatter: (v: number) => `${(v * 100).toFixed(0)}%`
            }
          }
        ],
        dataZoom: [
          { type: "inside", xAxisIndex: 0 },
          { type: "slider", xAxisIndex: 0, height: 18, bottom: 10 }
        ],
        series: [
          {
            name: "Equity",
            type: "line",
            yAxisIndex: 0,
            data: selectedCurve.map((p) => p.equity),
            showSymbol: false,
            lineStyle: { width: 2, color: "#2c7d59" }
          },
          {
            name: "Benchmark",
            type: "line",
            yAxisIndex: 0,
            data: selectedCurve.map((p) => p.benchmarkEquity),
            showSymbol: false,
            lineStyle: { width: 1.5, type: "dashed", color: "#5c7c90" }
          },
          {
            name: "Drawdown",
            type: "line",
            yAxisIndex: 1,
            data: selectedCurve.map((p) => p.drawdown),
            showSymbol: false,
            lineStyle: { width: 1.2, color: "#a93131" },
            areaStyle: {
              color: "rgba(169, 49, 49, 0.22)"
            }
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
  }, [selectedCurve]);

  useEffect(() => {
    return () => {
      chartRef.current?.dispose();
      chartRef.current = null;
    };
  }, []);

  if (curve.length === 0 || profiles.length === 0) {
    return <p>バックテスト時系列がまだありません。</p>;
  }

  return (
    <div className="grid" style={{ gap: 10 }}>
      <div className="grid two" style={{ alignItems: "center", gap: 8 }}>
        <div className="mono">Cost Profile</div>
        <div>
          <select
            value={selected}
            onChange={(e) => setSelected(e.target.value)}
            aria-label="cost profile"
          >
            {profiles.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="timeline-chart-wrap" ref={containerRef} style={{ height: 390 }} />
    </div>
  );
}
