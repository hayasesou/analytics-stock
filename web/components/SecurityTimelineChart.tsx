"use client";

import { useEffect, useMemo, useRef } from "react";
import * as echarts from "echarts/core";
import { ScatterChart, LineChart } from "echarts/charts";
import {
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

import {
  SecurityTimelineEvent,
  SecurityTimelinePrice,
  SecurityTimelineSignal
} from "@/lib/types";

echarts.use([
  ScatterChart,
  LineChart,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  CanvasRenderer
]);

type Props = {
  prices: SecurityTimelinePrice[];
  signals: SecurityTimelineSignal[];
  events: SecurityTimelineEvent[];
};

type ChartRow = {
  date: string;
  close: number;
};

type SignalDot = {
  date: string;
  y: number;
  reason: string | null;
  entryAllowed: boolean;
  confidence: SecurityTimelineSignal["confidence"];
};

type EventDot = {
  date: string;
  y: number;
  title: string;
  importance: "high" | "medium" | "low";
  eventType: string;
};

function toShortDate(input: string): string {
  const date = new Date(input);
  if (Number.isNaN(date.getTime())) {
    return input;
  }
  return date.toISOString().slice(0, 10);
}

function resolvePriceAtDate(rows: ChartRow[], targetDate: string): number | null {
  if (rows.length === 0) {
    return null;
  }
  let candidate = rows[0].close;
  for (const row of rows) {
    if (row.date <= targetDate) {
      candidate = row.close;
      continue;
    }
    break;
  }
  return candidate;
}

function formatSignalReason(reason: string | null): string {
  if (reason === "confidence_high_and_top10") {
    return "HighかつTop10";
  }
  if (reason === "risk_alert_mode_entry_cap") {
    return "リスク警戒モード上限";
  }
  if (reason === "confidence_not_high") {
    return "Confidence不足";
  }
  if (reason === "rank_outside_top10") {
    return "順位条件外";
  }
  return reason ?? "-";
}

export function SecurityTimelineChart({ prices, signals, events }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<echarts.ECharts | null>(null);

  const baseRows = useMemo<ChartRow[]>(
    () =>
      [...prices]
        .sort((a, b) => a.date.localeCompare(b.date))
        .map((p) => ({
          date: p.date,
          close: p.close
        })),
    [prices]
  );

  const signalDots = useMemo<SignalDot[]>(
    () =>
      signals
        .map((s) => {
          const y = resolvePriceAtDate(baseRows, s.date);
          if (y == null) {
            return null;
          }
          return {
            date: s.date,
            y,
            reason: s.reason,
            entryAllowed: s.entryAllowed,
            confidence: s.confidence
          };
        })
        .filter((v): v is SignalDot => v != null),
    [baseRows, signals]
  );

  const eventDots = useMemo<EventDot[]>(
    () =>
      events
        .map((e) => {
          const date = toShortDate(e.date || e.eventTime);
          const y = resolvePriceAtDate(baseRows, date);
          if (y == null) {
            return null;
          }
          return {
            date,
            y,
            title: e.title,
            importance: e.importance,
            eventType: e.eventType
          };
        })
        .filter((v): v is EventDot => v != null),
    [baseRows, events]
  );

  useEffect(() => {
    if (!containerRef.current || baseRows.length === 0) {
      return;
    }

    const chart = chartRef.current ?? echarts.init(containerRef.current);
    chartRef.current = chart;

    const closeMap = new Map<string, number>();
    for (const row of baseRows) {
      closeMap.set(row.date, row.close);
    }
    const signalMap = new Map<string, SignalDot[]>();
    for (const row of signalDots) {
      const group = signalMap.get(row.date) ?? [];
      group.push(row);
      signalMap.set(row.date, group);
    }
    const eventMap = new Map<string, EventDot[]>();
    for (const row of eventDots) {
      const group = eventMap.get(row.date) ?? [];
      group.push(row);
      eventMap.set(row.date, group);
    }

    chart.setOption(
      {
        grid: { left: 36, right: 20, top: 40, bottom: 56 },
        legend: { top: 8 },
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "cross" },
          formatter: (params: unknown) => {
            const rows = Array.isArray(params) ? params : [params];
            const date = String(rows[0]?.axisValueLabel ?? rows[0]?.axisValue ?? "");
            const close = closeMap.get(date);
            const lines: string[] = [
              `<strong>${date}</strong>`,
              `Close: ${close == null ? "-" : close.toFixed(2)}`
            ];
            const daySignals = signalMap.get(date) ?? [];
            if (daySignals.length > 0) {
              lines.push("<strong>Signal</strong>");
              for (const s of daySignals) {
                lines.push(
                  `${s.entryAllowed ? "Entry候補" : "Signal"} / ${formatSignalReason(s.reason)} / ${s.confidence ?? "-"}`
                );
              }
            }
            const dayEvents = eventMap.get(date) ?? [];
            if (dayEvents.length > 0) {
              lines.push("<strong>Event</strong>");
              for (const e of dayEvents) {
                lines.push(`${e.title} (${e.eventType})`);
              }
            }
            return lines.join("<br/>");
          }
        },
        xAxis: {
          type: "category",
          data: baseRows.map((r) => r.date),
          boundaryGap: false
        },
        yAxis: {
          type: "value",
          scale: true
        },
        dataZoom: [
          { type: "inside", xAxisIndex: 0 },
          { type: "slider", xAxisIndex: 0, height: 18, bottom: 10 }
        ],
        series: [
          {
            name: "Price",
            type: "line",
            data: baseRows.map((r) => r.close),
            showSymbol: false,
            lineStyle: { width: 2, color: "#2c7d59" }
          },
          {
            name: "Signals",
            type: "scatter",
            data: signalDots.map((s) => [s.date, s.y]),
            symbolSize: 10,
            itemStyle: { color: "#d35400" }
          },
          {
            name: "Events",
            type: "scatter",
            data: eventDots.map((e) => [e.date, e.y]),
            symbolSize: 9,
            itemStyle: { color: "#0f5ea8" }
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
  }, [baseRows, signalDots, eventDots]);

  useEffect(() => {
    return () => {
      chartRef.current?.dispose();
      chartRef.current = null;
    };
  }, []);

  if (baseRows.length === 0) {
    return <p>価格データがないため、タイムラインを表示できません。</p>;
  }

  return <div className="timeline-chart-wrap" ref={containerRef} style={{ height: 380 }} />;
}
