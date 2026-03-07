"use client";

import { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import { BarChart, LineChart, ScatterChart } from "echarts/charts";
import { GridComponent, TooltipComponent, LegendComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([LineChart, BarChart, ScatterChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer]);

type ChartPoint = [string, number];

type ChartSeries = {
  name: string;
  data: ChartPoint[];
};

type ChartSpec = {
  title?: string;
  kind?: "line" | "bar" | "scatter" | "area";
  xAxisLabel?: string;
  yAxisLabel?: string;
  series: ChartSeries[];
};

export function ResearchArtifactChart({ spec }: { spec: ChartSpec }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<echarts.ECharts | null>(null);

  useEffect(() => {
    if (!containerRef.current || !Array.isArray(spec.series) || spec.series.length === 0) {
      return;
    }

    const chart = chartRef.current ?? echarts.init(containerRef.current);
    chartRef.current = chart;
    const categories = spec.series[0]?.data.map((point) => point[0]) ?? [];

    chart.setOption(
      {
        grid: { left: 44, right: 24, top: 40, bottom: 40 },
        legend: { top: 8 },
        tooltip: { trigger: "axis" },
        xAxis: {
          type: "category",
          data: categories,
          boundaryGap: false,
          name: spec.xAxisLabel || undefined,
        },
        yAxis: {
          type: "value",
          scale: true,
          name: spec.yAxisLabel || undefined,
        },
        series: spec.series.map((series) => ({
          name: series.name,
          type: spec.kind === "scatter" ? "scatter" : spec.kind === "bar" ? "bar" : "line",
          data: series.data.map((point) => point[1]),
          showSymbol: spec.kind === "scatter",
          smooth: false,
          areaStyle: spec.kind === "area" ? {} : undefined,
        })),
      },
      { notMerge: true }
    );

    const onResize = () => chart.resize();
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, [spec]);

  useEffect(() => {
    return () => {
      chartRef.current?.dispose();
      chartRef.current = null;
    };
  }, []);

  return <div className="timeline-chart-wrap" ref={containerRef} style={{ height: 320 }} />;
}
