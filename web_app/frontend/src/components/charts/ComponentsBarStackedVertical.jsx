import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import { getMetricColor } from "../../utils/common.js";

const TOTAL = "__total__"; // invisible series that carries the total above each bar
const OTHERS = "__others__"; // bucket for the smallest metrics
const OTHERS_COLOR = "#6b7280"; // neutral grey, distinct from the background and from any metric color
const DEFAULT_MAX_METRICS = 10;
const MIN_LABEL_RATIO = 0.02; // segment < 5 % of the tallest bar: no label inside

export function ComponentsBarStackedVertical({
  id,
  data,
  t,
  i18n,
  nf,
  custom,
  height,
}) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const labelPrefix = cfg.label_prefix ?? "page.chart-names";
    const metricPrefix = cfg.metric_prefix ?? "metric";
    const maxMetrics = cfg.max_metrics ?? DEFAULT_MAX_METRICS;
    const pct = new Intl.NumberFormat(i18n.language, {
      style: "percent",
      maximumFractionDigits: 0,
    });

    // 1. Total per bar, then keep the top N (descending: biggest bar on the left)
    const totals = {};
    data.forEach((d) => {
      totals[d.libelle] = (totals[d.libelle] ?? 0) + d.value;
    });
    const keys = Object.keys(totals);
    const libelles = cfg.limit ? keys.slice(0, cfg.limit) : keys;
    const kept = new Set(libelles);

    // 2. Total per metric, restricted to the kept bars, to rank the legend
    const metricTotals = {};
    data.forEach((d) => {
      if (kept.has(d.libelle)) {
        metricTotals[d.metric] = (metricTotals[d.metric] ?? 0) + d.value;
      }
    });
    const ranked = Object.keys(metricTotals).sort(
      (a, b) => metricTotals[b] - metricTotals[a],
    );
    const main = ranked.slice(0, maxMetrics);
    const mainSet = new Set(main);
    const metrics = ranked.length > maxMetrics ? [...main, OTHERS] : main;

    // 3. Lookup `${bar}|${metric}` -> value; minor metrics are summed into OTHERS
    //    and remembered per bar so the tooltip can still list them
    const lookup = new Map();
    const othersDetail = {};
    data.forEach((d) => {
      if (!kept.has(d.libelle)) return;
      const metric = mainSet.has(d.metric) ? d.metric : OTHERS;
      const key = `${d.libelle}|${metric}`;
      lookup.set(key, (lookup.get(key) ?? 0) + d.value);
      if (metric === OTHERS) {
        if (!othersDetail[d.libelle]) othersDetail[d.libelle] = [];
        othersDetail[d.libelle].push({ metric: d.metric, value: d.value });
      }
    });

    const max = Math.max(...libelles.map((l) => totals[l]), 1);
    const metricName = (m) =>
      m === OTHERS ? t("page.others", "Others") : t(`${metricPrefix}.${m}`, m);
    const metricColor = (m) =>
      m === OTHERS
        ? OTHERS_COLOR
        : getMetricColor(custom?.colors_metrics ?? {}, m, cfg.default_colors);

    console.log("ComponentsBarStackedVertical", cfg);

    return {
      aria: { enabled: true },
      tooltip: {
        trigger: "axis",
        confine: true,
        axisPointer: { type: "shadow" },
        formatter: (params) => {
          const key = libelles[params[0].dataIndex];
          const sum = totals[key];
          const header = `<b>${params[0].name}</b><br/>${nf.format(sum)} ${t(
            "page.tooltip_text",
          )}`;

          const lines = [];
          params
            .filter((p) => p.seriesName !== TOTAL && p.value > 0)
            .forEach((p) => {
              // seriesName is already translated: no second call to t()
              lines.push(
                `${p.marker} ${p.seriesName} : <b>${nf.format(p.value)}</b> (${pct.format(p.value / sum)})`,
              );
              // list the metrics hidden inside "Others" (top 5 only)
              if (p.seriesId === OTHERS) {
                const detail = [...(othersDetail[key] ?? [])].sort(
                  (a, b) => b.value - a.value,
                );
                detail.slice(0, 5).forEach((x) => {
                  lines.push(
                    `&nbsp;&nbsp;&nbsp;· ${t(`${metricPrefix}.${x.metric}`, x.metric)} : ${nf.format(x.value)}`,
                  );
                });
                if (detail.length > 5) lines.push("&nbsp;&nbsp;&nbsp;…");
              }
            });

          return [header, ...lines].join("<br/>");
        },
      },
      legend: {
        top: 0,
        left: "center",
        textStyle: { fontSize: 13, color: "#fff" },
        data: metrics.map(metricName),
      },
      grid: {
        top: 56,
        left: 8,
        right: 16,
        bottom: 8,
        containLabel: true,
      },
      xAxis: {
        type: "category",
        data: libelles.map((l) => t(`${labelPrefix}.${l}`, l)),
        axisTick: { show: false },
        axisLabel: {
          interval: 0, // never silently drop a label
          rotate: cfg.axis_rotate,
          width: custom?.common?.labelMaxLength,
          overflow: "truncate",
          fontSize: 12,
          color: "#fff",
        },
      },
      yAxis: {
        type: "value",
        name: t("page.axes_text"),
        nameLocation: "middle",
        nameGap: 56,
        nameTextStyle: { fontSize: 14, color: "#fff" },
        axisLabel: {
          formatter: (v) => nf.format(v),
          fontSize: 14,
          color: "#fff",
        },
        boundaryGap: [0, "8%"], // leaves room for the total label above the tallest bar
      },
      series: [
        ...metrics.map((m) => ({
          id: m, // exposed as seriesId in the tooltip
          name: metricName(m),
          type: "bar",
          stack: "total",
          emphasis: { focus: "series" },
          itemStyle: { color: metricColor(m) },
          label: {
            show: true,
            fontSize: 11,
            color: "#fff",
            formatter: (p) =>
              p.value / max < MIN_LABEL_RATIO ? "" : nf.format(p.value),
          },
          data: libelles.map((l) => lookup.get(`${l}|${m}`) ?? 0),
        })),
        {
          // zero-height series stacked last: only used to display the bar total above it
          id: TOTAL,
          name: TOTAL,
          type: "bar",
          stack: "total",
          silent: true,
          itemStyle: { color: "transparent" },
          data: libelles.map(() => 0),
          label: {
            show: cfg.show_totals ?? true,
            position: "top", // valid position for vertical bars (no "outside")
            color: "#fff",
            fontSize: 12,
            formatter: (p) => nf.format(totals[libelles[p.dataIndex]]),
          },
        },
      ],
    };
  }, [data, t, i18n.language, nf, id, custom]);

  if (!option) return null;

  return (
    <ReactECharts
      option={option}
      style={{ width: "100%", height: height || custom?.[id]?.height }}
    />
  );
}
