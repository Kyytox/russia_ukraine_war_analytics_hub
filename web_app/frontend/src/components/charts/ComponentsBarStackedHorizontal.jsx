import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import { getMetricColor } from "../../utils/common.js";

const TOTAL = "__total__"; // invisible series that carries the total at the end of each bar
const OTHERS = "__others__"; // bucket for the smallest metrics (countries)
const OTHERS_COLOR = "#6b7280"; // neutral grey, distinct from the background and from any country color
const DEFAULT_MAX_METRICS = 17; // metrics kept as their own segment before grouping into "Others"
const MIN_LABEL_RATIO = 0.03; // segment < 3 % of the longest bar: no label inside

export function ComponentsBarStackedHorizontal({
  tab,
  id,
  data,
  t,
  i18n,
  nf,
  custom,
}) {
  const result = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const limit = cfg.limit; // top N bars to display (undefined = all)
    const maxMetrics = cfg.max_metrics ?? DEFAULT_MAX_METRICS;
    const pct = new Intl.NumberFormat(i18n.language, {
      style: "percent",
      maximumFractionDigits: 0,
    });

    // 1. Total per bar, then keep the top N (ascending order: biggest bar ends up on top)
    const totals = {};
    data.forEach((d) => {
      totals[d.libelle] = (totals[d.libelle] ?? 0) + d.value;
    });
    const sorted = Object.keys(totals).sort((a, b) => totals[a] - totals[b]);
    const libelles = limit ? sorted.slice(-limit) : sorted;
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
      m === OTHERS ? t("page.others", "Others") : t(`metrics.${m}`, m);
    const metricColor = (m) =>
      m === OTHERS
        ? OTHERS_COLOR
        : getMetricColor(custom?.colors_metrics ?? {}, m, cfg.default_colors);

    const option = {
      aria: { enabled: true },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        formatter: (params) => {
          const key = libelles[params[0].dataIndex];
          const sum = totals[key];
          const header = `<b>${params[0].name}</b><br/>${nf.format(sum)} ${t(
            `page.${tab}.tooltip_text`,
          )}`;

          const lines = [];
          params
            .filter((p) => p.seriesName !== TOTAL && p.value > 0)
            .forEach((p) => {
              lines.push(
                `${p.marker} ${p.seriesName} : <b>${nf.format(p.value)}</b> (${pct.format(p.value / sum)})`,
              );
              // list the countries hidden inside "Others" (top 5 only)
              if (p.seriesId === OTHERS) {
                const detail = [...(othersDetail[key] ?? [])].sort(
                  (a, b) => b.value - a.value,
                );
                detail.slice(0, 5).forEach((x) => {
                  lines.push(
                    `&nbsp;&nbsp;&nbsp;· ${t(`metrics.${x.metric}`, x.metric)} : ${nf.format(x.value)}`,
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
      grid: { top: 56, left: 8, right: 24, bottom: 60, containLabel: true },
      yAxis: {
        type: "category",
        data: libelles.map((l) => t(`page.content.${l}`, l)),
        axisTick: { show: false },
        axisLabel: {
          fontSize: 13,
          color: "#fff",
          interval: 0, // never silently drop a label
          width: custom?.common?.labelMaxLength,
          overflow: "truncate",
        },
      },
      xAxis: {
        type: "value",
        name: t(`page.${tab}.axes_text`),
        nameLocation: "middle",
        nameGap: 32,
        nameTextStyle: { fontSize: 14, color: "#fff" },
        axisLabel: {
          formatter: (v) => nf.format(v),
          fontSize: 14,
          color: "#fff",
        },
        boundaryGap: [0, "8%"], // leaves room for the total label at the end of the longest bar
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
            textBorderWidth: 2,
            formatter: (p) =>
              p.value / max < MIN_LABEL_RATIO ? "" : nf.format(p.value),
          },
          data: libelles.map((l) => lookup.get(`${l}|${m}`) ?? 0),
        })),
        {
          // zero-width series stacked last: only used to display the bar total on its right
          id: TOTAL,
          name: TOTAL,
          type: "bar",
          stack: "total",
          silent: true,
          itemStyle: { color: "transparent" },
          data: libelles.map(() => 0),
          label: {
            show: true,
            position: "right",
            color: "#fff",
            fontSize: 12,
            formatter: (p) => nf.format(totals[libelles[p.dataIndex]]),
          },
        },
      ],
    };

    return {
      option,
      height: cfg.height,
    };
  }, [data, t, i18n.language, nf, id, custom, tab]);

  if (!result) return null;

  return (
    <ReactECharts
      option={result.option}
      style={{ width: "100%", height: result.height }}
    />
  );
}
