import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import { getMetricColor } from "../../utils/common.js";

const LABEL_MAX_WIDTH = 250; // y-axis labels are truncated beyond this (full name stays in the tooltip)

export function ComponentsBarHorizontal({ id, data, t, i18n, nf, custom, height }) {
  const result = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const labelPrefix = cfg.label_prefix ?? "page.content";
    const pct = new Intl.NumberFormat(i18n.language, {
      style: "percent",
      maximumFractionDigits: 1,
    });

    // Computed before the top N cut, so percentages refer to the whole dataset
    const total = data.reduce((sum, d) => sum + d.count, 0);

    // Sort a copy so the incoming props are never mutated, then apply the top N
    const sorted =
      cfg.sort === false
        ? [...data]
        : [...data].sort((a, b) => b.count - a.count);
    const rows = cfg.limit ? sorted.slice(0, cfg.limit) : sorted;

    const items = rows.map((d) => ({
      name: t(`${labelPrefix}.${d.metric}`, d.metric),
      value: d.count,
      // Color is looked up with the raw key so it stays stable across languages
      itemStyle: {
        color: getMetricColor(
          custom?.colors_metrics ?? {},
          d.metric,
          cfg.default_colors,
        ),
      },
    }));

    const option = {
      aria: { enabled: true },
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: {
          // x-axis zoom only; opt-in to keep the toolbox light
          ...(cfg.zoomable && { dataZoom: { xAxisIndex: "none" } }),
          saveAsImage: {},
        },
      },
      // containLabel lets the grid adapt to long y-axis labels; top leaves room for the toolbox
      grid: { top: 32, left: 8, right: 32, bottom: 48, containLabel: true },
      tooltip: {
        trigger: "axis",
        confine: true,
        axisPointer: { type: "shadow" },
        formatter: (params) => {
          const p = params[0];
          const share = cfg.show_percent
            ? ` (${pct.format(p.value / total)})`
            : "";
          return `${p.marker} ${p.name}<br/><b>${nf.format(p.value)}</b> ${t(
            cfg.tooltip ?? "page.tooltip_text",
          )}${share}`;
        },
      },
      yAxis: {
        type: "category",
        data: items.map((d) => d.name),
        inverse: true, // first item (the biggest) is drawn at the top
        axisTick: { show: false },
        axisLabel: {
          interval: 0, // never silently drop a category label
          width: LABEL_MAX_WIDTH,
          overflow: "truncate",
          fontSize: 13,
          color: "#fff",
        },
      },
      xAxis: {
        type: "value",
        name: t(cfg.axis_text ?? "page.axes_text"),
        nameLocation: "middle",
        nameGap: 32,
        nameTextStyle: { fontSize: 14, color: "#fff" },
        axisLabel: {
          formatter: (v) => nf.format(v),
          fontSize: 14,
          color: "#fff",
        },
        splitLine: { lineStyle: { color: "rgba(255,255,255,0.12)" } },
        boundaryGap: [0, "8%"], // leaves room for the value label at the end of the longest bar
      },
      series: [
        {
          type: "bar",
          itemStyle: { borderRadius: [0, 4, 4, 0] },
          label: {
            show: true,
            position: "right", // valid position for horizontal bars (no "outside")
            fontSize: 11,
            color: "#fff",
            formatter: (p) => nf.format(p.value),
          },
          data: items,
        },
      ],
    };

    return {
      option,
      height: height || cfg.height || 400,
    };
  }, [data, t, i18n.language, nf, id, custom, height]);

  if (!result) return null;

  return (
    <ReactECharts
      option={result.option}
      style={{ width: "100%", height: result.height }}
    />
  );
}
