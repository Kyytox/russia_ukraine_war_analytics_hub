import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const LABEL_MAX_WIDTH = 150; // x-axis labels are truncated beyond this (full name stays in the tooltip)

export function ComponentsBarVertical({
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
    const labelPrefix = cfg.label_prefix ?? "page.content";
    const pct = new Intl.NumberFormat(i18n.language, {
      style: "percent",
      maximumFractionDigits: 1,
    });
    const total = data.reduce((sum, d) => sum + d.value, 0);

    const items = data.map((d) => ({
      name: t(`${labelPrefix}.${d.metric}`, d.metric),
      value: d.value,
      itemStyle: {
        color: custom?.colors_metrics?.[d.metric] ?? cfg.default_colors,
      },
    }));

    return {
      aria: { enabled: true },
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
      },
      grid: {
        top: 48,
        left: "6%",
        right: "6%",
        bottom: "5%",
        containLabel: true,
      },
      tooltip: {
        trigger: "axis",
        // confine: true,
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
      xAxis: {
        type: "category",
        data: items.map((d) => d.name),
        axisTick: { show: false },
        axisLabel: {
          interval: 0, // never silently drop a category label
          rotate: cfg.xAxisRotate,
          width: LABEL_MAX_WIDTH,
          overflow: "truncate",
          fontSize: 13,
          color: "#fff",
        },
      },
      yAxis: {
        type: "value",
        name: t(cfg.axis_text ?? "page.axes_text"),
        nameLocation: "end",
        nameTextStyle: { fontSize: 14, color: "#fff" },
        axisLabel: {
          formatter: (v) => nf.format(v),
          fontSize: 14,
          color: "#fff",
        },
        splitLine: { lineStyle: { color: "rgba(255,255,255,0.12)" } },
      },
      series: [
        {
          type: "bar",
          itemStyle: { borderRadius: [4, 4, 0, 0] },
          label: {
            show: true,
            position: "top", // always above the bar: readable whatever the bar height
            fontSize: 11,
            color: "#fff",
            formatter: (p) => nf.format(p.value),
          },
          data: items,
        },
      ],
    };
  }, [data, t, i18n.language, nf, id, custom]);

  if (!option) return null;

  // Prioriser : prop height > custom[id].height > 400 (défaut)
  const chartHeight = height || custom?.[id]?.height || 400;

  return (
    <ReactECharts
      option={option}
      style={{
        width: "100%",
        height: chartHeight,
        alignSelf: "center",
      }}
    />
  );
}
