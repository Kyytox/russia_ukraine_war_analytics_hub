import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

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

    // Get chart configuration for the given id
    const cfg = custom?.[id] ?? {};

    // Formatter for percentage
    const pct = new Intl.NumberFormat(i18n.language, {
      style: "percent",
      maximumFractionDigits: 1,
    });

    // Calc Total for tooltip percentage
    const total = data.reduce((sum, d) => sum + d.value, 0);

    // Map data to chart items
    const items = data.map((d) => ({
      name: t(`${d.metric}`, d.metric),
      value: d.value,
      itemStyle: {
        color: custom?.colors_metrics?.[d.metric] ?? cfg.default_colors,
      },
    }));

    return {
      aria: { enabled: true },
      grid: {
        top: "10%",
        left: "4%",
        right: "4%",
        bottom: "5%",
        containLabel: true,
      },
      tooltip: {
        trigger: "axis",
        confine: true,
        axisPointer: { type: "shadow" },
        formatter: (params) => {
          const p = params[0];
          const share = ` (${pct.format(p.value / total)})`;
          return `${p.marker} ${p.name}<br/><b>${nf.format(p.value)}</b> ${t("page.tooltip_text")}${share}`;
        },
      },
      xAxis: {
        type: "category",
        data: items.map((d) => d.name),
        axisTick: { show: false },
        axisLabel: {
          interval: 0, // never silently drop a category label
          rotate: cfg.xAxisRotate,
          width: custom?.common?.labelMaxLength,
          overflow: "truncate",
          fontSize: 13,
          color: "#fff",
        },
      },
      yAxis: {
        type: "value",
        name: t(cfg.axis_text ?? "page.axes_text"),
        nameLocation: "end",
        nameTextStyle: custom?.common?.nameTextStyle,
        axisLabel: {
          formatter: (v) => nf.format(v),
          fontSize: 14,
          color: "#fff",
        },
        splitLine: custom?.common?.splitLine,
      },
      series: [
        {
          type: "bar",
          itemStyle: { borderRadius: [4, 4, 0, 0] },
          label: {
            show: true,
            position: "top",
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

  // Determine chart height:
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
