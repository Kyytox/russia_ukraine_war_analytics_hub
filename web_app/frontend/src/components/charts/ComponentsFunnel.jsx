import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsFunnel({ id, data, t, i18n, nf, custom, height }) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const labelPrefix = "page.content";
    const pct = new Intl.NumberFormat(i18n.language, {
      style: "percent",
      maximumFractionDigits: 1,
    });

    const total = data.reduce((sum, d) => sum + d.value, 0);

    const items = [...data]
      .sort((a, b) => b.value - a.value)
      .map((d) => ({
        name: t(`${labelPrefix}.${d.metric}`, d.metric),
        value: d.value,
        itemStyle: {
          color: custom?.colors_metrics?.[d.metric],
        },
      }));

    return {
      aria: { enabled: true },
      grid: {
        top: "5%",
        left: "6%",
        right: "6%",
        bottom: "5%",
        containLabel: true,
      },
      tooltip: {
        trigger: "item",
        confine: true,
        formatter: (params) => {
          const share =
            cfg.show_percent && total !== 0
              ? ` (${pct.format(params.value / total)})`
              : "";
          return `${params.marker} ${params.name}<br/><b>${nf.format(params.value)}</b> ${t(cfg.tooltip ?? "page.tooltip_text")}${share}`;
        },
      },
      series: [
        {
          type: "funnel",
          left: "10%",
          right: "10%",
          label: {
            show: true,
            position: "inside",
            fontSize: 12,
            color: "#fff",
            formatter: cfg.show_percent
              ? (p) => `${p.name}\n${pct.format(p.value / total)}`
              : (p) => `${p.name}\n${nf.format(p.value)}`,
          },
          labelLine: { show: false },
          itemStyle: {
            borderColor: "#fff",
            borderWidth: 1,
            color: (params) => items[params.dataIndex].itemStyle.color,
          },
          data: items,
        },
      ],
    };
  }, [data, t, i18n.language, nf, id, custom]);

  console.log("ComponentsFunnel custom", custom);

  if (!option) return null;
  const chartHeight = height || custom?.[id]?.height;

  return (
    <ReactECharts
      option={option}
      style={{ width: "100%", height: chartHeight, alignSelf: "center" }}
    />
  );
}
