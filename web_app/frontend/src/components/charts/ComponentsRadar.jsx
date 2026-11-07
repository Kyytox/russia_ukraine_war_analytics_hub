import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsRadar({ id, data, t, i18n, nf, custom, height }) {
  const option = useMemo(() => {
    if (!data?.length) return null;
    const cfg = custom?.[id] ?? {};
    const labelPrefix = cfg.label_prefix ?? "page.content";

    const maxValue = Math.max(...data.map((d) => d.value)) * 1.2;

    return {
      grid: {
        top: "5%",
        left: "5%",
        right: "5%",
        bottom: "5%",
      },
      tooltip: {
        trigger: "item",
        confine: true,
        formatter: (params) => {
          return `${params.marker} ${params.name}<br/><b>${nf.format(
            params.value,
          )}</b> ${t(cfg.tooltip ?? "page.tooltip_text")}`;
        },
      },
      radar: {
        indicator: data.map((d) => ({
          name: t(`${labelPrefix}.${d.metric}`, d.metric),
          max: maxValue,
          color: "#fff",
          axisLabel: { fontSize: 12 },
        })),
        axisLine: custom?.common?.splitLine,
        splitLine: custom?.common?.splitLine,
        splitArea: { areaStyle: { color: ["rgba(255,255,255,0.05)"] } },
      },
      series: [
        {
          type: "radar",
          data: [
            {
              value: data.map((d) => d.value),
              name: t("page.values"),
              itemStyle: { color: cfg.default_colors ?? "#4CAF50" },
              lineStyle: { color: cfg.default_colors ?? "#4CAF50", width: 2 },
              // areaStyle: {
              //   color: cfg.default_colors ?? "rgba(76, 175, 80, 0.3)",
              // },
            },
          ],
          label: {
            show: true,
            formatter: (p) => nf.format(p.value),
            color: "#fff",
          },
        },
      ],
    };
  }, [data, t, id, custom]);

  if (!option) return null;
  const chartHeight = height || custom?.[id]?.height;

  return (
    <ReactECharts
      option={option}
      style={{ width: "100%", height: chartHeight, alignSelf: "center" }}
    />
  );
}
