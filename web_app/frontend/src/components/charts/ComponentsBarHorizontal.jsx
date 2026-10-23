import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import { getMetricColor } from "../../utils/common.js";

export function ComponentsBarHorizontal({ id, data, t, i18n, nf, custom }) {
  const option = useMemo(() => {
    const items = data
      .map((d) => ({
        name: t(`page.content.${d.metric}`, d.metric),
        value: d.count,
      }))
      .reverse()
      .sort((a, b) => a.value - b.value)
      .slice(-custom?.[id]?.limit);

    console.log("ComponentsBarHorizontal", id, items);

    return {
      toolbox: {
        right: 30,
        top: -10,
        iconStyle: { borderColor: "#fff" },
        feature: {
          dataZoom: { xAxisIndex: "none" },
          saveAsImage: {},
        },
      },
      grid: {
        bottom: "6%",
        top: "8%",
        left: "6%",
        right: "6%",
      },
      tooltip: {
        trigger: "axis",
        axisPointer: {
          type: "shadow",
        },
        valueFormatter: (value) =>
          nf.format(value) + " " + t(`page.tooltip_text`),
      },
      yAxis: {
        type: "category",
        data: items.map((d) => d.name),
        axisLabel: {
          fontSize: 13,
          color: "#fff",
        },
      },
      xAxis: {
        type: "value",
        name: t(`page.axes_text`),
        nameLocation: "middle",
        nameTextStyle: {
          fontSize: 14,
          color: "#fff",
        },
        axisLabel: {
          formatter: (value) => nf.format(value),
          fontSize: 14,
          color: "#fff",
        },
      },
      series: [
        {
          type: "bar",
          itemStyle: {
            borderRadius: [0, 4, 4, 0],
          },
          label: {
            show: true,
            position: "outside",
            fontSize: 11,
            color: "#fff",
            formatter: (p) => nf.format(p.value),
          },
          data: items.map((d) => ({
            value: d.value,
            itemStyle: {
              color: getMetricColor(
                custom.colors_metrics,
                d.name,
                custom[id].default_colors,
              ),
            },
          })),
        },
      ],
    };
  }, [data, t, i18n.language, nf, id]);

  return (
    <ReactECharts
      option={option}
      style={{ width: "100%", height: custom?.[id]?.height }}
    />
  );
}
