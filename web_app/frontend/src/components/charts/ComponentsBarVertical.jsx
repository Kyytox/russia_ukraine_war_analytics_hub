import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsBarVertical({ id, data, t, i18n, nf, custom }) {
  const option = useMemo(() => {
    const items = data.map((d) => ({
      name: t(`page.content.${d.metric}`, d.metric),
      value: d.count,
    }));

    const max = Math.max(...items.map((d) => d.value), 1);
    const MIN_RATIO = 0.08;

    return {
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: {
          dataZoom: { yAxisIndex: "none" },
          saveAsImage: {},
        },
      },
      grid: {
        left: "6%",
        right: "6%",
        bottom: "5%",
        containLabel: true,
      },

      tooltip: {
        trigger: "axis",
        axisPointer: {
          type: "shadow",
        },
        valueFormatter: (value) => nf.format(value),
      },
      xAxis: {
        type: "category",
        data: items.map((d) => d.name),
        axisLabel: {
          text: "test",
          interval: 0,
          rotate: custom?.[id]?.xAxisRotate || 40,
          fontSize: 13,
          color: "#fff",
        },
      },
      yAxis: {
        type: "value",
        name: t(`page.axes_text`),
        nameLocation: "end",
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
            color: custom?.[id]?.default_colors,
            borderRadius: [4, 4, 0, 0],
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
            // bar too small, display label outside
            ...(d.value / max < MIN_RATIO && {
              label: {
                position: "top",
                color: "#fff",
              },
            }),
          })),
        },
      ],
    };
  }, [data, t, i18n.language, nf, id]);

  return (
    <ReactECharts
      option={option}
      style={{
        width: "100%",
        height: custom?.[id]?.height,
        alignSelf: "center",
      }}
    />
  );
}
