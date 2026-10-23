import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsLineArea({ id, data, t, i18n, nf, custom }) {
  const option = useMemo(() => {
    const items = data.map((d) => ({
      name: d.metric,
      value: d.count,
    }));

    const color = custom?.[id]?.colors || "#7F557D";

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
        bottom: "12%",
        containLabel: true,
      },
      tooltip: {
        trigger: "axis",
        axisPointer: {
          type: "line",
          lineStyle: { color: "#fff", opacity: 0.4 },
        },
        valueFormatter: (value) => nf.format(value),
      },

      xAxis: {
        type: "category",
        boundaryGap: false,
        data: items.map((d) => d.name),
        axisLabel: {
          hideOverlap: true,
          rotate: custom?.[id]?.xAxisRotate || 0,
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
        splitLine: {
          lineStyle: { color: "rgba(255,255,255,0.15)" },
        },
      },
      series: [
        {
          type: "line",
          smooth: true,
          showSymbol: false, // évite des milliers de points sur les séries par jour
          symbolSize: 6,
          lineStyle: { width: 2, color },
          itemStyle: { color },
          areaStyle: {},
          emphasis: { focus: "series" },
          data: items.map((d) => d.value),
        },
      ],
    };
  }, [data, t, i18n.language, nf, id, custom]);

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
