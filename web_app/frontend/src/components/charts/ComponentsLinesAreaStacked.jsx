import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsLinesAreaStacked({ id, data, t, i18n, nf, custom, height }) {
  const option = useMemo(() => {
    // Flat data: [{ libelle, metric, value }]
    const categories = [];
    const catIndex = new Map();
    const seriesMap = new Map(); // metric -> values array

    for (const row of data) {
      if (!catIndex.has(row.libelle)) {
        catIndex.set(row.libelle, categories.length);
        categories.push(row.libelle);
      }
      if (!seriesMap.has(row.metric)) {
        seriesMap.set(row.metric, []);
      }
    }

    for (const row of data) {
      const idx = catIndex.get(row.libelle);
      for (const [, values] of seriesMap) {
        values[idx] ??= null; // fill gaps
      }
      seriesMap.get(row.metric)[idx] = row.value;
    }

    // optional custom ordering of series, e.g. ["Total", "Sabotage"]
    const seriesNames = [...seriesMap.keys()];
    const colors = custom?.colors_metrics;

    return {
      backgroundColor: "transparent",
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: {
          dataZoom: { yAxisIndex: "none" },
        },
      },
      grid: {
        left: "3%",
        right: "3%",
        bottom: "3%",
        containLabel: true,
      },
      tooltip: {
        trigger: "axis",
        axisPointer: {
          type: "line",
          lineStyle: { color: "#fff" },
        },
        valueFormatter: (value) => nf.format(value),
        color: "#fff",
      },

      xAxis: {
        type: "category",
        boundaryGap: false,
        data: categories,
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
        nameTextStyle: { fontSize: 14, color: "#fff" },
        axisLabel: {
          formatter: (value) => nf.format(value),
          fontSize: 14,
          color: "#fff",
        },
        splitLine: { lineStyle: { color: "rgba(255,255,255,0.15)" } },
      },
      series: seriesNames.map((name, idx) => ({
        name,
        type: "line",
        stack: "total",
        symbolSize: 5,
        lineStyle: { width: 2 },
        itemStyle: { color: colors[name] },
        areaStyle: { opacity: 0.75 },
        label: {
          show: true,
          position: "top",
          color: "#fff",
          fontSize: 11,
        },
        emphasis: { focus: "series" },
        data: seriesMap.get(name),
      })),
    };
  }, [data, t, i18n.language, nf, id, custom]);

  return (
    <ReactECharts
      option={option}
      style={{
        width: "100%",
        height: height || custom?.[id]?.height || 400,
        alignSelf: "center",
      }}
      theme={custom?.theme || "dark"}
    />
  );
}
