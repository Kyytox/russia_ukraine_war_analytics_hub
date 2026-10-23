// Theme centralisé pour ECharts
// Couleurs alignées avec DICT_CONTENT dans backend/app.py

export const chartThemeLight = {
  color: ["#961010", "#4abb15", "#14aca4", "#821f9b", "#121096", "#ff6b6b"],
  backgroundColor: "transparent",
  textStyle: {
    fontFamily:
      '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
    fontSize: 12,
    color: "#333",
  },
  title: {
    textStyle: {
      color: "#1a1a1a",
      fontSize: 16,
      fontWeight: "bold",
    },
  },
  line: {
    itemStyle: {
      borderWidth: 1,
    },
    lineStyle: {
      width: 2,
    },
    symbolSize: 4,
    smooth: false,
  },
  radar: {
    itemStyle: {
      borderWidth: 1,
    },
    lineStyle: {
      width: 2,
    },
    symbolSize: 4,
    smooth: false,
  },
  bar: {
    itemStyle: {
      barBorderWidth: 0,
      barBorderRadius: 0,
    },
  },
  pie: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  scatter: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  boxplot: {
    itemStyle: {
      borderWidth: 1,
      borderRadius: 0,
    },
  },
  parallel: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  sankey: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  funnel: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  gauge: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  candlestick: {
    itemStyle: {
      color: "#961010",
      color0: "#4abb15",
      borderColor: "#961010",
      borderColor0: "#4abb15",
      borderWidth: 1,
    },
  },
  graph: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
    lineStyle: {
      width: 1,
      color: "#aaa",
    },
    symbolSize: 4,
    smooth: false,
    color: ["#961010", "#4abb15", "#14aca4", "#821f9b", "#121096"],
    label: {
      color: "#333",
    },
  },
  map: {
    itemStyle: {
      areaColor: "#f3f3f3",
      borderColor: "#516b91",
      borderWidth: 0.5,
    },
    label: {
      color: "#000",
    },
    emphasis: {
      itemStyle: {
        areaColor: "#ffd700",
        borderColor: "#516b91",
        borderWidth: 1,
      },
      label: {
        color: "#000",
      },
    },
  },
  geo: {
    itemStyle: {
      areaColor: "#f3f3f3",
      borderColor: "#516b91",
      borderWidth: 0.5,
    },
    label: {
      color: "#000",
    },
    emphasis: {
      itemStyle: {
        areaColor: "#ffd700",
        borderColor: "#516b91",
        borderWidth: 1,
      },
      label: {
        color: "#000",
      },
    },
  },
  categoryAxis: {
    axisLine: {
      show: true,
      lineStyle: {
        color: "#ccc",
      },
    },
    axisTick: {
      show: true,
      lineStyle: {
        color: "#ccc",
      },
    },
    axisLabel: {
      show: true,
      color: "#666",
    },
    splitLine: {
      show: false,
      lineStyle: {
        color: ["#e8e8e8"],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: ["rgba(250,250,250,0.3)", "rgba(200,200,200,0.3)"],
      },
    },
  },
  valueAxis: {
    axisLine: {
      show: false,
      lineStyle: {
        color: "#ccc",
      },
    },
    axisTick: {
      show: false,
      lineStyle: {
        color: "#ccc",
      },
    },
    axisLabel: {
      show: true,
      color: "#666",
    },
    splitLine: {
      show: true,
      lineStyle: {
        color: ["#e8e8e8"],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: ["rgba(250,250,250,0.3)", "rgba(200,200,200,0.3)"],
      },
    },
  },
  logAxis: {
    axisLine: {
      show: false,
      lineStyle: {
        color: "#ccc",
      },
    },
    axisTick: {
      show: false,
      lineStyle: {
        color: "#ccc",
      },
    },
    axisLabel: {
      show: true,
      color: "#666",
    },
    splitLine: {
      show: true,
      lineStyle: {
        color: ["#e8e8e8"],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: ["rgba(250,250,250,0.3)", "rgba(200,200,200,0.3)"],
      },
    },
  },
  timeAxis: {
    axisLine: {
      show: true,
      lineStyle: {
        color: "#ccc",
      },
    },
    axisTick: {
      show: true,
      lineStyle: {
        color: "#ccc",
      },
    },
    axisLabel: {
      show: true,
      color: "#666",
    },
    splitLine: {
      show: false,
      lineStyle: {
        color: ["#e8e8e8"],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: ["rgba(250,250,250,0.3)", "rgba(200,200,200,0.3)"],
      },
    },
  },
  toolbox: {
    iconStyle: {
      borderColor: "#666",
    },
    emphasis: {
      iconStyle: {
        borderColor: "#000",
      },
    },
  },
  legend: {
    textStyle: {
      color: "#333",
    },
  },
  tooltip: {
    axisPointer: {
      lineStyle: {
        color: "#ccc",
        width: 1,
      },
      crossStyle: {
        color: "#ccc",
        width: 1,
      },
    },
    backgroundColor: "rgba(50, 50, 50, 0.9)",
    borderColor: "#333",
    borderWidth: 0,
    textStyle: {
      color: "#fff",
    },
  },
  timeline: {
    lineStyle: {
      color: "#ccc",
      width: 1,
    },
    itemStyle: {
      color: "#961010",
      borderWidth: 1,
    },
    controlStyle: {
      color: "#666",
      borderColor: "#ccc",
      borderWidth: 0.5,
    },
    checkpointStyle: {
      color: "#4abb15",
      borderColor: "rgba(194,53,49,0.5)",
    },
    label: {
      color: "#666",
    },
    emphasis: {
      itemStyle: {
        color: "#4abb15",
      },
      controlStyle: {
        color: "#666",
        borderColor: "#ccc",
        borderWidth: 0.5,
      },
      label: {
        color: "#666",
      },
    },
  },
};

export const chartThemeDark = {
  color: ["#ee6666", "#5ebd3e", "#05c0de", "#b6a2de", "#5470c6", "#91d1d1"],
  backgroundColor: "transparent",
  textStyle: {
    fontFamily:
      '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
    fontSize: 12,
    color: "#e0e0e0",
  },
  title: {
    textStyle: {
      color: "#f0f0f0",
      fontSize: 16,
      fontWeight: "bold",
    },
  },
  line: {
    itemStyle: {
      borderWidth: 1,
    },
    lineStyle: {
      width: 2,
    },
    symbolSize: 4,
    smooth: false,
  },
  radar: {
    itemStyle: {
      borderWidth: 1,
    },
    lineStyle: {
      width: 2,
    },
    symbolSize: 4,
    smooth: false,
  },
  bar: {
    itemStyle: {
      barBorderWidth: 0,
      barBorderRadius: 0,
    },
  },
  pie: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  scatter: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  boxplot: {
    itemStyle: {
      borderWidth: 1,
      borderRadius: 0,
    },
  },
  parallel: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  sankey: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  funnel: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  gauge: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
  },
  candlestick: {
    itemStyle: {
      color: "#ee6666",
      color0: "#5ebd3e",
      borderColor: "#ee6666",
      borderColor0: "#5ebd3e",
      borderWidth: 1,
    },
  },
  graph: {
    itemStyle: {
      borderWidth: 0,
      borderRadius: 0,
    },
    lineStyle: {
      width: 1,
      color: "#444",
    },
    symbolSize: 4,
    smooth: false,
    color: ["#ee6666", "#5ebd3e", "#05c0de", "#b6a2de", "#5470c6"],
    label: {
      color: "#e0e0e0",
    },
  },
  map: {
    itemStyle: {
      areaColor: "#333",
      borderColor: "#516b91",
      borderWidth: 0.5,
    },
    label: {
      color: "#e0e0e0",
    },
    emphasis: {
      itemStyle: {
        areaColor: "#ffd700",
        borderColor: "#516b91",
        borderWidth: 1,
      },
      label: {
        color: "#fff",
      },
    },
  },
  geo: {
    itemStyle: {
      areaColor: "#333",
      borderColor: "#516b91",
      borderWidth: 0.5,
    },
    label: {
      color: "#e0e0e0",
    },
    emphasis: {
      itemStyle: {
        areaColor: "#ffd700",
        borderColor: "#516b91",
        borderWidth: 1,
      },
      label: {
        color: "#fff",
      },
    },
  },
  categoryAxis: {
    axisLine: {
      show: true,
      lineStyle: {
        color: "#444",
      },
    },
    axisTick: {
      show: true,
      lineStyle: {
        color: "#444",
      },
    },
    axisLabel: {
      show: true,
      color: "#999",
    },
    splitLine: {
      show: false,
      lineStyle: {
        color: ["#2a2a2a"],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: ["rgba(250,250,250,0.02)", "rgba(200,200,200,0.02)"],
      },
    },
  },
  valueAxis: {
    axisLine: {
      show: false,
      lineStyle: {
        color: "#444",
      },
    },
    axisTick: {
      show: false,
      lineStyle: {
        color: "#444",
      },
    },
    axisLabel: {
      show: true,
      color: "#999",
    },
    splitLine: {
      show: true,
      lineStyle: {
        color: ["#2a2a2a"],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: ["rgba(250,250,250,0.02)", "rgba(200,200,200,0.02)"],
      },
    },
  },
  logAxis: {
    axisLine: {
      show: false,
      lineStyle: {
        color: "#444",
      },
    },
    axisTick: {
      show: false,
      lineStyle: {
        color: "#444",
      },
    },
    axisLabel: {
      show: true,
      color: "#999",
    },
    splitLine: {
      show: true,
      lineStyle: {
        color: ["#2a2a2a"],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: ["rgba(250,250,250,0.02)", "rgba(200,200,200,0.02)"],
      },
    },
  },
  timeAxis: {
    axisLine: {
      show: true,
      lineStyle: {
        color: "#444",
      },
    },
    axisTick: {
      show: true,
      lineStyle: {
        color: "#444",
      },
    },
    axisLabel: {
      show: true,
      color: "#999",
    },
    splitLine: {
      show: false,
      lineStyle: {
        color: ["#2a2a2a"],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: ["rgba(250,250,250,0.02)", "rgba(200,200,200,0.02)"],
      },
    },
  },
  toolbox: {
    iconStyle: {
      borderColor: "#999",
    },
    emphasis: {
      iconStyle: {
        borderColor: "#fff",
      },
    },
  },
  legend: {
    textStyle: {
      color: "#e0e0e0",
    },
  },
  tooltip: {
    axisPointer: {
      lineStyle: {
        color: "#666",
        width: 1,
      },
      crossStyle: {
        color: "#666",
        width: 1,
      },
    },
    backgroundColor: "rgba(50, 50, 50, 0.9)",
    borderColor: "#666",
    borderWidth: 0,
    textStyle: {
      color: "#fff",
    },
  },
  timeline: {
    lineStyle: {
      color: "#444",
      width: 1,
    },
    itemStyle: {
      color: "#ee6666",
      borderWidth: 1,
    },
    controlStyle: {
      color: "#999",
      borderColor: "#444",
      borderWidth: 0.5,
    },
    checkpointStyle: {
      color: "#5ebd3e",
      borderColor: "rgba(194,53,49,0.5)",
    },
    label: {
      color: "#999",
    },
    emphasis: {
      itemStyle: {
        color: "#5ebd3e",
      },
      controlStyle: {
        color: "#999",
        borderColor: "#444",
        borderWidth: 0.5,
      },
      label: {
        color: "#999",
      },
    },
  },
};
