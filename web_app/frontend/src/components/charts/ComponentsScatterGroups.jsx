import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const LABEL_MAX_WIDTH = 150;

export function ComponentsScatterGroups({ id, data, t, i18n, nf, custom, height }) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const labelPrefix = cfg.label_prefix ?? "page.content";

    const categories = []; // x: dates
    const groups = []; // y: groups
    const catIndex = new Map();
    const groupIndex = new Map();

    for (const row of data) {
      if (!catIndex.has(row.libelle)) {
        catIndex.set(row.libelle, categories.length);
        categories.push(row.libelle);
      }
      if (!groupIndex.has(row.metric)) {
        groupIndex.set(row.metric, groups.length);
        groups.push(row.metric);
      }
    }

    const pointsByGroup = new Map(groups.map((g) => [g, []]));
    for (const row of data) {
      pointsByGroup
        .get(row.metric)
        .push([
          catIndex.get(row.libelle),
          groupIndex.get(row.metric),
          row.value,
        ]);
    }

    return {
      backgroundColor: "transparent",
      aria: { enabled: true },
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: {
          ...(cfg.zoomable && { dataZoom: {} }),
        },
      },
      grid: {
        top: "3%",
        left: "1%",
        right: "3%",
        bottom: "2%",
        containLabel: true,
      },
      legend: {
        show: cfg.showLegend ?? false,
        top: 0,
        textStyle: { color: "#fff", fontSize: 13 },
      },
      tooltip: {
        trigger: "item",
        confine: true,
        formatter: (p) =>
          `${p.marker} <b>${p.seriesName}</b><br/>${categories[p.value[0]]}` +
          (p.value[2] != null ? ` — <b>${nf.format(p.value[2])}</b>` : ""),
      },
      xAxis: {
        type: "category",
        data: categories,
        boundaryGap: true,
        axisLine: { lineStyle: { color: "rgba(255,255,255,0.3)" } },
        axisLabel: {
          hideOverlap: true,
          rotate: cfg.xAxisRotate ?? 45,
          width: LABEL_MAX_WIDTH,
          overflow: "truncate",
          fontSize: 12,
          color: "#fff",
        },
      },
      yAxis: {
        type: "category",
        data: groups.map((g) => t(`${labelPrefix}.${g}`, g)),
        inverse: true,
        axisLabel: { fontSize: 13, color: "#fff" },
        axisLine: { lineStyle: { color: "rgba(255,255,255,0.3)" } },
        splitLine: {
          show: true,
          lineStyle: { color: "rgba(255,255,255,0.2)", type: "dashed" },
        },
      },
      series: groups.map((name) => ({
        name: t(`${labelPrefix}.${name}`, name),
        type: "scatter",
        symbolSize: 7,
        itemStyle: {
          color: custom?.colors_metrics?.[name] ?? cfg.default_colors?.[name],
        },
        emphasis: { focus: "series" },
        data: pointsByGroup.get(name),
      })),
    };
  }, [data, t, i18n.language, nf, id, custom]);

  if (!option) return null;

  return (
    <ReactECharts
      option={option}
      style={{
        width: "100%",
        height: height || custom?.[id]?.height || 400,
        alignSelf: "center",
      }}
      theme="dark"
    />
  );
}
