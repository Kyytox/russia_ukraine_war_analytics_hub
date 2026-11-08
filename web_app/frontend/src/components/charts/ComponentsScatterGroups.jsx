import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsScatterGroups({
  tab,
  id,
  data,
  t,
  i18n,
  nf,
  custom,
}) {
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
      aria: { enabled: true },
      grid: {
        top: "3%",
        left: "1%",
        right: "3%",
        bottom: "2%",
        containLabel: true,
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
        axisLine: custom?.common?.splitLine,
        axisLabel: {
          hideOverlap: true,
          rotate: cfg.xAxisRotate ?? 45,
          width: custom?.common?.labelMaxLength,
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
        axisLine: custom?.common?.splitLine,
        splitLine: custom?.common?.splitLine,
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
  }, [data, t, i18n.language, nf, id, custom, tab]);

  if (!option) return null;

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
