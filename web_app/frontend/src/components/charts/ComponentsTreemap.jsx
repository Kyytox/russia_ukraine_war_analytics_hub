import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const BORDER_COLOR = "#1c2430";
const FALLBACK_COLOR = "##0e0e0e"; // used when a key has no color in colors_metrics

export function ComponentsTreemap({
  tab,
  id,
  data,
  t,
  i18n,
  nf,
  custom,
  gradient,
}) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const colors = custom?.colors_metrics ?? {};

    const translate = (key) => t(`page.content.${key}`, { defaultValue: key });
    const pctFmt = new Intl.NumberFormat(i18n.language, {
      style: "percent",
      maximumFractionDigits: 1,
    });

    const values = data.map((d) => Number(d.value));
    const total = values.reduce((sum, v) => sum + v, 0);

    // libelle -> children
    const groups = new Map();
    data.forEach((d) => {
      if (!groups.has(d.libelle)) groups.set(d.libelle, []);
      groups.get(d.libelle).push({
        name: translate(d.metric),
        value: Number(d.value),
        metric: d.metric,
      });
    });

    const tree = [...groups].map(([key, children]) => {
      const parentColor = colors[key] ?? FALLBACK_COLOR;

      return {
        name: translate(key),
        itemStyle: {
          borderColor: parentColor,
          ...(!gradient && { color: parentColor }),
        },
        children: [...children]
          .sort((a, b) => b.value - a.value)
          .map((c) => ({
            name: c.name,
            value: c.value,
            ...(!gradient && {
              itemStyle: { color: colors[c.metric] ?? parentColor },
            }),
          })),
      };
    });

    return {
      aria: { enabled: true },
      ...(gradient && {
        visualMap: {
          show: false,
          type: "continuous",
          min: Math.min(...values),
          max: Math.max(...values),
          dimension: "value",
          inRange: { color: cfg.gradient },
        },
      }),
      tooltip: {
        trigger: "item",
        formatter: (p) =>
          `${p.name} <br/> ${nf.format(p.value)} (${pctFmt.format(p.value / total)})`,
      },
      series: [
        {
          type: "treemap",
          data: tree,
          left: 0,
          right: 0,
          top: 20,
          bottom: 40,
          roam: false,
          nodeClick: "zoomToNode",
          ...(gradient && { visualDimension: 0 }),
          breadcrumb: {
            show: true,
            bottom: 0,
            height: 22,
            itemStyle: {
              color: BORDER_COLOR,
              textStyle: { color: "#fff" },
            },
          },
          label: {
            show: true,
            fontSize: cfg.fontSize ?? 12,
            overflow: "truncate",
            formatter: (p) =>
              `${p.name} \n ${nf.format(p.value)} (${pctFmt.format(p.value / total)})`,
          },
          upperLabel: {
            show: true,
            height: 22,
            fontWeight: "bold",
            position: "insideLeft",
            formatter: (p) => `${p.name} · ${nf.format(p.value)}`,
          },
          levels: [
            {
              itemStyle: { borderWidth: 0, gapWidth: 0 },
              upperLabel: { show: false },
            },
            {
              itemStyle: {
                borderColor: BORDER_COLOR,
                borderWidth: 2,
                gapWidth: 1,
              },
            },
            {
              itemStyle: {
                borderColor: BORDER_COLOR,
                borderWidth: 1,
                gapWidth: 2,
              },
            },
          ],
        },
      ],
    };
  }, [data, t, i18n.language, nf, id, custom, gradient, tab]);

  if (!option) return null;

  // Determine chart height:
  const chartHeight = custom?.[id]?.height;

  return (
    <ReactECharts
      option={option}
      notMerge
      style={{ width: "100%", height: chartHeight }}
    />
  );
}
