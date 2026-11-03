import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const FALLBACK_COLOR = "#6b7280"; // gris neutre
const BORDER_COLOR = "#1c2430"; // couleur des bordures

export function ComponentsTreemap({ id, data, t, i18n, nf, custom, height }) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const translate = (key) => t(`page.content.${key}`, key);
    const pctFmt = new Intl.NumberFormat(i18n.language, {
      style: "percent",
      maximumFractionDigits: 1,
    });
    const total = data.reduce((sum, d) => sum + d.value, 0);

    // Helper function to get color from custom config with fallback
    const getColor = (key) => custom?.colors_metrics?.[key] ?? FALLBACK_COLOR;

    // parent -> children
    const groups = new Map();
    data.forEach((d) => {
      if (!groups.has(d.parent)) groups.set(d.parent, []);
      groups.get(d.parent).push({
        name: translate(d.label),
        value: d.value,
        label: d.label, // Garder la clé originale pour le matching de couleur
      });
    });

    // Build tree with color information for all levels
    const tree = [...groups].map(([key, children]) => ({
      name: translate(key),
      value: children.reduce((sum, c) => sum + c.value, 0), // Parent value = sum of children
      itemStyle: {
        color: getColor(key), // Couleur du parent
      },
      children: children.map((child) => ({
        name: child.name,
        value: child.value,
        itemStyle: {
          color: getColor(child.label), // Couleur de l'enfant
        },
      })),
    }));

    return {
      backgroundColor: "transparent",
      aria: { enabled: true },
      tooltip: {
        confine: true,
        formatter: (p) => {
          const path = p.treePathInfo
            .slice(1) // drop the invisible root
            .map((n) => n.name)
            .join(" › ");
          return `${path}<br/><b>${nf.format(p.value)}</b> ${t("page.tooltip_text")} (${pctFmt.format(p.value / total)})`;
        },
      },
      series: [
        {
          type: "treemap",
          data: tree,
          left: 0,
          right: 0,
          top: 20,
          roam: false,
          nodeClick: "zoomToNode",
          breadcrumb: {
            show: true,
            bottom: 0,
            height: 22,
            itemStyle: {
              color: "#1c2430",
              textStyle: { color: "#fff" },
            },
          },
          label: {
            show: true,
            fontSize: cfg.fontSize ?? 12,
            overflow: "truncate",
            formatter: (p) => `${p.name}\n${nf.format(p.value)}`,
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
              // Parent level (level 0)
              itemStyle: {
                borderColor: BORDER_COLOR,
                borderWidth: 3,
                gapWidth: 3,
              },
            },
            {
              // Children level (level 1)
              itemStyle: {
                borderColor: BORDER_COLOR,
                borderColorSaturation: 0.5,
                borderWidth: 2,
                gapWidth: 2,
              },
            },
          ],
        },
      ],
    };
  }, [data, t, i18n.language, nf, id, custom]);

  if (!option) return null;

  const chartHeight = height || custom?.[id]?.height || 400;

  return (
    <ReactECharts
      option={option}
      notMerge
      style={{ width: "100%", height: chartHeight }}
      theme="dark"
    />
  );
}
