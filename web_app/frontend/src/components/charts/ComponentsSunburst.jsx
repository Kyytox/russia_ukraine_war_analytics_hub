import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const FALLBACK_COLOR = "#6b7280"; // gris neutre, distinct du fond et des bordures
const BORDER_COLOR = "#0d1117"; // à aligner sur le fond de la carte

export function ComponentsSunburst({ id, data, t, i18n, nf, custom, height }) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const translate = (key) => t(`page.content.${key}`, key);
    const pctFmt = new Intl.NumberFormat(i18n.language, {
      style: "percent",
      maximumFractionDigits: 1,
    });

    // Helper function to get color from custom config
    const getColor = (key) => custom?.colors_metrics?.[key] ?? FALLBACK_COLOR;

    // parent -> enfants
    const groups = new Map();
    data.forEach((d) => {
      if (!groups.has(d.parent)) groups.set(d.parent, []);
      groups.get(d.parent).push({
        name: translate(d.label),
        value: d.value,
        label: d.label, // Keep original label for color lookup
      });
    });

    // Build tree with color information for all levels
    const tree = [...groups].map(([key, children]) => ({
      name: translate(key),
      value: children.reduce((sum, c) => sum + c.value, 0), // Parent value = sum of children
      itemStyle: {
        color: getColor(key), // Color for parent level
      },
      children: children.map((child) => ({
        name: child.name,
        value: child.value,
        itemStyle: {
          color: getColor(child.label), // Color for children level
        },
      })),
    }));

    console.log("ComponentsSunburst tree", tree);

    const total = data.reduce((sum, d) => sum + d.value, 0);

    return {
      aria: { enabled: true },
      tooltip: {
        formatter: (p) => {
          const path = p.treePathInfo
            .slice(1)
            .map((n) => n.name)
            .join(" › ");
          return `${path}<br/><b>${nf.format(p.value)}</b> ${t("page.tooltip_text")} (${pctFmt.format(p.value / total)})`;
        },
      },
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: { saveAsImage: {} },
      },
      graphic: [
        {
          type: "text",
          left: "center",
          top: "center",
          silent: true,
          style: {
            text: `${nf.format(total)}\n${t("page.total_label")}`,
            textAlign: "center",
            textVerticalAlign: "middle",
            fill: "#fff",
            font: "600 16px sans-serif",
            lineHeight: 22,
          },
        },
      ],
      series: [
        {
          type: "sunburst",
          data: tree,
          radius: ["22%", "98%"],
          emphasis: { focus: "ancestor" },
          itemStyle: {
            borderRadius: 4,
            borderWidth: 2,
            borderColor: BORDER_COLOR,
          },
          label: {
            show: true,
            fontSize: 11,
            overflow: "truncate",
            rotate: "radial",
            minAngle: 6,
            formatter: (p) => `${p.name}\n${nf.format(p.value)}`,
          },
          levels: [
            {
              // center - no specific styling needed
            },
            {
              // First ring (parents)
              label: { rotate: "tangential", minAngle: 12 },
              itemStyle: {
                borderRadius: 4,
                borderWidth: 2,
                borderColor: BORDER_COLOR,
              },
            },
            {
              // Second ring (children)
              label: { rotate: "radial", align: "center", minAngle: 3 },
              itemStyle: {
                borderWidth: 1,
                borderRadius: 3,
                borderColor: BORDER_COLOR,
              },
            },
          ],
        },
      ],
    };
  }, [data, t, i18n.language, nf, custom]);

  if (!option) return null;

  return (
    <ReactECharts
      option={option}
      style={{ width: "100%", height: height || custom?.[id]?.height || 480 }}
    />
  );
}
