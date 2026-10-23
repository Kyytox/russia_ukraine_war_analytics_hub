import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsTreemap({ id, data, t, i18n, nf, custom }) {
  const option = useMemo(() => {
    // create a map of countries to weapons
    const libelles = new Map();
    data.forEach((d) => {
      if (!libelles.has(d.parent)) libelles.set(d.parent, []);
      libelles.get(d.parent).push({
        name: t(`page.content.${d.label}`, d.label),
        value: d.value,
      });
    });

    const tree = [...libelles].map(([country, children]) => ({
      name: t(`page.content.${country}`, country),
      itemStyle: {
        color: custom.colors_metrics[country] || "#1c2430",
      },
      children: children.sort((a, b) => b.value - a.value),
    }));

    return {
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: { saveAsImage: {} },
      },
      tooltip: {
        formatter: (p) => {
          const path = p.treePathInfo
            .slice(1)
            .map((n) => n.name)
            .join(" › ");
          return `${path}<br/><b>${nf.format(p.value)}</b>`;
        },
      },

      series: [
        {
          type: "treemap",
          data: tree,
          roam: true,
          nodeClick: "zoomToNode",
          breadcrumb: {
            show: true,
            bottom: 0,
            itemStyle: { color: "#1c2430", textStyle: { color: "#fff" } },
          },
          label: {
            show: true,
            color: "#fff",
            fontSize: 12,
            overflow: "truncate",
            formatter: (p) => `${p.name}\n${nf.format(p.value)}`,
          },
          upperLabel: {
            show: true,
            height: 26,
            color: "#fff",
            fontWeight: "bold",
            formatter: (p) => `${p.name}`,
            position: "insideLeft",
          },
          levels: [
            {
              // level 1
              itemStyle: {
                borderColor: "#120f09",
                borderWidth: 3,
                gapWidth: 3,
              },
            },
            {
              // level 2
              colorSaturation: [0.35, 0.6], // nuances de la couleur du pays
              itemStyle: {
                borderColorSaturation: 0.6,
                borderWidth: 1,
                gapWidth: 1,
              },
            },
          ],
        },
      ],
    };
  }, [data, t, i18n.language, nf]);

  return (
    <ReactECharts option={option} style={{ width: "100%", height: 800 }} />
  );
}
