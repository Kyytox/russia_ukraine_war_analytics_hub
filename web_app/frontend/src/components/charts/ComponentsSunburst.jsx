import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsSunburst({ id, data, t, i18n, nf, custom }) {
  const option = useMemo(() => {
    // create a map of parent to labels and values
    const dataSun = new Map();
    data.forEach((d) => {
      if (!dataSun.has(d.parent)) dataSun.set(d.parent, []);
      dataSun.get(d.parent).push({
        name: t(`page.content.${d.label}`, d.label),
        value: d.value,
      });
    });

    const tree = [...dataSun].map(([label, children]) => ({
      name: t(`page.content.${label}`, label),
      children: children.sort((a, b) => b.value - a.value),
    }));

    return {
      tooltip: {
        formatter: (p) => {
          const path = p.treePathInfo
            .slice(1)
            .map((n) => n.name)
            .join(" › ");
          return `${path}<br/><b>${nf.format(p.value)}</b>`;
        },
      },
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: {
          saveAsImage: {},
        },
      },
      series: [
        {
          type: "sunburst",
          data: tree,
          emphasis: {
            focus: "ancestor",
          },
          radius: [20, "99%"],
          itemStyle: {
            borderRadius: 4,
            borderWidth: 2,
            borderColor: "#1c2430",
          },
          label: {
            show: true,
            fontSize: 11,
            overflow: "truncate",
            formatter: (p) => `${p.name}\n${nf.format(p.value)}`,
            rotate: "radial",
          },
          levels: [
            {},
            {
              itemStyle: {
                borderWidth: 2,
              },
              label: {
                rotate: "tangential",
              },
            },
            {
              label: {
                align: "right",
              },
            },
            {
              label: {
                position: "outside",
                padding: 3,
                silent: false,
              },
              itemStyle: {
                borderWidth: 3,
              },
            },
          ],
          color: tree.map((d) => custom.colors_metrics[d.name] || "#1c2430"),
        },
      ],
    };
  }, [data, t, i18n.language]);

  return (
    <ReactECharts
      option={option}
      style={{ width: "100%", height: custom?.[id]?.height }}
    />
  );
}
