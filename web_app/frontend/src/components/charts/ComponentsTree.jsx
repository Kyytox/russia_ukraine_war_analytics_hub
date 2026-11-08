import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsTree({ tab, id, data = [], t, i18n, nf, custom }) {
  const option = useMemo(() => {
    const name = (metric) => t(`page.content.${metric}`, metric);

    // Restructure the data into a tree format
    const bylibelle = new Map();

    data.forEach(({ libelle, metric, value }) => {
      if (!bylibelle.has(libelle)) {
        bylibelle.set(libelle, []);
      }

      bylibelle.get(libelle).push({
        // name: name(metric),
        name: `${name(metric)} (${nf.format(value)})`,
        value,
      });
    });

    // Create the root nodes
    const roots = Array.from(bylibelle.entries())
      .map(([libelle, children]) => ({
        name: name(libelle),
        value: children.reduce((sum, child) => sum + child.value, 0),
        children: children.sort((a, b) => b.value - a.value),
      }))
      .sort((a, b) => b.value - a.value);

    // Create the root node for the tree
    const root = {
      name: t("page.chart-names.root", "Total"),
      value: roots.reduce((sum, item) => sum + item.value, 0),
      children: roots,
    };

    return {
      tooltip: {
        trigger: "item",
        triggerOn: "mousemove",
        formatter: (p) => `${p.name} : ${nf.format(p.value)}`,
        feature: { dataZoom: { xAxisIndex: "none" }, saveAsImage: {} },
      },
      series: [
        {
          type: "tree",
          data: [root],
          left: "7%",
          top: "1%",
          bottom: "1%",
          right: "20%",
          symbolSize: 7,
          initialTreeDepth: 2,
          metric: {
            position: "bottom",
            verticalAlign: "middle",
            align: "right",
            fontSize: 11,
            color: "#fff",
          },
          label: {
            show: true,
            fontsize: 13,
            position: "right",
            fontweight: "normal",
            bordercololor: "transparent",
            color: "#fff",
          },
          leaves: {
            metric: {
              position: "right",
              verticalAlign: "middle",
              align: "left",
            },
          },
          emphasis: { focus: "descendant" },
          expandAndCollapse: true,
          animationDuration: 550,
          animationDurationUpdate: 750,
        },
      ],
    };
  }, [data, t, i18n.language, nf, tab]);
  return (
    <ReactECharts
      option={option}
      notMerge
      style={{
        width: "100%",
        height: custom?.[id]?.height,
      }}
    />
  );
}
