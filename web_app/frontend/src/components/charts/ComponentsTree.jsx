import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsTree({ id, data = [], t, i18n, nf, custom }) {
  const option = useMemo(() => {
    const name = (label) => t(`page.content.${label}`, label);

    // Restructure the data into a tree format
    const byParent = new Map();

    data.forEach(({ parent, label, value }) => {
      if (!byParent.has(parent)) {
        byParent.set(parent, []);
      }

      byParent.get(parent).push({
        // name: name(label),
        name: `${name(label)} (${nf.format(value)})`,
        value,
      });
    });

    // Create the root nodes
    const roots = Array.from(byParent.entries())
      .map(([parent, children]) => ({
        name: name(parent),
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
          top: "1%",
          left: "7%",
          bottom: "1%",
          right: "20%",
          symbolSize: 7,
          initialTreeDepth: 2,
          label: {
            position: "bottom",
            verticalAlign: "middle",
            align: "right",
            fontSize: 11,
            color: "#fff",
          },
          leaves: {
            label: {
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
  }, [data, t, i18n.language, nf]);
  return (
    <ReactECharts
      option={option}
      notMerge
      style={{
        width: "100%",
        height: custom?.[id]?.height ?? 500,
      }}
    />
  );
}
