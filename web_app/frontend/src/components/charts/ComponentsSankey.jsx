import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

// Returns true if the links contain a cycle (ECharts Sankey must be a DAG)
const hasCycle = (links) => {
  const adj = new Map();
  links.forEach((l) => {
    if (!adj.has(l.source)) adj.set(l.source, []);
    adj.get(l.source).push(l.target);
  });
  const state = new Map(); // 1 = visiting, 2 = done
  const visit = (n) => {
    if (state.get(n) === 1) return true;
    if (state.get(n) === 2) return false;
    state.set(n, 1);
    for (const next of adj.get(n) ?? []) if (visit(next)) return true;
    state.set(n, 2);
    return false;
  };
  return [...adj.keys()].some(visit);
};

export function ComponentsSankey({ id, data, t, nf, custom, height }) {
  const option = useMemo(() => {
    if (!data?.length) return null;
    const cfg = custom?.[id] ?? {};

    // 1. Each row = one link: libelle (source) -> metric (target), weighted by value
    //    Rows without libelle (root nodes) have no link, so they are skipped
    const linkMap = new Map();
    data.forEach((d) => {
      const source = d.libelle;
      const target = d.metric;
      const value = Number(d.value);
      if (!source || !target || !(value > 0)) return;
      const k = `${source}|${target}`;
      linkMap.set(k, (linkMap.get(k) ?? 0) + value); // sum if duplicates
    });

    const links = [...linkMap].map(([k, value]) => {
      const [source, target] = k.split("|");
      return { source, target, value };
    });
    if (!links.length) return null;

    if (hasCycle(links)) {
      console.warn(`[Sankey ${id}] cycle detected in data, chart not rendered`);
      return null;
    }

    // 2. Nodes = every unique name found in links
    const names = [...new Set(links.flatMap((l) => [l.source, l.target]))];
    const nodes = names.map((name) => {
      const color = custom?.colors_metrics?.[name]; // optional fixed color
      return { name, ...(color && { itemStyle: { color } }) };
    });

    const translate = (name) => t(`${"page.content"}.${name}`, String(name));

    return {
      aria: { enabled: true },
      tooltip: {
        trigger: "item",
        confine: true,
        formatter: (p) =>
          p.dataType === "edge"
            ? `${translate(p.data.source)} → ${translate(p.data.target)} : <b>${nf.format(p.data.value)} ${cfg.tooltip_text ?? ""}</b>`
            : `${translate(p.name)} : <b>${nf.format(p.value)} ${cfg.tooltip_text ?? ""}</b>`,
      },
      series: [
        {
          type: "sankey",
          data: nodes,
          links,
          top: "6%",
          bottom: "6%",
          left: "2%",
          right: "12%", // room for the last column metrics
          nodeWidth: cfg.nodeWidth ?? 18,
          nodeGap: cfg.nodeGap ?? 10,
          draggable: false,

          emphasis: { focus: "adjacency" },
          lineStyle: { color: "gradient", curveness: 0.4, opacity: 0.8 },
          itemStyle: { borderWidth: 0 },
          metric: {
            color: "#fff",
            fontSize: 13,
            formatter: (p) => translate(p.name),
          },
          label: {
            show: true,
            fontSize: 13,
            fontWeight: "normal",
            borderCololor: "transparent",
            color: "#FFF",
          },
        },
      ],
    };
  }, [data, t, nf, id, custom]);

  if (!option) return null;

  return (
    <ReactECharts
      option={option}
      notMerge
      style={{ width: "100%", height: height || custom?.[id]?.height }}
    />
  );
}
