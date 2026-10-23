import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const TOP_LEVEL2 = 20; // nombre de nœuds gardés au niveau 2, le reste va dans "Autres"
const MIN_LABEL_LEVEL1 = 40; // un nœud de niveau 1 plus petit n'affiche pas son label (tooltip conservé)
const ROW_HEIGHT = 26; // hauteur allouée par nœud dans la colonne la plus chargée
const OTHER = "__other__";

export function ComponentsSankey({ data, t, i18n, nf }) {
  const { option, height } = useMemo(() => {
    // 1. niveau 2 : top N + regroupement du reste en un seul nœud "Autres"
    const lvl2 = data
      .filter((d) => d.level === 2)
      .sort((a, b) => b.value - a.value);

    const others = new Map(); // parent -> somme des valeurs regroupées (garde les flux cohérents)
    lvl2
      .slice(TOP_LEVEL2)
      .forEach((d) =>
        others.set(d.parent, (others.get(d.parent) ?? 0) + d.value),
      );

    const rows = [
      ...data.filter((d) => d.level < 2),
      ...lvl2.slice(0, TOP_LEVEL2),
      ...[...others].map(([parent, value]) => ({
        parent,
        label: OTHER,
        value,
        level: 2,
      })),
    ];

    // 2. liens : le niveau 0 n'a pas de parent, il ne crée donc aucun lien
    const links = rows
      .filter((d) => d.level > 0 && d.parent)
      .map((d) => ({
        source: `${d.level - 1}:${d.parent}`, // "0:UAV", "1:UAV Combat"
        target: `${d.level}:${d.label}`, // "1:UAV Combat", "2:Geran-4 UAV"
        value: d.value,
      }));

    // 3. total entrant par nœud (pour masquer les labels des petits nœuds)
    const incoming = new Map();
    links.forEach((l) =>
      incoming.set(l.target, (incoming.get(l.target) ?? 0) + l.value),
    );

    // 4. nœuds, avec leur colonne forcée
    const ids = [...new Set(links.flatMap((l) => [l.source, l.target]))];
    const nodes = ids.map((id) => {
      const depth = Number(id.split(":")[0]);
      const show = depth !== 1 || (incoming.get(id) ?? 0) >= MIN_LABEL_LEVEL1;
      return { name: id, depth, label: { show } };
    });

    // 5. hauteur adaptée à la colonne la plus chargée
    const perLevel = {};
    ids.forEach((id) => {
      const l = id.split(":")[0];
      perLevel[l] = (perLevel[l] ?? 0) + 1;
    });
    const height = Math.max(
      600,
      Math.max(...Object.values(perLevel)) * ROW_HEIGHT * 2,
    );

    // 6. libellé traduit à partir de l'id "niveau:nom"
    const label = (id) => {
      const [level, ...rest] = id.split(":");
      const text = rest.join(":");
      if (text === OTHER) return t("sankey.other");
      return t(`sankey.level${level}.${text}`, text);
    };

    const option = {
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: { saveAsImage: {} },
      },
      tooltip: {
        trigger: "item",
        formatter: (p) =>
          p.dataType === "edge"
            ? `${label(p.data.source)} → ${label(p.data.target)}<br/><b>${nf.format(p.value)}</b>`
            : `${label(p.data.name)}<br/><b>${nf.format(p.value)}</b>`,
      },
      series: [
        {
          type: "sankey",
          data: nodes,
          links,
          left: 20,
          right: 220,
          top: 20,
          bottom: 20,
          nodeWidth: 14,
          nodeGap: 6,
          lineStyle: { color: "source", opacity: 0.4, curveness: 0.5 },
          emphasis: { focus: "adjacency" },
          label: {
            color: "#fff",
            fontSize: 12,
            width: 200,
            overflow: "truncate",
            formatter: (p) => label(p.name),
          },
        },
      ],
    };

    return { option, height };
  }, [data, t, i18n.language]);

  return <ReactECharts option={option} style={{ width: "100%", height }} />;
}
