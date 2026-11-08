import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const FALLBACK_COLOR = "#6b7280";
const BORDER_COLOR = "#0d1117"; // à aligner sur le fond de la carte

// Texte sombre sur fond clair, blanc sinon (couleurs hex #rrggbb)
const textColorFor = (bg) => {
  const m = /^#?([0-9a-f]{6})$/i.exec(bg ?? "");
  if (!m) return "#fff";
  const n = parseInt(m[1], 16);
  const r = (n >> 16) & 255;
  const g = (n >> 8) & 255;
  const b = n & 255;
  return (0.299 * r + 0.587 * g + 0.114 * b) / 255 > 0.6 ? "#111827" : "#fff";
};

export function ComponentsSunburst({ tab, id, data, t, i18n, nf, custom }) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const translate = (key) => t(`page.content.${key}`, key);
    const pctFmt = new Intl.NumberFormat(i18n.language, {
      style: "percent",
      maximumFractionDigits: 1,
    });
    const getColor = (key) => custom?.colors_metrics?.[key] ?? FALLBACK_COLOR;

    // libelle -> enfants
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
      const color = getColor(key);
      return {
        name: translate(key),
        itemStyle: { color },
        label: { color: textColorFor(color) },
        children: children.map((c) => {
          const childColor = getColor(c.metric);
          return {
            name: c.name,
            value: c.value,
            itemStyle: { color: childColor },
            label: { color: textColorFor(childColor) },
          };
        }),
      };
    });

    const total = data.reduce((sum, d) => sum + Number(d.value), 0);

    return {
      aria: { enabled: true },
      tooltip: {
        confine: true,
        formatter: (p) => {
          const path = p.treePathInfo
            .slice(1)
            .map((n) => n.name)
            .join(" › ");
          return `${p.marker} ${path}<br/><b>${nf.format(p.value)}</b> ${t(
            `page.${tab}.tooltip_text`,
          )} (${pctFmt.format(p.value / total)})`;
        },
      },
      graphic: [
        {
          type: "text",
          left: "center",
          top: "center",
          silent: true,
          style: {
            text: `${nf.format(total)}\n${t(`page.${tab}.total_metric`)}`,
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
          sort: "desc", // mets null pour garder l'ordre des données
          emphasis: { focus: "ancestor" },
          itemStyle: {
            borderWidth: 1,
            borderColor: BORDER_COLOR,
          },
          label: {
            show: true,
            fontSize: 11,
            rotate: "radial",
            minAngle: 3, // masque l'étiquette sous 3° (évite le chevauchement)
            align: "center",
            fontWeight: 500,
            formatter: (p) => {
              return `${p.name}\n${nf.format(p.value)}`;
            },
          },
          levels: [
            {}, // racine virtuelle
            {
              // anneau 1 : libelle
              r0: "22%",
              r: "58%",
              itemStyle: {
                borderRadius: 4,
                borderWidth: 2,
                borderColor: BORDER_COLOR,
              },
              label: {
                rotate: "tangential",
                minAngle: 12,
                fontSize: 12,
                fontWeight: 600,
              },
            },
            {
              // anneau 2 : metric
              r0: "58%",
              r: "98%",
              itemStyle: {
                borderRadius: 2,
                borderWidth: 1,
                borderColor: BORDER_COLOR,
              },
              label: {
                rotate: "radial",
                align: "center",
                minAngle: 3,
                fontSize: 11,
              },
            },
          ],
        },
      ],
    };
  }, [data, t, i18n.language, nf, custom, tab]);

  if (!option) return null;

  return (
    <ReactECharts
      option={option}
      style={{ width: "100%", height: custom?.[id]?.height }}
    />
  );
}
