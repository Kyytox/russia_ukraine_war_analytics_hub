import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const FALLBACK_COLORS = [
  "#7F557D",
  "#9D4EDD",
  "#B298DC",
  "#D0A9F5",
  "#8B2E2E",
  "#C0655B",
  "#5B8C85",
  "#E0A990",
];

export function ComponentsPie({ id, data, t, i18n, nf, custom, height }) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const labelPrefix = cfg.label_prefix ?? "page.content";
    const total = data.reduce((sum, d) => sum + d.value, 0);
    const minInsidePct = cfg.minInsidePercent ?? 5;

    const items = data.map((d, i) => {
      const pct = (d.value / total) * 100;
      const outside = pct < minInsidePct;
      return {
        name: t(`${labelPrefix}.${d.metric}`, d.metric),
        value: d.value,
        itemStyle: {
          color:
            custom?.colors_metrics?.[d.metric] ??
            FALLBACK_COLORS[i % FALLBACK_COLORS.length],
          borderColor: "#0f1420",
          borderWidth: 2,
        },
        label: {
          position: outside ? "outside" : "inside",
        },
      };
    });

    return {
      backgroundColor: "transparent",
      title: {
        text: `${nf.format(total)}\n${t(`page.${id}.text`)}`,
        left: "center",
        top: "center",
        textStyle: { fontSize: 17, fontWeight: "bold", color: "#fff" },
      },
      tooltip: {
        trigger: "item",
        confine: true,
        formatter: (p) =>
          `${p.marker} ${p.name}<br/><b>${nf.format(p.value)}</b> ${t("page.tooltip_text")} (${p.percent}%)`,
      },
      series: [
        {
          type: "pie",
          radius: cfg.radius ?? ["32%", "80%"], // small center hole, thick ring
          center: ["50%", "50%"],
          startAngle: cfg.startAngle ?? 120,
          padAngle: cfg.padAngle ?? 1,
          itemStyle: { borderRadius: 6 },
          avoidLabelOverlap: true,
          label: {
            show: cfg.showLabels ?? true,
            fontSize: 13,
            color: "#fff",
            formatter: "{b}\n{d}%",
          },
          labelLine: {
            show: true,
            length: 10,
            length2: 12,
            lineStyle: { color: "rgba(255,255,255,0.4)" },
          },
          emphasis: {
            scale: true,
            scaleSize: 5,
            itemStyle: { shadowBlur: 12, shadowColor: "rgba(0,0,0,0.5)" },
          },
          data: items,
        },
      ],
    };
  }, [data, t, i18n.language, nf, id, custom]);

  if (!option) return null;

  return (
    <ReactECharts
      option={option}
      theme="dark"
      style={{
        width: "100%",
        height: height || custom?.[id]?.height || 400,
        alignSelf: "center",
      }}
    />
  );
}
