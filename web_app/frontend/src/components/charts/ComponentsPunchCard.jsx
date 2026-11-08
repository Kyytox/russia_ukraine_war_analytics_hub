import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const isNumeric = (arr) =>
  arr.every((v) => v !== "" && !Number.isNaN(Number(v)));
const unique = (values) => [...new Set(values.map(String))];

export function ComponentsPunchCard({ tab, id, data, t, i18n, nf, custom }) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const labelPrefix = cfg.label_prefix ?? "page.content";
    const minS = 1;
    const maxS = 40;

    const label = (key) => String(t(`${labelPrefix}.${key}`, key));

    // --- Catégories : ordre custom > ordre d'apparition (aucun tri)
    const xs = (cfg.x_order ?? unique(data.map((d) => d.libelle))).map(String);
    const ys = (cfg.y_order ?? unique(data.map((d) => d.metric))).map(String);
    const yNumeric = isNumeric(ys);
    const xIndex = new Map(xs.map((k, i) => [k, i]));
    const yIndex = new Map(ys.map((k, i) => [k, i]));

    // --- Couleur par libelle
    const colorOf = (lib) => custom?.colors_metrics?.[lib];

    // --- Taille : racine carrée pour que la surface soit proportionnelle
    const maxValue = Math.max(...data.map((d) => Math.abs(d.value)), 1);
    const sizeOf = (v) =>
      minS + (maxS - minS) * Math.sqrt(Math.abs(v) / maxValue);

    // --- Une série par libelle (couleur + légende)
    const series = xs.map((lib) => ({
      name: label(lib),
      type: "scatter",
      data: data
        .filter(
          (d) => String(d.libelle) === lib && yIndex.has(String(d.metric)),
        )
        .map((d) => ({
          value: [
            xIndex.get(lib),
            yIndex.get(String(d.metric)),
            d.value,
            lib,
            String(d.metric),
          ],
        })),
      symbolSize: (val) => sizeOf(val[2]),
      itemStyle: { color: colorOf(lib) },
      emphasis: { scale: 1, itemStyle: { opacity: 1 } },
    }));

    return {
      aria: { enabled: true },
      grid: {
        top: 48,
        left: "4%",
        right: "4%",
        bottom: "5%",
        containLabel: true,
      },
      tooltip: {
        trigger: "item",
        confine: true,
        formatter: (p) => {
          const [, , v, x, y] = p.value;
          return `${p.marker} ${label(x)}<br/><b>Age: ${label(y)}<br/><b>${nf.format(v)}</b> ${t(
            cfg.tooltip ?? `page.${tab}.tooltip_text`,
          )}`;
        },
      },
      xAxis: {
        type: "category",
        data: xs.map(label),
        axisTick: { show: false, alignWithLabel: true },
        axisLine: { show: true },
        splitLine: custom?.common?.splitLine,
        axisLabel: {
          interval: 0,
          rotate: cfg.xAxisRotate,
          width: custom?.common?.labelMaxLength,
          overflow: "truncate",
          fontSize: 13,
          color: "#fff",
        },
      },
      yAxis: {
        name: cfg.yAxisName,
        type: "category",
        data: ys.map(label),
        inverse: !yNumeric,
        axisTick: { show: false },
        axisLine: custom?.common?.splitLine,
        axisLabel: { interval: 0, fontSize: 13, color: "#fff" },
      },
      series,
    };
  }, [data, t, i18n.language, nf, id, custom, tab]);

  if (!option) return null;
  const chartHeight = custom?.[id]?.height;

  return (
    <ReactECharts
      option={option}
      style={{
        width: "100%",
        height: chartHeight,
        alignSelf: "center",
      }}
    />
  );
}
