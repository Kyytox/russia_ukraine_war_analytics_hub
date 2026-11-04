import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const LABEL_MAX_WIDTH = 150;

const isNumeric = (arr) =>
  arr.every((v) => v !== "" && !Number.isNaN(Number(v)));
const uniqueSorted = (values) => {
  const uniq = [...new Set(values.map(String))];
  return isNumeric(uniq) ? uniq.sort((a, b) => Number(a) - Number(b)) : uniq;
};

export function ComponentsPunchCard({ id, data, t, i18n, nf, custom, height }) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const labelPrefix = cfg.label_prefix ?? "page.content";
    const minS = cfg.min_size ?? 3;
    const maxS = cfg.max_size ?? 40;
    const alpha = cfg.opacity ?? 0.85;

    const label = (key) => String(t(`${labelPrefix}.${key}`, key));

    // --- Catégories : ordre custom > tri numérique > ordre d'apparition
    const xs = (cfg.x_order ?? uniqueSorted(data.map((d) => d.libelle))).map(
      String,
    );
    const ys = (cfg.y_order ?? uniqueSorted(data.map((d) => d.metric))).map(
      String,
    );
    const yNumeric = isNumeric(ys);
    const xIndex = new Map(xs.map((k, i) => [k, i]));
    const yIndex = new Map(ys.map((k, i) => [k, i]));

    // --- Couleur par libelle
    const colorOf = (lib) => custom?.colors_libs?.[lib];

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
      itemStyle: { color: colorOf(lib), opacity: alpha },
      emphasis: { scale: 1.1, itemStyle: { opacity: 1 } },
    }));

    return {
      backgroundColor: "transparent",
      aria: { enabled: true },
      toolbox: { right: 10, iconStyle: { borderColor: "#fff" } },
      legend: {
        show: cfg.show_legend ?? true,
        type: "scroll",
        top: 0,
        textStyle: { color: "#fff" },
      },
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
          return `${p.marker} ${label(x)} - ${label(y)}<br/><b>${nf.format(v)}</b> ${t(
            cfg.tooltip ?? "page.tooltip_text",
          )}`;
        },
      },
      xAxis: {
        type: "category",
        data: xs.map(label),
        axisTick: { show: false, alignWithLabel: true },
        axisLine: { show: false },
        splitLine: {
          show: true,
          lineStyle: { color: "rgba(255,255,255,0.12)" },
        },
        axisLabel: {
          interval: 0,
          rotate: cfg.xAxisRotate,
          width: LABEL_MAX_WIDTH,
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
        axisLine: {
          show: true,
          lineStyle: { color: "rgba(255,255,255,0.12)" },
        },
        axisLabel: { interval: 0, fontSize: 13, color: "#fff" },
      },
      series,
    };
  }, [data, t, i18n.language, nf, id, custom]);

  if (!option) return null;
  const chartHeight = height || custom?.[id]?.height || 400;

  return (
    <ReactECharts
      option={option}
      style={{
        width: "100%",
        height: chartHeight,
        alignSelf: "center",
      }}
      theme="dark"
    />
  );
}
