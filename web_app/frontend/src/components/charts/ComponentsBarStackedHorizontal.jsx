import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import { getMetricColor } from "../../utils/common.js";

const TOTAL = "__total__"; // série invisible qui porte le total au bout de chaque barre

export function ComponentsBarStackedHorizontal({
  id,
  data,
  t,
  i18n,
  nf,
  custom,
}) {
  const option = useMemo(() => {
    const totals = {};
    data.forEach((d) => {
      totals[d.libelle] = (totals[d.libelle] ?? 0) + d.count;
    });

    const libelles = Object.keys(totals)
      .sort((a, b) => totals[a] - totals[b])
      .slice(-custom?.[id]?.limit ?? Infinity);

    // lookup table for quick access to counts by libelle and metric
    const lookup = new Map(
      data.map((d) => [`${d.libelle}|${d.metric}`, d.count]),
    );

    const metricTotals = {};
    data.forEach((d) => {
      if (totals[d.libelle] !== undefined && libelles.includes(d.libelle)) {
        metricTotals[d.metric] = (metricTotals[d.metric] ?? 0) + d.count;
      }
    });
    const metrics = Object.keys(metricTotals).sort(
      (a, b) => metricTotals[b] - metricTotals[a],
    );

    const max = Math.max(...libelles.map((w) => totals[w]), 1);
    const MIN_RATIO = 0.02; // segment < 4 % de la plus grande barre : pas de label dedans
    const metricName = (c) => t(`metrics.${c}`, c);

    return {
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: { dataZoom: { xAxisIndex: "none" }, saveAsImage: {} },
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        formatter: (params) => {
          const rows = params.filter(
            (p) => p.seriesName !== TOTAL && p.value > 0,
          );
          const sum = rows.reduce((s, p) => s + p.value, 0);
          return [
            `<b>${params[0].name}</b> : ${nf.format(sum)} ${t(custom[id].tooltip)}`,
            ...rows.map(
              (p) => `${p.marker} ${p.seriesName} : ${nf.format(p.value)}`,
            ),
          ].join("<br/>");
        },
      },

      legend: {
        top: 0,
        left: "center",
        textStyle: { fontSize: 13, color: "#fff" },
        data: metrics.map((c) => metricName(c)),
      },
      yAxis: {
        type: "category",
        data: libelles.map((w) => t(`page.content.${w}`, w)),
        axisLabel: { fontSize: 13, color: "#fff" },
      },
      xAxis: {
        type: "value",
        name: t(`page.axes_text`),
        nameLocation: "middle",
        nameGap: 32,
        nameTextStyle: { fontSize: 14, color: "#fff" },
        axisLabel: {
          formatter: (v) => nf.format(v),
          fontSize: 14,
          color: "#fff",
        },
        boundaryGap: [0, "8%"],
      },

      series: [
        ...metrics.map((c) => ({
          name: metricName(c),
          type: "bar",
          stack: "total",
          emphasis: { focus: "series" },
          itemStyle: {
            color: getMetricColor(
              custom.colors_metrics,
              c,
              custom[id].default_colors,
            ),
          },
          label: {
            show: true,
            fontSize: 11,
            color: "#fff",
            formatter: (p) =>
              p.value / max < MIN_RATIO ? "" : nf.format(p.value),
          },
          data: libelles.map((w) => lookup.get(`${w}|${c}`) ?? 0),
        })),
        {
          name: TOTAL,
          type: "bar",
          stack: "total",
          silent: true,
          itemStyle: { color: "transparent" },
          data: libelles.map(() => 0),
          label: {
            show: false,
            position: "right",
            color: "#fff",
            fontSize: 12,
            formatter: (p) => nf.format(totals[libelles[p.dataIndex]]),
          },
        },
      ],
    };
  }, [data, t, i18n.language, nf, id]);

  return (
    <ReactECharts
      option={option}
      style={{ width: "100%", height: custom?.[id]?.height }}
    />
  );
}
