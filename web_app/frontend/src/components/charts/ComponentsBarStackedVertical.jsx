import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const TOTAL = "__total__";

export function ComponentsBarStackedVertical({
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

    const limit = custom?.[id]?.limit ?? null;
    var libelles = null;

    if (limit === null) {
      var libelles = Object.keys(totals);
    } else {
      var libelles = Object.keys(totals).slice(0, limit);
    }

    const lookup = new Map(
      data.map((d) => [`${d.libelle}|${d.metric}`, d.count]),
    );

    const countTotals = {};
    data.forEach((d) => {
      if (libelles.includes(d.libelle)) {
        countTotals[d.metric] = (countTotals[d.metric] ?? 0) + d.count;
      }
    });
    const metrics = Object.keys(countTotals).sort(
      (a, b) => countTotals[b] - countTotals[a],
    );

    const max = Math.max(...libelles.map((w) => totals[w]), 1);
    const MIN_RATIO = 0.02; // segment < 2 % de la plus grande barre : pas de label dedans
    const metricName = (c) => t(`metric.${c}`, c);

    return {
      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: { dataZoom: { yAxisIndex: "none" }, saveAsImage: {} },
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        formatter: (params) => {
          const rows = params.filter(
            (p) => p.seriesName !== TOTAL && p.value > 0,
          );
          const sum = rows.reduce((s, p) => s + p.value, 0);
          const tooltipLabel = t(custom[id].tooltip);
          return [
            `<b>${params[0].name}</b> : ${nf.format(sum)} ${tooltipLabel}`,
            ...rows.map(
              (p) =>
                `${p.marker} ${metricName(p.seriesName)} : ${nf.format(p.value)}`,
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
      grid: {
        bottom: "6%",
        top: "8%",
        left: "6%",
        right: "6%",
      },

      xAxis: {
        type: "category",
        data: libelles.map((w) => t(`page.chart-names.${w}`, w)),
        axisLabel: {
          fontSize: 12,
          color: "#fff",
          rotate: 45,
          interval: 0, // force l'affichage de tous les labels
        },
      },
      yAxis: {
        type: "value",
        name: t(`page.axes_text`),
        nameLocation: "middle",
        nameGap: 50,
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
          itemStyle: { color: custom?.colors_metrics?.[c] ?? undefined },
          label: {
            show: true,
            fontSize: 11,
            // color: "#fff",
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
            show: custom?.[id]?.show_totals ?? true,
            position: "outside",
            color: "#fff",
            fontSize: 12,
            formatter: (p) => nf.format(totals[libelles[p.dataIndex]]),
          },
        },
      ],
    };
  }, [data, t, i18n.language, nf, id, custom]);

  return (
    <ReactECharts
      option={option}
      style={{
        width: "100%",
        height: custom?.[id]?.height ?? "400px",
      }}
    />
  );
}
