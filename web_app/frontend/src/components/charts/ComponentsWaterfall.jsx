import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const LABEL_MAX_WIDTH = 150;

export function ComponentsWaterfall({
  id,
  data,
  t,
  i18n,
  nf,
  custom,
  height,
  mode,
  showTotal,
  showLegend,
}) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    const cfg = custom?.[id] ?? {};
    const labelPrefix = cfg.label_prefix ?? "page.content";

    // --- Résolution des paramètres : prop > custom[id] > défaut
    const byMetric = (mode ?? cfg.mode ?? "global") === "byMetric";
    const withTotal = !byMetric && (showTotal ?? cfg.show_total ?? false);
    const withLegend = showLegend ?? cfg.show_legend ?? byMetric;

    // --- Couleurs
    const metrics = [...new Set(data.map((d) => d.metric))];
    const colorOf = (metric) => custom?.colors_metrics?.[metric];

    let cumulative = 0;
    let prevMetric = null;
    const rows = data.map((d) => {
      if (byMetric && d.metric !== prevMetric) cumulative = 0;
      prevMetric = d.metric;

      const start = cumulative;
      cumulative += d.value;
      return {
        name: String(t(`${labelPrefix}.${d.libelle}`, d.libelle)),
        metric: d.metric,
        value: d.value,
        start,
        end: cumulative,
        color: colorOf(d.metric, d.value),
      };
    });

    if (withTotal) {
      const total = data.reduce((s, d) => s + d.value, 0);
      rows.push({
        name: t("page.total"),
        metric: "__total",
        value: total,
        start: 0,
        end: total,
        // color:
      });
    }

    const names = rows.map((r) => r.name);

    // --- Série invisible (base)
    const baseSeries = {
      type: "bar",
      stack: "wf",
      silent: true,
      itemStyle: { color: "transparent" },
      emphasis: { disabled: true },
      tooltip: { show: false },
      data: rows.map((r) => Math.min(r.start, r.end)),
    };

    // --- Séries visibles : une par metric ou une seule
    const groups = byMetric
      ? [...metrics, ...(withTotal ? ["__total"] : [])]
      : ["__all"];
    const valueSeries = groups.map((g) => ({
      type: "bar",
      stack: "wf",
      name: byMetric ? String(t(`${labelPrefix}.${g}`, g)) : undefined,
      barCategoryGap: "20%",
      itemStyle: { borderRadius: [2, 2, 0, 0] },
      label: {
        show: true,
        position: "top",
        fontSize: 11,
        color: "#fff",
        formatter: (p) => nf.format(rows[p.dataIndex].value),
      },
      data: rows.map((r) =>
        !byMetric || r.metric === g
          ? { value: Math.abs(r.end - r.start), itemStyle: { color: r.color } }
          : null,
      ),
    }));

    return {
      backgroundColor: "transparent",
      aria: { enabled: true },
      toolbox: { right: 10, iconStyle: { borderColor: "#fff" } },
      legend: withLegend
        ? { top: 0, textStyle: { color: "#fff" } }
        : { show: false },
      grid: {
        top: 56,
        left: "6%",
        right: "6%",
        bottom: "3%",
        containLabel: true,
      },
      tooltip: {
        trigger: "axis",
        confine: true,
        formatter: (params) => {
          const p = params[0];
          const r = rows[p.dataIndex];
          if (!r) return "";
          return `Age:  ${r.name}<br/><b>${nf.format(r.value)}</b> ${t(
            cfg.tooltip ?? "page.tooltip_text",
          )}`;
        },
      },
      xAxis: {
        type: "category",
        data: names,
        axisTick: { show: false },
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
        type: "value",
        name: t(cfg.yAxisName ?? "page.axes_text"),
        nameLocation: "end",
        nameTextStyle: { fontSize: 14, color: "#fff" },
        axisLabel: {
          formatter: (v) => nf.format(v),
          fontSize: 14,
          color: "#fff",
        },
        splitLine: { lineStyle: { color: "rgba(255,255,255,0.12)" } },
      },
      series: [baseSeries, ...valueSeries],
    };
  }, [data, t, i18n.language, nf, id, custom, mode, showTotal, showLegend]);

  if (!option) return null;
  const chartHeight = height || custom?.[id]?.height;

  return (
    <ReactECharts
      option={option}
      style={{ width: "100%", height: chartHeight, alignSelf: "center" }}
      theme="dark"
    />
  );
}
