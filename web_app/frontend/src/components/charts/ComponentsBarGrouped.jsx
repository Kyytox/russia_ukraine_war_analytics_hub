import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const sortNatural = (a, b) =>
  String(a).localeCompare(String(b), undefined, { numeric: true });

export function ComponentsBarGrouped({ tab, id, data, t, nf, custom }) {
  const option = useMemo(() => {
    if (!data?.length) return null;
    const cfg = custom?.[id] ?? {};

    // 1. Categories (axis) = metric, groups (bars side by side) = libelle
    const totals = new Map();
    data.forEach((d) => {
      totals.set(d.metric, (totals.get(d.metric) ?? 0) + Number(d.value));
    });

    let categories = [...totals.keys()];
    categories = categories.sort(sortNatural);

    const groups = [...new Set(data.map((d) => d.libelle))].sort(sortNatural);

    // 2. Lookup "libelle|metric" -> value
    const lookup = new Map();
    data.forEach((d) => {
      const k = `${d.libelle}|${d.metric}`;
      lookup.set(k, (lookup.get(k) ?? 0) + Number(d.value)); // sum if duplicates
    });

    // 3. One series per group
    const series = groups.map((g) => {
      const color = custom?.colors_metrics?.[g];
      return {
        name: String(g),
        type: "bar",
        data: categories.map((c) => lookup.get(`${g}|${c}`) ?? null),
        itemStyle: {
          borderRadius: [4, 4, 0, 0],
          ...(color && { color }),
        },
        label: {
          show: true,
          fontSize: 11,
          position: "top",
          color: "#fff",
          formatter: (p) => (p.value == null ? "" : nf.format(p.value)),
        },
        emphasis: { focus: "series" },
      };
    });

    // Labels on the category axis (translated if a translation exists)
    const categoryAxis = {
      type: "category",
      data: categories.map((c) =>
        t(`${cfg.label_prefix ?? "page.content"}.${c}`, String(c)),
      ),
      axisTick: { show: false },
      axisLabel: {
        color: "#fff",
        fontSize: 11,
        interval: 0,
        hideOverlap: true,
        rotate: 30,
      },
    };

    const valueAxis = {
      type: "value",
      name: t(cfg.axis_text ?? `page.${tab}.axes_text`),
      nameLocation: "end",
      nameTextStyle: {
        fontSize: 14,
        color: "#fff",
        align: "left",
      },
      axisLabel: {
        formatter: (v) => nf.format(v),
        fontSize: 14,
        color: "#fff",
      },
      // splitLine: {
      //   lineStyle: {
      //     color: "rgba(255,255,255,0.32)",
      //   },
      // },
    };

    return {
      // aria: { enabled: true },
      grid: {
        top: "15%",
        left: "4%",
        right: "4%",
        bottom: "2%",
        containLabel: true,
      },
      legend: {
        type: "scroll",
        top: 10,
        left: 10,
        textStyle: { color: "#fff" },
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        confine: true,
        formatter: (params) => {
          // Hide empty values, sort by value (descending)
          const rows = params
            .filter((p) => p.value != null)
            .sort((a, b) => b.value - a.value);
          return [
            `<b>${params[0].axisValueLabel}</b>`,
            ...rows.map(
              (p) =>
                `${p.marker} ${p.seriesName} : <b>${nf.format(p.value)}</b>`,
            ),
          ].join("<br/>");
        },
      },
      xAxis: categoryAxis,
      yAxis: valueAxis,
      series,
    };
  }, [data, t, nf, id, custom, tab]);

  if (!option) return null;

  return (
    <ReactECharts
      option={option}
      notMerge
      style={{ width: "100%", height: custom?.[id]?.height }}
    />
  );
}
