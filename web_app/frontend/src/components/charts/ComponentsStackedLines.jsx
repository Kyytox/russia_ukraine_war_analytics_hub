import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const sortNatural = (a, b) =>
  String(a).localeCompare(String(b), undefined, { numeric: true });

// Accepts "01-2022" (MM-YYYY), "2022-01" (YYYY-MM) or "2022-01-15" -> "2022-01"
// Internal format is YYYY-MM so alphabetical sort = chronological sort
const toYM = (v) => {
  if (v == null) return null;
  const s = String(v).trim();

  // MM-YYYY (or M-YYYY)
  let m = s.match(/^(\d{1,2})[-/](\d{4})$/);
  if (m) return `${m[2]}-${m[1].padStart(2, "0")}`;

  // YYYY-MM or YYYY-MM-DD
  m = s.match(/^(\d{4})[-/](\d{1,2})/);
  if (m) return `${m[1]}-${m[2].padStart(2, "0")}`;

  return null;
};

// Continuous list of months between min and max (avoids gaps on the axis)
const monthRange = (min, max) => {
  const out = [];
  let [y, m] = min.split("-").map(Number);
  const [yEnd, mEnd] = max.split("-").map(Number);
  while (y < yEnd || (y === yEnd && m <= mEnd)) {
    out.push(`${y}-${String(m).padStart(2, "0")}`);
    if (++m > 12) {
      m = 1;
      y++;
    }
  }
  return out;
};

// "month": metric is 1..12 ("3" or "03"); otherwise "month_year"
const detectXType = (data) => {
  const s = String(data?.[0]?.metric ?? "").trim();
  return /^\d{1,2}$/.test(s) && Number(s) >= 1 && Number(s) <= 12
    ? "month"
    : "month_year";
};

export function ComponentsStackedLines({
  tab,
  id,
  data,
  t,
  i18n,
  nf,
  custom,
  xType: xTypeProp, // "month" | "month_year" | "category"
}) {
  const option = useMemo(() => {
    if (!data?.length) return null;

    // Get chart configuration for the given id
    const cfg = custom?.[id] ?? {};
    const xType = xTypeProp ?? detectXType(data);

    // 1. Normalize `metric` so lookup keys match xKeys, depending on the mode
    const normalize = (m) =>
      xType === "month" ? Number(m) : xType === "month_year" ? toYM(m) : m;

    // 2. X axis
    let xKeys, xLabels;
    if (xType === "month") {
      xKeys = Array.from({ length: 12 }, (_, i) => i + 1);
      xLabels = xKeys.map((m) =>
        new Intl.DateTimeFormat(i18n.language, { month: "short" }).format(
          new Date(2000, m - 1, 1),
        ),
      );
    } else if (xType === "month_year") {
      const sorted = [
        ...new Set(data.map((d) => toYM(d.metric)).filter(Boolean)),
      ].sort();
      if (!sorted.length) return null;
      xKeys = monthRange(sorted[0], sorted[sorted.length - 1]);
      xLabels = xKeys.map((k) => {
        const [y, m] = k.split("-").map(Number);
        return new Intl.DateTimeFormat(i18n.language, {
          month: "short",
          year: "numeric",
        }).format(new Date(y, m - 1, 1));
      });
    } else {
      // Default: generic categories, translated if a label exists
      xKeys = [...new Set(data.map((d) => d.metric))].sort(sortNatural);
      xLabels = xKeys.map((k) => t(`${"page.content"}.${k}`, String(k)));
    }

    // 3. One series per `libelle`, lookup "libelle|x" -> value
    const seriesKeys = [...new Set(data.map((d) => d.libelle))].sort(
      sortNatural,
    );
    const lookup = new Map();
    data.forEach((d) => {
      const k = `${d.libelle}|${normalize(d.metric)}`;
      lookup.set(k, (lookup.get(k) ?? 0) + Number(d.value)); // sum if duplicates
    });

    const series = seriesKeys.map((key) => {
      const color = custom?.colors_metrics?.[key]; // optional fixed color, otherwise ECharts palette
      return {
        name: String(key),
        type: "line",
        data: xKeys.map((x) => lookup.get(`${key}|${x}`) ?? null), // null = no point
        showSymbol: false,
        symbolSize: 6,
        lineStyle: { width: 3, ...(color && { color }) },
        ...(color && { itemStyle: { color } }),
        emphasis: { focus: "series" },
      };
    });

    const isMY = xType === "month_year";

    return {
      aria: { enabled: true },
      grid: {
        top: "14%",
        left: "3%",
        right: "3%",
        bottom: "4%",
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
        confine: true,
        formatter: (params) => {
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
      xAxis: {
        type: "category",
        data: xLabels,
        boundaryGap: false,
        axisTick: { show: false },
        axisLabel: {
          color: "#fff",
          fontSize: 13,
          interval: isMY ? "auto" : 0, // 12 months: show all; month_year: let ECharts space labels
          rotate: isMY ? 30 : 0,
          hideOverlap: true,
        },
      },
      yAxis: {
        type: "value",
        name: t(`page.${tab}.axes_text`),
        nameLocation: "end",
        boundaryGap: [0, "10%"],
        nameTextStyle: custom?.common?.nameTextStyle,
        axisLabel: {
          formatter: (v) => nf.format(v),
          fontSize: 14,
          color: "#fff",
        },
        splitLine: custom?.common?.splitLine,
      },
      series,
    };
  }, [data, xTypeProp, t, i18n.language, nf, id, custom, tab]);

  if (!option) return null;

  return (
    <ReactECharts
      option={option}
      notMerge
      style={{ width: "100%", height: custom?.[id]?.height }}
    />
  );
}
