import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const YEAR_BLOCK = 12 * 7 + 50;
const TOP_OFFSET = 50;

export function ComponentsHeatmap({ id, data, t, i18n, nf, custom, height }) {
  const { option } = useMemo(() => {
    const lang = i18n.language;
    const items = data.map((d) => ({ date: d.metric, value: d.value }));

    const color = custom?.[id]?.colors || "#A84657";
    const max = Math.max(...items.map((d) => d.value), 1);

    // années présentes dans les données, triées
    const years = [...new Set(items.map((d) => d.date.slice(0, 4)))].sort();

    // libellés des mois / jours traduits selon la langue courante
    const monthNames = Array.from({ length: 12 }, (_, m) =>
      new Intl.DateTimeFormat(lang, { month: "short" }).format(
        new Date(2022, m, 1),
      ),
    );
    // dayLabel : 0 = dimanche ... 6 = samedi (la semaine commence le lundi)
    const dayNames = Array.from({ length: 7 }, (_, d) =>
      new Intl.DateTimeFormat(lang, { weekday: "short" }).format(
        new Date(2022, 0, 2 + d),
      ),
    );

    const calendars = years.map((year, i) => ({
      top: TOP_OFFSET + i * YEAR_BLOCK,
      left: 60,
      right: 30,
      bottom: 40,
      orient: custom?.[id]?.orient || "horizontal",
      range: year,
      itemStyle: {
        borderWidth: 2,
        borderColor: "#0f1a20",
        color: "rgba(255,255,255,0.05)", // 0 data
      },
      splitLine: { show: false },
      yearLabel: {
        show: true,
        position: "left",
        margin: 40,
        color: "#fff",
        fontSize: 14,
      },
      monthLabel: {
        nameMap: monthNames,
        color: "#fff",
        fontSize: 12,
      },
      dayLabel: {
        firstDay: 1,
        nameMap: dayNames,
        color: "#fff",
        fontSize: 11,
      },
    }));

    const series = years.map((year, i) => ({
      type: "heatmap",
      coordinateSystem: "calendar",
      calendarIndex: i,
      data: items
        .filter((d) => d.date.startsWith(year))
        .map((d) => [d.date, d.value]),
    }));

    return {
      option: {
        tooltip: {
          formatter: (p) => `${p.value[0]}<br/><b>${nf.format(p.value[1])}</b>`,
        },
        visualMap: {
          type: "continuous",
          min: 0,
          max,
          calculable: false,
          orient: custom?.[id]?.orient || "horizontal",
          left: "center",
          bottom: -15,
          text: [nf.format(max), "0"],
          textStyle: { color: "#fff", fontSize: 12 },
          inRange: { color: ["rgba(255,255,255,0.2)", color] },
          formatter: (value) => nf.format(Math.round(value)),
        },
        calendar: calendars,
        series,
      },
    };
  }, [data, t, i18n.language, nf, id, custom, height]);

  const chartHeight = height || custom?.[id]?.height;

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
