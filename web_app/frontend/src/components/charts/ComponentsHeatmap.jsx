import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

const CELL_SIZE = 16; // hauteur d'une case (un jour)
const YEAR_BLOCK = CELL_SIZE * 7 + 50; // hauteur d'un calendrier + espacement
const TOP_OFFSET = 50;
const BOTTOM_OFFSET = 60; // place pour la légende (visualMap)

export function ComponentsHeatmap({ id, data, t, i18n, nf, custom }) {
  const { option, height } = useMemo(() => {
    const items = data.map((d) => ({ date: d.metric, value: d.count }));

    const color = custom?.[id]?.colors || "#7F557D";
    const max = Math.max(...items.map((d) => d.value), 1);

    // années présentes dans les données, triées
    const years = [...new Set(items.map((d) => d.date.slice(0, 4)))].sort();

    // libellés des mois / jours traduits selon la langue courante
    const lang = i18n.language;
    const monthNames = Array.from({ length: 12 }, (_, m) =>
      new Intl.DateTimeFormat(lang, { month: "short" }).format(
        new Date(2022, m, 1),
      ),
    );
    // dayLabel : 0 = dimanche ... 6 = samedi (la semaine commence le lundi)
    const dayNames = Array.from({ length: 7 }, (_, d) =>
      new Intl.DateTimeFormat(lang, { weekday: "short" }).format(
        new Date(2022, 0, 2 + d), // 2022-01-02 est un dimanche
      ),
    );

    const calendars = years.map((year, i) => ({
      top: TOP_OFFSET + i * YEAR_BLOCK,
      left: 60,
      right: 30,
      orient: custom?.[id]?.orient || "horizontal",
      cellSize: ["auto", CELL_SIZE],
      range: year,
      itemStyle: {
        borderWidth: 2,
        borderColor: "#0f1a20",
        color: "rgba(255,255,255,0.06)", // jours sans donnée
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
      height: TOP_OFFSET + years.length * YEAR_BLOCK + BOTTOM_OFFSET,
      option: {
        toolbox: {
          right: 10,
          iconStyle: { borderColor: "#fff" },
          feature: {
            saveAsImage: {},
          },
        },
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
          bottom: 10,
          itemWidth: 14,
          itemHeight: 160,
          text: [nf.format(max), "0"],
          textStyle: { color: "#fff", fontSize: 12 },
          inRange: { color: ["rgba(255,255,255,0.06)", color] },
          formatter: (value) => nf.format(Math.round(value)),
        },
        calendar: calendars,
        series,
      },
    };
  }, [data, t, i18n.language, nf, id, custom]);

  return (
    <ReactECharts
      option={option}
      style={{
        width: "100%",
        height: custom?.[id]?.height || height,
        alignSelf: "center",
      }}
    />
  );
}
