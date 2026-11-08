import { useMemo } from "react";
import * as echarts from "echarts";
import ReactECharts from "echarts-for-react";
import { useApi } from "../../hooks/useApi";

const DEFAULT_GRADIENT = [
  "#c4aa5e",
  "#c2885f",
  "#bf635a",
  "#b85462",
  "#a85199",
  "#50499c",
];

import geo from "./ru_region.json";

// enregistrée une seule fois, au chargement du module
// echarts.registerMap("ru-regions", geo);

export function ComponentsMap({ tab, id, data, t, i18n, nf, custom }) {
  // la carte est enregistrée une seule fois, quand le fichier est chargé
  const mapReady = useMemo(() => {
    if (!geo) return false;
    echarts.registerMap("ru-regions", geo);
    return true;
  }, [geo, tab]);

  const option = useMemo(() => {
    if (!mapReady || !data?.length) return null;
    const cfg = custom?.[id] ?? {};
    const values = data.map((d) => d.value);

    return {
      aria: { enabled: true },
      tooltip: {
        confine: true,
        formatter: (p) =>
          p.value == null || Number.isNaN(p.value)
            ? p.name
            : `${p.name}<br/><b>${nf.format(p.value)}</b> ${t(`page.${tab}.tooltip_text`)}`,
      },
      visualMap: {
        min: Math.min(...values),
        max: Math.max(...values),
        calculable: true,
        orient: "horizontal",
        left: "center",
        bottom: 10,
        // textStyle: { color: "#fff" },
        inRange: { color: cfg.gradient ?? DEFAULT_GRADIENT },
      },
      series: [
        {
          type: "map",
          map: "ru-regions",
          nameProperty: "name",
          roam: true,
          top: 10,
          bottom: 60,
          data: data.map((d) => ({ name: d.metric, value: d.value })),
          label: { show: false },
          itemStyle: {
            areaColor: "#2a303b",
            borderColor: "#0d1117",
            borderWidth: 0.5,
          },
          emphasis: {
            label: { show: false },
            itemStyle: { areaColor: "#ff8a00" },
          },
        },
      ],
    };
  }, [data, mapReady, t, i18n.language, nf, id, custom, tab]);

  if (!option) return null;

  const chartHeight = custom?.[id]?.height;

  return (
    <ReactECharts
      option={option}
      notMerge
      style={{ width: "100%", height: chartHeight }}
    />
  );
}
