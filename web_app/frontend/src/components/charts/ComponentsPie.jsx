import { useMemo } from "react";
import ReactECharts from "echarts-for-react";

export function ComponentsPie({ id, data, t, i18n, nf, custom }) {
  const option = useMemo(() => {
    const items = data.map((d) => ({
      name: t(`page.content.${d.metric}`, d.metric),
      value: d.count,
    }));
    const total = items.reduce((sum, d) => sum + d.value, 0);

    return {
      title: {
        text: `${nf.format(total)}\n${t(`page.${id}.text`)}`,
        left: "center",
        top: "center",
        textStyle: {
          fontSize: 17,
          fontWeight: "bold",
          color: "#fff",
        },
      },

      toolbox: {
        right: 10,
        iconStyle: { borderColor: "#fff" },
        feature: {
          saveAsImage: {},
        },
      },

      tooltip: {
        trigger: "item",
        valueFormatter: (value) => nf.format(value),
        feature: {
          dataZoom: { xAxisIndex: "none" },
          saveAsImage: {},
        },
      },
      series: [
        {
          type: "pie",
          radius: ["35%", "90%"],
          padAngle: 2,
          itemStyle: {
            borderRadius: 4,
          },
          label: {
            show: true,
            position: "inside",
            fontSize: 14,
            formatter: "{b}\n{d}%",
          },
          data: items,
          color: items.map((d) => custom.colors_metrics[d.name] || "#1c2430"),
        },
      ],
    };
  }, [data, t, i18n.language, nf, custom, id]);

  return (
    <ReactECharts
      option={option}
      style={{
        width: "100%",
        height: "100%",
        maxWidth: custom?.[id]?.maxWidth,
        minHeight: custom?.[id]?.minHeight,
        alignSelf: "center",
      }}
    />
  );
}
