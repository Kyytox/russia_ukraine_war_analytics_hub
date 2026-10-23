import ReactECharts from "echarts-for-react";
import { chartThemeLight } from "../theme/chartTheme";

/**
 * Exemple de composant avec thème ECharts
 * À adapter selon tes besoins
 */
export default function ChartExample() {
  // Option du graphique (données + configuration spécifique)
  const chartOption = {
    title: {
      text: "Example Chart",
      left: "center",
    },
    tooltip: {
      trigger: "axis",
    },
    legend: {
      data: ["Series 1", "Series 2"],
      top: 30,
    },
    xAxis: {
      type: "category",
      data: ["Jan", "Feb", "Mar", "Apr", "May", "Jun"],
    },
    yAxis: {
      type: "value",
    },
    series: [
      {
        name: "Series 1",
        data: [120, 132, 101, 134, 90, 230],
        type: "line",
      },
      {
        name: "Series 2",
        data: [220, 182, 191, 234, 290, 330],
        type: "line",
      },
    ],
  };

  return (
    <div style={{ width: "100%", height: "400px" }}>
      <ReactECharts
        option={chartOption}
        theme={chartThemeLight}  {/* ← Passe le thème ici */}
        style={{ height: "100%" }}
      />
    </div>
  );
}
