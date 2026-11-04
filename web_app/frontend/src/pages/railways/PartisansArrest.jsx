import { useApi } from "../../hooks/useApi.js";

import ChartState from "../../components/charts/ChartState.jsx";
import ChartHeader from "../../components/charts/ChartHeader.jsx";
import ChartModal from "../../components/charts/ChartModal.jsx";

import { ComponentsFunnel } from "../../components/charts/ComponentsFunnel.jsx";
import { ComponentsPie } from "../../components/charts/ComponentsPie.jsx";
import { ComponentsTreemap } from "../../components/charts/ComponentsTreemap.jsx";
import { ComponentsPictorialBar } from "../../components/charts/ComponentsPictorialBar.jsx";
import { ComponentsWaterfall } from "../../components/charts/ComponentsWaterfall.jsx";
import { ComponentsHeatmap } from "../../components/charts/ComponentsHeatmap.jsx";
import { ComponentsRadar } from "../../components/charts/ComponentsRadar.jsx";
import { ComponentsPunchCard } from "../../components/charts/ComponentsPunchCard.jsx";

const TAB = "partisans-arrest";

export default function PartisansArrest({ PAGE, t, i18n, nf, customCharts }) {
  const prefix = `${PAGE}/${TAB}`;
  const custom = customCharts?.[`${TAB}`] ?? {};

  console.log("PartisansArrest TAB", TAB);
  console.log("PartisansArrest custom", custom);
  console.log("PartisansArrest custom", customCharts?.[`${TAB}`]);

  const section1 = useApi(`/api/${prefix}/section1`);
  const section2 = useApi(`/api/${prefix}/section2`);
  const section3 = useApi(`/api/${prefix}/section3`);
  const section4 = useApi(`/api/${prefix}/section4`);
  const section5 = useApi(`/api/${prefix}/section5`);
  const section6 = useApi(`/api/${prefix}/section6`);
  const section7 = useApi(`/api/${prefix}/section7`);

  return (
    <>
      <section className="charts-grid">
        <h2 className="section-title span-12">{t(`page.${TAB}.title`)}</h2>

        <section className="chart span-6">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart1`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart1`} t={t}>
            <ChartState query={section1}>
              {(data, height) => (
                <ComponentsFunnel
                  id="chart1"
                  data={data.filter(
                    (d) =>
                      d.libelle === "total_sabotage" ||
                      d.libelle === "total_sabotage_arrested" ||
                      d.libelle === "total_partisans_arrested",
                  )}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                  height={height}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart2`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart2`} t={t}>
            <ChartState query={section1}>
              {(data, height) => (
                <ComponentsPie
                  id="chart2"
                  data={data.filter(
                    (d) =>
                      d.libelle === "total_sabotage_arrested" ||
                      d.libelle === "total_sabotage_no_arrested",
                  )}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                  height={height}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart3`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart3`} t={t}>
            <ChartState query={section1}>
              {(data, height) => (
                <ComponentsTreemap
                  id="chart3"
                  data={data.filter((d) => d.libelle === "applicable_laws")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                  height={height}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-4">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart4`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart4`} t={t}>
            <ChartState query={section1}>
              {(data, height) => (
                <ComponentsPictorialBar
                  id="chart4"
                  data={data.filter((d) => d.libelle === "partisans_age")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                  height={height}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart5`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart5`} t={t}>
            <ChartState query={section2}>
              {(data, height) => (
                <ComponentsWaterfall
                  id="chart5"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                  height={height}
                  mode="global"
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart6`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart6`} t={t}>
            <ChartState query={section3}>
              {(data, height) => (
                <ComponentsWaterfall
                  id="chart6"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                  height={height}
                  mode="byMetric"
                  palette={["#1f7a9e", "#5a9e5f", "#d4f01f", "#ff8c1a"]}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart7`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart7`} t={t}>
            <ChartState query={section4}>
              {(data, height) => (
                <ComponentsPunchCard
                  id="chart7"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                  height={height}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>
      </section>
    </>
  );
}
