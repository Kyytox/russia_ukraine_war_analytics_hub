import { useState } from "react";
import { useApi } from "../../hooks/useApi.js";
import { usePageTranslation } from "../../hooks/usePageTranslation.js";
import { getPageFromPath } from "../../utils/common.js";

import ChartState from "../../components/charts/ChartState.jsx";
import ChartHeader from "../../components/charts/ChartHeader.jsx";
import ChartModal from "../../components/charts/ChartModal.jsx";

import { ComponentsBarStackedVertical } from "../../components/charts/ComponentsBarStackedVertical.jsx";
import { ComponentsPie } from "../../components/charts/ComponentsPie.jsx";

const TAB = "collisions";

export default function Collisions({ PAGE, t, i18n, nf, customCharts }) {
  const prefix = `${PAGE}/${TAB}`;
  const custom = customCharts?.[`${TAB}`] ?? {}; // Get custom chart config for this tab

  const section1 = useApi(`/api/${prefix}/section1`);
  const section2 = useApi(`/api/${prefix}/section2`);
  const section3 = useApi(`/api/${prefix}/section3`);

  return (
    <>
      <section className="charts-grid">
        <h2 className="section-title span-12">{t(`page.${TAB}.title`)}</h2>
        <section className="chart span-4">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart1`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart1`} t={t}>
            <ChartState query={section1}>
              {(data, height) => (
                <ComponentsPie
                  id="chart1"
                  data={data.filter((d) => d.libelle === "coll_with")}
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
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart4`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart4`} t={t}>
            <ChartState query={section2}>
              {(data, height) => (
                <ComponentsBarStackedVertical
                  id="chart4"
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

        <section className="chart span-4">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart2`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart2`} t={t}>
            <ChartState query={section1}>
              {(data, height) => (
                <ComponentsPie
                  id="chart2"
                  data={data.filter((d) => d.libelle === "dmg_eqp_coll")}
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
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart5`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart5`} t={t}>
            <ChartState query={section3}>
              {(data, height) => (
                <ComponentsBarStackedVertical
                  id="chart5"
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
