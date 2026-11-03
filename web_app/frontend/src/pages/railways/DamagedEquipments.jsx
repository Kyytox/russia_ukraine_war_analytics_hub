import { useState } from "react";
import { useApi } from "../../hooks/useApi.js";
import { usePageTranslation } from "../../hooks/usePageTranslation.js";
import { getPageFromPath } from "../../utils/common.js";

import ChartState from "../../components/charts/ChartState.jsx";
import ChartHeader from "../../components/charts/ChartHeader.jsx";
import ChartModal from "../../components/charts/ChartModal.jsx";

import { ComponentsMap } from "../../components/charts/ComponentsMap.jsx";
import { ComponentsStackedLines } from "../../components/charts/ComponentsStackedLines.jsx";
import { ComponentsBarStackedVertical } from "../../components/charts/ComponentsBarStackedVertical.jsx";
import { ComponentsLineArea } from "../../components/charts/ComponentsLineArea.jsx";
import { ComponentsHeatmap } from "../../components/charts/ComponentsHeatmap.jsx";
import { ComponentsTree } from "../../components/charts/ComponentsTree.jsx";
import { ComponentsPie } from "../../components/charts/ComponentsPie.jsx";
import { ComponentsBarVertical } from "../../components/charts/ComponentsBarVertical.jsx";
import { ComponentsBarHorizontal } from "../../components/charts/ComponentsBarHorizontal.jsx";
import { ComponentsBarStackedHorizontal } from "../../components/charts/ComponentsBarStackedHorizontal.jsx";
import { ComponentsTreemap } from "../../components/charts/ComponentsTreemap.jsx";
import { ComponentsSunburst } from "../../components/charts/ComponentsSunburst.jsx";
import { ComponentsBarGrouped } from "../../components/charts/ComponentsBarGrouped.jsx";
import { ComponentsSankey } from "../../components/charts/ComponentsSankey.jsx";

const TAB = "damaged-equipments";

export default function DamagedEquipments({ PAGE, t, i18n, nf, customCharts }) {
  const prefix = `${PAGE}/${TAB}`;
  const custom = customCharts?.[`${TAB}`] ?? {}; // Get custom chart config for this tab

  const [hideMoscowChart7, setHideMoscowChart7] = useState(false);

  const section1 = useApi(`/api/${prefix}/section1`);
  const section2 = useApi(`/api/${prefix}/section2`);
  const section3 = useApi(`/api/${prefix}/section3`);
  const section4 = useApi(`/api/${prefix}/section4`);
  const section5 = useApi(`/api/${prefix}/section5`);
  const section6 = useApi(`/api/${prefix}/section6`);

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
                data={data.filter((d) => d.libelle === "dmg_eqp")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              height={height} />
            )}
            </ChartState>
          </ChartModal>
        </section>
        <section className="chart span-8">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart2`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart2`} t={t}>
            <ChartState query={section2}>
            {(data, height) => (
              <ComponentsStackedLines
                id="chart2"
                data={data}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
                xTypeProp="month_year"
              height={height} />
            )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-4">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart3`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart3`} t={t}>
            <ChartState query={section4}>
            {(data, height) => (
              <ComponentsBarGrouped
                id="chart3"
                data={data}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              height={height} />
            )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart4`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart4`} t={t}>
            <ChartState query={section3}>
            {(data, height) => (
              <ComponentsBarStackedVertical
                id="chart4"
                data={data}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              height={height} />
            )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart5`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart5`} t={t}>
            <ChartState query={section5}>
            {(data, height) => (
              <ComponentsSunburst
                id="chart5"
                data={data}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              height={height} />
            )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart6`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart6`} t={t}>
            <ChartState query={section5}>
            {(data, height) => (
              <ComponentsSankey
                id="chart6"
                data={data}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              height={height} />
            )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart7`} t={t} />
          <div className="filter-controls">
            <label className="filter-checkbox">
              <input
                type="checkbox"
                checked={hideMoscowChart7}
                onChange={(e) => setHideMoscowChart7(e.target.checked)}
                className="filter-input"
              />
              <span className="filter-label">
                {t(`page.${TAB}.hide_moscow`) || "Hide Moscow"}
              </span>
            </label>
          </div>
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart7`} t={t}>
            <ChartState query={section6}>
            {(data, height) => {
              const filteredData = hideMoscowChart7
                ? data.filter(
                    (d) => d.libelle !== "Moscow" && d.libelle !== "Москва",
                  )
                : data;
              return (
                <ComponentsBarStackedHorizontal
                  id="chart7"
                  data={filteredData}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                height={height} />
              );
            }}
            </ChartState>
          </ChartModal>
        </section>
      </section>
    </>
  );
}
