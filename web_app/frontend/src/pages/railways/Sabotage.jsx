import { useState } from "react";
import { useApi } from "../../hooks/useApi.js";
import { useConfigCharts } from "../../hooks/useConfigCharts.js";

import ChartState from "../../components/charts/ChartState.jsx";
import ChartHeader from "../../components/charts/ChartHeader.jsx";
import ChartModal from "../../components/charts/ChartModal.jsx";

import { ComponentsMap } from "../../components/charts/ComponentsMap.jsx";
import { ComponentsStackedLines } from "../../components/charts/ComponentsStackedLines.jsx";
import { ComponentsBarStackedVertical } from "../../components/charts/ComponentsBarStackedVertical.jsx";
import { ComponentsLinesAreaStacked } from "../../components/charts/ComponentsLinesAreaStacked.jsx";
import { ComponentsPie } from "../../components/charts/ComponentsPie.jsx";
import { ComponentsBarVertical } from "../../components/charts/ComponentsBarVertical.jsx";
import { ComponentsBarStackedHorizontal } from "../../components/charts/ComponentsBarStackedHorizontal.jsx";
import { ComponentsTreemap } from "../../components/charts/ComponentsTreemap.jsx";
import { ComponentsScatterGroups } from "../../components/charts/ComponentsScatterGroups.jsx";

const TAB = "sabotage";

export default function Sabotage({ PAGE, t, i18n, nf, customCharts }) {
  const prefix = `${PAGE}/${TAB}`;
  const cfg = useConfigCharts(PAGE, TAB);

  const section1 = useApi(`/api/${prefix}/section1`);
  const section2 = useApi(`/api/${prefix}/section2`);
  const section3 = useApi(`/api/${prefix}/section3`);
  const section4 = useApi(`/api/${prefix}/section4`);
  const section5 = useApi(`/api/${prefix}/section5`);
  const section6 = useApi(`/api/${prefix}/section6`);
  const section7 = useApi(`/api/${prefix}/section7`);
  const section8 = useApi(`/api/${prefix}/section8`);

  console.log("sections ", section5, section6);

  const [hideMoscowChart12, setHideMoscowChart12] = useState(false);
  const [hideMoscowChart13, setHideMoscowChart13] = useState(false);

  return (
    <>
      <section className="charts-grid">
        <h2 className="section-title span-12">{t(`page.${TAB}.title`)}</h2>
        <section className="chart span-4">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart1`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart1`} t={t}>
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarVertical
                  tab={TAB} id="chart1"
                  data={data.filter((d) => d.libelle === "sabotage_year")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart2`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart2`} t={t}>
            <ChartState query={section2}>
              {(data) => (
                <ComponentsStackedLines
                  tab={TAB} id="chart2"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart3`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart3`} t={t}>
            <ChartState query={section3}>
              {(data) => (
                <ComponentsLinesAreaStacked
                  tab={TAB} id="chart3"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-4">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart4`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart4`} t={t}>
            <ChartState query={section1}>
              {(data) => (
                <ComponentsPie
                  tab={TAB} id="chart4"
                  data={data.filter((d) => d.libelle === "prtsn_grp")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart5`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart5`} t={t}>
            <ChartState query={section4}>
              {(data) => (
                <ComponentsScatterGroups
                  tab={TAB} id="chart5"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart9`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart9`} t={t}>
            <ChartState query={section7}>
              {(data) => (
                <ComponentsBarStackedVertical
                  tab={TAB} id="chart9"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>
        <section className="chart span-4">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart8`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart8`} t={t}>
            <ChartState query={section1}>
              {(data) => (
                <ComponentsPie
                  tab={TAB} id="chart8"
                  data={data.filter((d) => d.libelle === "dmg_eqp_sabotage")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart6`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart6`} t={t}>
            <ChartState query={section5}>
              {(data) => (
                <ComponentsTreemap
                  tab={TAB} id="chart6"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                  gradient={false}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-4">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart7`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart7`} t={t}>
            <ChartState query={section5}>
              {(data) => (
                <ComponentsBarStackedVertical
                  tab={TAB} id="chart7"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart10`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart10`} t={t}>
            <ChartState query={section6}>
              {(data) => (
                <ComponentsTreemap
                  tab={TAB} id="chart10"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                  gradient={false}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-4">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart11`} t={t} />
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart11`} t={t}>
            <ChartState query={section6}>
              {(data) => (
                <ComponentsBarStackedVertical
                  tab={TAB} id="chart11"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>
        <section className="chart span-12">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart12`} t={t} />
          <div className="filter-controls">
            <label className="filter-checkbox">
              <input
                type="checkbox"
                checked={hideMoscowChart12}
                onChange={(e) => setHideMoscowChart12(e.target.checked)}
                className="filter-input"
              />
              <span className="filter-label">
                {t(`page.${TAB}.hide_moscow`) || "Hide Moscow"}
              </span>
            </label>
          </div>
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart12`} t={t}>
            <ChartState query={section1}>
              {(data) => {
                const filteredData = hideMoscowChart12
                  ? data.filter(
                      (d) =>
                        d.libelle === "region_sabotage" &&
                        d.metric !== "Moscow" &&
                        d.metric !== "Москва",
                    )
                  : data.filter((d) => d.libelle === "region_sabotage");
                return (
                  <ComponentsMap
                    tab={TAB} id="chart12"
                    data={filteredData}
                    t={t}
                    i18n={i18n}
                    nf={nf}
                    custom={cfg}
                  />
                );
              }}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart13`} t={t} />
          <div className="filter-controls">
            <label className="filter-checkbox">
              <input
                type="checkbox"
                checked={hideMoscowChart13}
                onChange={(e) => setHideMoscowChart13(e.target.checked)}
                className="filter-input"
              />
              <span className="filter-label">
                {t(`page.${TAB}.hide_moscow`) || "Hide Moscow"}
              </span>
            </label>
          </div>
          <ChartModal pageKey={TAB} chartKey={`page.${TAB}.chart13`} t={t}>
            <ChartState query={section8}>
              {(data) => {
                const filteredData = hideMoscowChart13
                  ? data.filter(
                      (d) => d.libelle !== "Moscow" && d.libelle !== "Москва",
                    )
                  : data;
                return (
                  <ComponentsBarStackedHorizontal
                    tab={TAB} id="chart13"
                    data={filteredData}
                    t={t}
                    i18n={i18n}
                    nf={nf}
                    custom={cfg}
                  />
                );
              }}
            </ChartState>
          </ChartModal>
        </section>
      </section>
    </>
  );
}
