import { useState } from "react";
import { useApi } from "../../hooks/useApi.js";
import { formatKpiValue } from "../../utils/common.js";
import { useConfigCharts } from "../../hooks/useConfigCharts.js";

import ChartState from "../../components/charts/ChartState.jsx";
import ChartHeader from "../../components/charts/ChartHeader.jsx";
import ChartModal from "../../components/charts/ChartModal.jsx";

import { ComponentsHeatmap } from "../../components/charts/ComponentsHeatmap.jsx";
import { ComponentsMap } from "../../components/charts/ComponentsMap.jsx";
import { ComponentsStackedLines } from "../../components/charts/ComponentsStackedLines.jsx";
import { ComponentsBarVertical } from "../../components/charts/ComponentsBarVertical.jsx";
import { ComponentsTreemap } from "../../components/charts/ComponentsTreemap.jsx";

export default function Overview({ PAGE, t, i18n, nf }) {
  const TAB = "overview";
  const prefix = `${PAGE}/${TAB}`;
  const cfg = useConfigCharts(PAGE, TAB); // Get custom chart config for this tab

  const [hideMoscowChart7, setHideMoscowChart7] = useState(true);
  const [hideMoscowChart8, setHideMoscowChart8] = useState(true);

  const kpi = useApi(`/api/${prefix}/kpi`);
  const section1 = useApi(`/api/${prefix}/section1`);
  const section2 = useApi(`/api/${prefix}/section2`);
  const section3 = useApi(`/api/${prefix}/section3`);
  const section4 = useApi(`/api/${prefix}/section4`);

  return (
    <>
      <section className="kpis">
        <ChartState query={kpi} height={96}>
          {(items) =>
            items.map((item) => (
              <div key={item.id} className="placeholder h-kpi">
                <span>{formatKpiValue(item.value, i18n.language)}</span>
                <h3>{t(`page.${TAB}.${item.id}`)}</h3>
              </div>
            ))
          }
        </ChartState>
      </section>

      <section className="charts-grid">
        <h2 className="section-title span-12">{t(`page.${TAB}.title`)}</h2>
        <section className="chart span-4">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart1`} t={t} />
          <ChartModal
            pageKey={TAB}
            chartKey={`page.${TAB}.chart1`}
            t={t}
            initialHeight={440}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarVertical
                  tab={TAB}
                  id="chart1"
                  data={data.filter((d) => d.libelle === "year")}
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
          <ChartModal
            pageKey={TAB}
            chartKey={`page.${TAB}.chart2`}
            t={t}
            initialHeight={440}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarVertical
                  tab={TAB}
                  id="chart2"
                  data={data.filter((d) => d.libelle === "month")}
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
          <ChartModal
            pageKey={TAB}
            chartKey={`page.${TAB}.chart3`}
            t={t}
            initialHeight={440}
          >
            <ChartState query={section2}>
              {(data) => (
                <ComponentsStackedLines
                  tab={TAB}
                  id="chart3"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                  xTypeProp="month"
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-5">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart4`} t={t} />
          <ChartModal
            pageKey={TAB}
            chartKey={`page.${TAB}.chart4`}
            t={t}
            initialHeight={440}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarVertical
                  tab={TAB}
                  id="chart4"
                  data={data.filter((d) => d.libelle === "inc_type")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={cfg}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>
        <section className="chart span-7">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart5`} t={t} />
          <ChartModal
            pageKey={TAB}
            chartKey={`page.${TAB}.chart5`}
            t={t}
            initialHeight={440}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarVertical
                  tab={TAB}
                  id="chart5"
                  data={data.filter((d) => d.libelle === "dmg_eqp")}
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
          <ChartModal
            pageKey={TAB}
            chartKey={`page.${TAB}.chart7`}
            t={t}
            initialHeight={440}
          >
            <ChartState query={section4}>
              {(data) => {
                const filteredData = hideMoscowChart7
                  ? data.filter(
                      (d) => d.metric !== "Moscow" && d.metric !== "Москва",
                    )
                  : data;
                return (
                  <ComponentsTreemap
                    tab={TAB}
                    id="chart7"
                    data={filteredData}
                    t={t}
                    i18n={i18n}
                    nf={nf}
                    custom={cfg}
                    gradient={true}
                  />
                );
              }}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart8`} t={t} />
          <div className="filter-controls">
            <label className="filter-checkbox">
              <input
                type="checkbox"
                checked={hideMoscowChart8}
                onChange={(e) => setHideMoscowChart8(e.target.checked)}
                className="filter-input"
              />
              <span className="filter-label">
                {t(`page.${TAB}.hide_moscow`) || "Hide Moscow"}
              </span>
            </label>
          </div>
          <ChartModal
            pageKey={TAB}
            chartKey={`page.${TAB}.chart8`}
            t={t}
            initialHeight={440}
          >
            <ChartState query={section4}>
              {(data) => {
                const filteredData = hideMoscowChart8
                  ? data.filter(
                      (d) => d.metric !== "Moscow" && d.metric !== "Москва",
                    )
                  : data;
                return (
                  <ComponentsMap
                    tab={TAB}
                    id="chart8"
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

        {section3.data && (
          <>
            {Array.from(
              new Set(
                section3.data.map((d) => d.metric.substring(0, 4)), // Extract year from YYYY-MM-DD
              ),
            )
              .sort()
              .map((year) => (
                <section key={year} className="chart span-6">
                  <ChartHeader
                    pageKey={TAB}
                    chartKey={`page.${TAB}.chart6`}
                    t={t}
                    year={year}
                  />
                  <ChartModal
                    pageKey={TAB}
                    chartKey={`page.${TAB}.chart6`}
                    t={t}
                    initialHeight={440}
                  >
                    <ChartState query={section3}>
                      {(data) => (
                        <ComponentsHeatmap
                          tab={TAB}
                          id="chart6"
                          data={data.filter((d) => d.metric.startsWith(year))}
                          t={t}
                          i18n={i18n}
                          nf={nf}
                          custom={cfg}
                        />
                      )}
                    </ChartState>
                  </ChartModal>
                </section>
              ))}
          </>
        )}
      </section>
    </>
  );
}
