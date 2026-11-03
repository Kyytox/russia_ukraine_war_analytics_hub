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

/**
 * Format KPI value based on type
 * - Numeric: formatted with locale
 * - Date (YYYY-MM-DD or YYYY-MM): formatted as date
 * - Other: returned as-is
 */
function formatKpiValue(value, locale) {
  if (!value) return value;

  // Check if numeric (starts with digit)
  if (/^\d/.test(value)) {
    // Check if it's a date (YYYY-MM-DD or YYYY-MM format)
    if (/^\d{4}-\d{2}(-\d{2})?$/.test(value)) {
      const date = new Date(value + "T00:00:00Z"); // Add time to parse as UTC
      if (!isNaN(date.getTime())) {
        // Format as date
        return new Intl.DateTimeFormat(locale, {
          year: "numeric",
          month: "long",
          day: value.length === 10 ? "numeric" : undefined,
        }).format(date);
      }
    }

    // Otherwise try to format as number
    const num = Number(value);
    if (!isNaN(num)) {
      return new Intl.NumberFormat(locale).format(num);
    }
  }

  return value;
}

const TAB = "overview";

export default function Overview({ PAGE, t, i18n, nf, customCharts }) {
  const prefix = `${PAGE}/${TAB}`;
  const custom = customCharts?.[`${TAB}`] ?? {}; // Get custom chart config for this tab

  const [hideMoscowChart7, setHideMoscowChart7] = useState(false);
  const [hideMoscowChart8, setHideMoscowChart8] = useState(false);

  const kpi = useApi(`/api/${prefix}/kpi`);
  const section1 = useApi(`/api/${prefix}/section1`);
  const section2 = useApi(`/api/${prefix}/section2`);
  // const section3 = useApi(`/api/${prefix}/section3`);
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
              {(data, height) => (
                <ComponentsBarVertical
                  id="chart1"
                  data={data.filter((d) => d.libelle === "year")}
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
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart2`} t={t} />
          <ChartModal
            pageKey={TAB}
            chartKey={`page.${TAB}.chart2`}
            t={t}
            initialHeight={440}
          >
            <ChartState query={section1}>
              {(data, height) => (
                <ComponentsBarVertical
                  id="chart2"
                  data={data.filter((d) => d.libelle === "month")}
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

        <section className="chart span-12">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart3`} t={t} />
          <ChartModal
            pageKey={TAB}
            chartKey={`page.${TAB}.chart3`}
            t={t}
            initialHeight={440}
          >
            <ChartState query={section2}>
              {(data, height) => (
                <ComponentsStackedLines
                  id="chart3"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                  xTypeProp="month"
                  height={height}
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
              {(data, height) => (
                <ComponentsBarVertical
                  id="chart4"
                  data={data.filter((d) => d.libelle === "inc_type")}
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
        <section className="chart span-7">
          <ChartHeader pageKey={TAB} chartKey={`page.${TAB}.chart5`} t={t} />
          <ChartModal
            pageKey={TAB}
            chartKey={`page.${TAB}.chart5`}
            t={t}
            initialHeight={440}
          >
            <ChartState query={section1}>
              {(data, height) => (
                <ComponentsBarVertical
                  id="chart5"
                  data={data.filter((d) => d.libelle === "dmg_eqp")}
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

        {/* LOOP over years in "by_day" section2 and display heatmap for each year 
        {section2.data && (
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
                    pageKey={PAGE}
                    chartKey={`chart6`}
                    t={t}
                    year={year}
                  />
                  <ChartState query={section3}>
                    {(data) => (
                      <ComponentsHeatmap
                        id={"chart7"}
                        data={data.filter((d) => d.metric.startsWith(year))}
                        t={t}
                        i18n={i18n}
                        nf={nf}
                        custom={customCharts}
                      />
                    )}
                  </ChartState>
                </section>
              ))}
          </>
        )} 
        */}

        {/* Chart 7: Treemap with independent Moscow filter */}
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
              {(data, height) => {
                const filteredData = hideMoscowChart7
                  ? data.filter(
                      (d) => d.label !== "Moscow" && d.label !== "Москва",
                    )
                  : data;
                return (
                  <ComponentsTreemap
                    id="chart7"
                    data={filteredData}
                    t={t}
                    i18n={i18n}
                    nf={nf}
                    custom={custom}
                    height={height}
                  />
                );
              }}
            </ChartState>
          </ChartModal>
        </section>

        {/* Chart 8: Map with independent Moscow filter */}
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
              {(data, height) => {
                const filteredData = hideMoscowChart8
                  ? data.filter(
                      (d) => d.label !== "Moscow" && d.label !== "Москва",
                    )
                  : data;
                return (
                  <ComponentsMap
                    id="chart8"
                    data={filteredData}
                    t={t}
                    i18n={i18n}
                    nf={nf}
                    custom={custom}
                    height={height}
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
