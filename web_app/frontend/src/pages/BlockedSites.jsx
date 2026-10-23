import { useEffect, useState } from "react";

import { useApi } from "../hooks/useApi.js";
import { useLanguageNavigate } from "../hooks/useLanguage.js";
import { usePageTranslation } from "../hooks/usePageTranslation.js";
import { useCustomChart } from "../hooks/useCustomChart.js";

import { ComponentsPie } from "../components/charts/ComponentsPie.jsx";
import { ComponentsBarVertical } from "../components/charts/ComponentsBarVertical.jsx";
import { ComponentsBarHorizontal } from "../components/charts/ComponentsBarHorizontal.jsx";
import { ComponentsBarStackedHorizontal } from "../components/charts/ComponentsBarStackedHorizontal.jsx";
import { ComponentsBarStackedVertical } from "../components/charts/ComponentsBarStackedVertical.jsx";
import { ComponentsTreemap } from "../components/charts/ComponentsTreemap.jsx";
import { ComponentsSunburst } from "../components/charts/ComponentsSunburst.jsx";
import { ComponentsLineArea } from "../components/charts/ComponentsLineArea.jsx";
import { ComponentsHeatmap } from "../components/charts/ComponentsHeatmap.jsx";
import { ComponentsSankey } from "../components/charts/ComponentsSankey.jsx";
import { ComponentsTree } from "../components/charts/ComponentsTree.jsx";

import ChartState from "../components/charts/ChartState.jsx";
import ChartHeader from "../components/charts/ChartHeader.jsx";

const PAGE_KEY = "page";
const PAGE_NAME = "blocked-sites";

export default function BlockedSites() {
  const [error, setError] = useState(false);

  const { t, i18n } = usePageTranslation(PAGE_NAME);
  const customCharts = useCustomChart(PAGE_NAME);
  const nf = new Intl.NumberFormat(i18n.language);

  const kpi = useApi("/api/blocked-sites/kpi");
  const section1 = useApi("/api/blocked-sites/section1");
  const section2 = useApi("/api/blocked-sites/section2");
  const section3 = useApi("/api/blocked-sites/section3");
  const section4 = useApi("/api/blocked-sites/section4");
  const section5 = useApi("/api/blocked-sites/section5");

  if (error) {
    return (
      <div className="page">
        <h1>{t(`sections.${PAGE_NAME}.title`)}</h1>
        <p>{t(`${PAGE_KEY}.error`)}</p>
      </div>
    );
  }

  return (
    <section className="page">
      <header className="page-head">
        <h1>{t(`sections.${PAGE_NAME}.title`)}</h1>
        <p>{t(`sections.${PAGE_NAME}.description`)}</p>
      </header>

      <section className="infos">
        <p>
          {t(`${PAGE_KEY}.infos1`)}
          <a
            href="https://www.top10vpn.com/research/websites-blocked-in-russia/"
            target="_blank"
            rel="noopener noreferrer"
          >
            Websites Blocked in Russia
          </a>
        </p>
        <p>{t(`${PAGE_KEY}.infos2`)}</p>
      </section>

      {kpi.data && (
        <div className="last-update">
          <p>
            {t(`${PAGE_KEY}.last_date_updated`)}
            {" : "}
            {kpi.data.find((item) => item.id === "last_date_updated")?.value ||
              t("common.loading")}
          </p>
        </div>
      )}

      <section className="kpis">
        <ChartState query={kpi} height={96}>
          {(items) =>
            items
              .filter((item) => item.id.startsWith("kpi"))
              .map((item) => (
                <div key={item.id} className="placeholder h-kpi">
                  <span>
                    {/^\d/.test(item.value)
                      ? nf.format(Number(item.value))
                      : t(`page.content.${item.value}`, item.value)}
                  </span>
                  <h3>{t(`${PAGE_KEY}.${item.id}`)}</h3>
                </div>
              ))
          }
        </ChartState>

        {/* Last date updated */}
        <ChartState query={kpi} height={96}>
          {(items) => {
            const lastDateItem = items.find(
              (item) => item.libelle === "last_date_updated",
            );
            return lastDateItem ? (
              <div className="placeholder h-kpi">
                <span>{lastDateItem.value}</span>
                <h3>{t(`${PAGE_KEY}.last_date_updated`)}</h3>
              </div>
            ) : null;
          }}
        </ChartState>
      </section>

      {/* 
      Exemple de bouton interne
      <div style={{ marginTop: "2rem" }}>
        <button onClick={() => navigate("/")}>{t("nav.home")}</button>
      </div> 
      */}

      <section className="charts-grid">
        <h2 className="section-title span-12">{t(`page.section1_title`)}</h2>
        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart3" t={t} />
          <ChartState query={section1} height={440}>
            {(data) => (
              <ComponentsBarHorizontal
                id="chart3"
                data={data.filter((d) => d.libelle === "by_category")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart4" t={t} />
          <ChartState query={section1} height={440}>
            {(data) => (
              <ComponentsBarHorizontal
                id="chart4"
                data={data.filter((d) => d.libelle === "by_banning_authority")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart1" t={t} />
          <ChartState query={section1} height={440}>
            {(data) => (
              <ComponentsBarHorizontal
                id="chart1"
                data={data.filter((d) => d.libelle === "by_country_domain")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart2" t={t} />
          <ChartState query={section1} height={440}>
            {(data) => (
              <ComponentsBarHorizontal
                id="chart2"
                data={data.filter((d) => d.libelle === "by_subcategory")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        <h2 className="section-title span-12">{t(`page.section2_title`)}</h2>
        <section className="chart span-4">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart5" t={t} />
          <ChartState query={section2} height={440}>
            {(data) => (
              <ComponentsBarVertical
                id="chart5"
                data={data.filter((d) => d.libelle === "by_year")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart6" t={t} />
          <ChartState query={section2} height={440}>
            {(data) => (
              <ComponentsBarVertical
                id="chart6"
                data={data.filter((d) => d.libelle === "by_month")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        {/* LOOP over years in "by_day" section2 and display heatmap for each year */}
        {section2.data && (
          <>
            {Array.from(
              new Set(
                section2.data
                  .filter((d) => d.libelle === "by_day")
                  .map((d) => d.metric.substring(0, 4)), // Extract year from YYYY-MM-DD
              ),
            )
              .sort()
              .map((year) => (
                <section key={year} className="chart span-6">
                  <ChartHeader
                    pageKey={PAGE_KEY}
                    chartKey={`chart7`}
                    t={t}
                    year={year}
                  />
                  <ChartState query={section2} height={440}>
                    {(data) => (
                      <ComponentsHeatmap
                        id={"chart7"}
                        data={data.filter(
                          (d) =>
                            d.libelle === "by_day" && d.metric.startsWith(year),
                        )}
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

        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart8" t={t} />
          <ChartState query={section2} height={440}>
            {(data) => (
              <ComponentsLineArea
                id="chart8"
                data={data.filter((d) => d.libelle === "by_day_cumul")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        <h2 className="section-title span-12">{t(`page.section3_title`)}</h2>
        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart9" t={t} />
          <ChartState query={section3} height={440}>
            {(data) => (
              <ComponentsBarStackedVertical
                id="chart10"
                data={data.filter((d) => d.id === "category")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart10" t={t} />
          <ChartState query={section3} height={440}>
            {(data) => (
              <ComponentsBarStackedVertical
                id="chart9"
                data={data.filter((d) => d.id === "banning_authority")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart11" t={t} />
          <ChartState query={section3} height={440}>
            {(data) => (
              <ComponentsBarStackedVertical
                id="chart11"
                data={data.filter((d) => d.id === "country_domain")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart12" t={t} />
          <ChartState query={section4} height={440}>
            {(data) => (
              <ComponentsTree
                id="chart12"
                data={data}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>

        <h2 className="section-title span-12">{t(`page.section4_title`)}</h2>
        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart13" t={t} />
          <ChartState query={section5} height={440}>
            {(data) => (
              <ComponentsBarStackedHorizontal
                id="chart13"
                data={data}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={customCharts}
              />
            )}
          </ChartState>
        </section>
      </section>
    </section>
  );
}
