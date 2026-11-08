import { useApi } from "../../hooks/useApi.js";
import { useConfigCharts } from "../../hooks/useConfigCharts.js";

import { ComponentsBarVertical } from "../../components/charts/ComponentsBarVertical.jsx";
import { ComponentsBarHorizontal } from "../../components/charts/ComponentsBarHorizontal.jsx";
import { ComponentsBarStackedHorizontal } from "../../components/charts/ComponentsBarStackedHorizontal.jsx";
import { ComponentsBarStackedVertical } from "../../components/charts/ComponentsBarStackedVertical.jsx";
import { ComponentsLineArea } from "../../components/charts/ComponentsLineArea.jsx";
import { ComponentsHeatmap } from "../../components/charts/ComponentsHeatmap.jsx";
import { ComponentsTree } from "../../components/charts/ComponentsTree.jsx";

import ChartState from "../../components/charts/ChartState.jsx";
import ChartHeader from "../../components/charts/ChartHeader.jsx";
import ChartModal from "../../components/charts/ChartModal.jsx";

import { TriangleAlert } from "lucide-react";

const PAGE_KEY = "page";
const TAB = "overview";
const PAGE_NAME = "blocked-sites";

export default function Overview({ PAGE, t, i18n, nf }) {
  const customCharts = useConfigCharts(PAGE_NAME);

  const kpi = useApi("/api/blocked-sites/kpi");
  const section1 = useApi("/api/blocked-sites/section1");
  const section2 = useApi("/api/blocked-sites/section2");
  const section3 = useApi("/api/blocked-sites/section3");
  const section4 = useApi("/api/blocked-sites/section4");
  const section5 = useApi("/api/blocked-sites/section5");

  return (
    <>
      <section className="infos">
        <p>
          <TriangleAlert />
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

      <section className="charts-grid">
        <h2 className="section-title span-12">{t(`page.section1_title`)}</h2>
        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart3" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart3`}
            t={t}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarHorizontal
                  tab={TAB} id="chart3"
                  data={data.filter((d) => d.libelle === "by_category")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart4" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart4`}
            t={t}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarHorizontal
                  tab={TAB} id="chart4"
                  data={data.filter(
                    (d) => d.libelle === "by_banning_authority",
                  )}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart1" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart1`}
            t={t}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarHorizontal
                  tab={TAB} id="chart1"
                  data={data.filter((d) => d.libelle === "by_country_domain")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart2" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart2`}
            t={t}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarHorizontal
                  tab={TAB} id="chart2"
                  data={data.filter((d) => d.libelle === "by_subcategory")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <h2 className="section-title span-12">{t(`page.section2_title`)}</h2>
        <section className="chart span-4">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart5" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart5`}
            t={t}
          >
            <ChartState query={section2}>
              {(data) => (
                <ComponentsBarVertical
                  tab={TAB} id="chart5"
                  data={data.filter((d) => d.libelle === "by_year")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart6" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart6`}
            t={t}
          >
            <ChartState query={section2}>
              {(data) => (
                <ComponentsBarVertical
                  tab={TAB} id="chart6"
                  data={data.filter((d) => d.libelle === "by_month")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
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
                  <ChartModal
                    pageKey={PAGE_KEY}
                    chartKey={`chart7`}
                    t={t}
                    year={year}
                  >
                    <ChartState query={section2}>
                      {(data) => (
                        <ComponentsHeatmap
                          tab={TAB} id={"chart7"}
                          data={data.filter(
                            (d) =>
                              d.libelle === "by_day" &&
                              d.metric.startsWith(year),
                          )}
                          t={t}
                          i18n={i18n}
                          nf={nf}
                          custom={customCharts}
                        />
                      )}
                    </ChartState>
                  </ChartModal>
                </section>
              ))}
          </>
        )}

        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart8" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart8`}
            t={t}
          >
            <ChartState query={section2}>
              {(data) => (
                <ComponentsLineArea
                  tab={TAB} id="chart8"
                  data={data.filter((d) => d.libelle === "by_day_cumul")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <h2 className="section-title span-12">{t(`page.section3_title`)}</h2>
        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart9" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart9`}
            t={t}
          >
            <ChartState query={section3}>
              {(data) => (
                <ComponentsBarStackedVertical
                  tab={TAB} id="chart10"
                  data={data.filter((d) => d.id === "category")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart10" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart10`}
            t={t}
          >
            <ChartState query={section3}>
              {(data) => (
                <ComponentsBarStackedVertical
                  tab={TAB} id="chart9"
                  data={data.filter((d) => d.id === "banning_authority")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart11" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart11`}
            t={t}
          >
            <ChartState query={section3}>
              {(data) => (
                <ComponentsBarStackedVertical
                  tab={TAB} id="chart11"
                  data={data.filter((d) => d.id === "country_domain")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart12" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart12`}
            t={t}
          >
            <ChartState query={section4}>
              {(data) => (
                <ComponentsTree
                  tab={TAB} id="chart12"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <h2 className="section-title span-12">{t(`page.section4_title`)}</h2>
        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart13" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart13`}
            t={t}
          >
            <ChartState query={section5}>
              {(data) => (
                <ComponentsBarStackedHorizontal
                  tab={TAB} id="chart13"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={customCharts}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>
      </section>
    </>
  );
}
