import { useApi } from "../../hooks/useApi.js";
import { useConfigCharts } from "../../hooks/useConfigCharts.js";

import { ComponentsPie } from "../../components/charts/ComponentsPie.jsx";
import { ComponentsBarVertical } from "../../components/charts/ComponentsBarVertical.jsx";
import { ComponentsBarHorizontal } from "../../components/charts/ComponentsBarHorizontal.jsx";
import { ComponentsBarStackedHorizontal } from "../../components/charts/ComponentsBarStackedHorizontal.jsx";
import { ComponentsTreemap } from "../../components/charts/ComponentsTreemap.jsx";
import { ComponentsSunburst } from "../../components/charts/ComponentsSunburst.jsx";

import ChartState from "../../components/charts/ChartState.jsx";
import ChartHeader from "../../components/charts/ChartHeader.jsx";
import ChartModal from "../../components/charts/ChartModal.jsx";

import { TriangleAlert } from "lucide-react";

const PAGE_KEY = "page";
const TAB = "overview";
const PAGE_NAME = "components-weapons";

export default function Overview({ PAGE, t, i18n, nf }) {
  const custom = useConfigCharts(PAGE_NAME);

  const kpi = useApi("/api/components-weapons/summary");
  const section1 = useApi("/api/components-weapons/section1");
  const section4 = useApi("/api/components-weapons/section4");
  const section2 = useApi("/api/components-weapons/section2");
  const section3 = useApi("/api/components-weapons/section3");

  return (
    <>
      {/* div alrts */}
      <section className="infos">
        <p>
          <TriangleAlert />
          {t(`${PAGE_KEY}.infos1`)}
          <a
            href="https://war-sanctions.gur.gov.ua/en/components"
            target="_blank"
            rel="noopener noreferrer"
          >
            War & Sanctions - Сomponents in the aggressor`s weapon.
          </a>
        </p>
        <p>{t(`${PAGE_KEY}.infos2`)}</p>
      </section>

      <section className="kpis">
        <ChartState query={kpi} height={96}>
          {(items) =>
            items.map((item) => (
              <div key={item.id} className="placeholder h-kpi">
                <span>
                  {/^\d/.test(item.value)
                    ? nf.format(Number(item.value))
                    : item.value}
                </span>
                <h3>{t(`${PAGE_KEY}.${item.id}`)}</h3>
              </div>
            ))
          }
        </ChartState>
      </section>

      <section className="charts-grid">
        <h2 className="section-title span-12">{t(`page.section1_title`)}</h2>
        <section className="chart span-4">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart1" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart1`}
            t={t}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsPie
                  tab={TAB} id="chart1"
                  data={data.filter((d) => d.libelle === "type_equipment")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart8" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart8`}
            t={t}
          >
            <ChartState query={section4}>
              {(data) => (
                <ComponentsSunburst
                  tab={TAB} id="chart8"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

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
                  data={data.filter(
                    (d) => d.libelle === "manufacturer_country",
                  )}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
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
                  data={data.filter((d) => d.libelle === "manufacturer")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-5">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart2" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart2`}
            t={t}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarVertical
                  tab={TAB} id="chart2"
                  data={data.filter((d) => d.libelle === "weapon_type")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-7">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart5" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart5`}
            t={t}
          >
            <ChartState query={section1}>
              {(data) => (
                <ComponentsBarHorizontal
                  tab={TAB} id="chart5"
                  data={data.filter((d) => d.libelle === "weapon")}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <h2 className="section-title span-12">{t(`page.section2_title`)}</h2>
        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart6" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart6`}
            t={t}
          >
            <ChartState query={section2}>
              {(data) => (
                <ComponentsBarStackedHorizontal
                  tab={TAB} id="chart6"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart7" t={t} />
          <ChartModal
            pageKey={PAGE_KEY}
            chartKey={`page.${PAGE_KEY}.chart7`}
            t={t}
          >
            <ChartState query={section3}>
              {(data) => (
                <ComponentsTreemap
                  tab={TAB} id="chart7"
                  data={data}
                  t={t}
                  i18n={i18n}
                  nf={nf}
                  custom={custom}
                />
              )}
            </ChartState>
          </ChartModal>
        </section>
      </section>
    </>
  );
}
