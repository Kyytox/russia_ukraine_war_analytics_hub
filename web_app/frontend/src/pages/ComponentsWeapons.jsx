import { useState } from "react";

import { useApi } from "../hooks/useApi.js";
import { usePageTranslation } from "../hooks/usePageTranslation.js";
import { useCustomChart } from "../hooks/useCustomChart.js";

import { ComponentsPie } from "../components/charts/ComponentsPie.jsx";
import { ComponentsBarVertical } from "../components/charts/ComponentsBarVertical.jsx";
import { ComponentsBarHorizontal } from "../components/charts/ComponentsBarHorizontal.jsx";
import { ComponentsBarStackedHorizontal } from "../components/charts/ComponentsBarStackedHorizontal.jsx";
import { ComponentsTreemap } from "../components/charts/ComponentsTreemap.jsx";
import { ComponentsSunburst } from "../components/charts/ComponentsSunburst.jsx";

import ChartState from "../components/charts/ChartState.jsx";
import ChartHeader from "../components/charts/ChartHeader.jsx";

const PAGE_KEY = "pages.components-weapons";
const PAGE_NAME = "components-weapons";

export default function ComponentsWeapons() {
  const [error, setError] = useState(false);

  const { t, i18n } = usePageTranslation("components-weapons");
  const custom = useCustomChart(PAGE_NAME);
  const nf = new Intl.NumberFormat(i18n.language);

  const kpi = useApi("/api/components-weapons/summary");
  const section1 = useApi("/api/components-weapons/section1");
  const section4 = useApi("/api/components-weapons/section4");
  const section2 = useApi("/api/components-weapons/section2");
  const section3 = useApi("/api/components-weapons/section3");

  if (error) {
    return (
      <div className="page">
        <h1>{t(`${PAGE_KEY}.title`)}</h1>
        <p>{t(`${PAGE_KEY}.error`)}</p>
      </div>
    );
  }

  return (
    <article className="page">
      <header className="page-head">
        <h1>{t(`${PAGE_KEY}.title`)}</h1>
        <p>{t(`${PAGE_KEY}.description`)}</p>
      </header>

      {/* div alrts */}
      <section className="infos">
        <p>
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

      {/* Exemple de bouton interne
      <div style={{ marginTop: "2rem" }}>
        <button onClick={() => navigate("/")}>{t("nav.home")}</button>
      </div> */}

      <section className="charts-grid">
        <section className="chart span-4">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart1" t={t} />
          <ChartState query={section1} height={440}>
            {(data) => (
              <ComponentsPie
                id="chart1"
                data={data.filter((d) => d.libelle === "type_equipment")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-8">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart8" t={t} />
          <ChartState query={section4} height={600}>
            {(data) => (
              <ComponentsSunburst
                id="chart8"
                data={data}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-6">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart3" t={t} />
          <ChartState query={section1} height={440}>
            {(data) => (
              <ComponentsBarHorizontal
                id="chart3"
                data={data.filter((d) => d.libelle === "manufacturer_country")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
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
                data={data.filter((d) => d.libelle === "manufacturer")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-5">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart2" t={t} />
          <ChartState query={section1} height={440}>
            {(data) => (
              <ComponentsBarVertical
                id="chart2"
                data={data.filter((d) => d.libelle === "weapon_type")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-7">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart5" t={t} />
          <ChartState query={section1} height={440}>
            {(data) => (
              <ComponentsBarHorizontal
                id="chart5"
                data={data.filter((d) => d.libelle === "weapon")}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart6" t={t} />
          <ChartState query={section2} height={440}>
            {(data) => (
              <ComponentsBarStackedHorizontal
                id="chart6"
                data={data}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              />
            )}
          </ChartState>
        </section>

        <section className="chart span-12">
          <ChartHeader pageKey={PAGE_KEY} chartKey="chart7" t={t} />
          <ChartState query={section3} height={440}>
            {(data) => (
              <ComponentsTreemap
                id="chart7"
                data={data}
                t={t}
                i18n={i18n}
                nf={nf}
                custom={custom}
              />
            )}
          </ChartState>
        </section>
      </section>
    </article>
  );
}
