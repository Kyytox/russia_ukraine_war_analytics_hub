import { useTranslation } from "react-i18next";

function Placeholder({ label, className = "" }) {
  return (
    <div className={`placeholder ${className}`}>
      <span>{label}</span>
    </div>
  );
}

// id = clé de la section (ex. "railways") : titre et description viennent des fichiers de traduction
export default function PageShell({
  id,
  kpiCount = 3,
  charts = 2,
  map = false,
  table = true,
}) {
  const { t } = useTranslation();
  const range = (n) => Array.from({ length: n }, (_, i) => i + 1);

  return (
    <article className="page">
      <header className="page-head">
        <div className="flag" aria-hidden="true" />
        <h1>{t(`sections.${id}.title`)}</h1>
        <p>{t(`sections.${id}.description`)}</p>
      </header>

      <section className="filters">
        <Placeholder label={t("page.filters")} className="h-filter" />
      </section>

      {kpiCount > 0 && (
        <section className="kpis">
          {range(kpiCount).map((n) => (
            <Placeholder
              key={n}
              label={t("page.kpi", { n })}
              className="h-kpi"
            />
          ))}
        </section>
      )}

      {map && (
        <section>
          <Placeholder label={t("page.map")} className="h-map" />
        </section>
      )}

      {charts > 0 && (
        <section className="charts">
          {range(charts).map((n) => (
            <Placeholder
              key={n}
              label={t("page.chart", { n })}
              className="h-chart"
            />
          ))}
        </section>
      )}

      {table && (
        <section>
          <Placeholder label={t("page.table")} className="h-table" />
        </section>
      )}

      <section className="sources">
        <h2>{t("page.sources")}</h2>
        <Placeholder label={t("page.sourcesList")} className="h-sources" />
      </section>
    </article>
  );
}
