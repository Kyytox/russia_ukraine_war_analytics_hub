import { useLanguageNavigate } from "../hooks/useLanguage.js";
import { usePageTranslation } from "../hooks/usePageTranslation.js";
import { getPageFromPath } from "../utils/common.js";

import { useApi } from "../hooks/useApi.js";
import ChartState from "../components/charts/ChartState.jsx";
import Cards from "../components/Cards.jsx";

export default function MilitaryLosses() {
  const PAGE = getPageFromPath(window.location.pathname);
  const { t } = usePageTranslation(PAGE);
  const navigate = useLanguageNavigate();

  // Utilise useApi pour récupérer les sections
  const prefix = `/api/${PAGE}`;

  const data = useApi(`${prefix}/content`);

  return (
    <section className="home">
      <section className="hero">
        <h1>{t(`page.title`)}</h1>
        <p>{t(`page.intro`)} </p>
      </section>

      {/* ChartState gère les états loading/error automatiquement */}
      <ChartState query={data}>
        {(sectionsData) => (
          <Cards
            sections={sectionsData}
            t={t}
            navigate={navigate}
            typeLink={"external"}
          />
        )}
      </ChartState>
    </section>
  );
}
