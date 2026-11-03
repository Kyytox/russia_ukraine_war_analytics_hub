import { useLanguageNavigate } from "../hooks/useLanguage.js";
import { usePageTranslation } from "../hooks/usePageTranslation.js";
import { getPageFromPath } from "../utils/common.js";

import { useApi } from "../hooks/useApi.js";
import ChartState from "../components/charts/ChartState.jsx";
import Cards from "../components/Cards.jsx";

export default function Home() {
  const PAGE = getPageFromPath(window.location.pathname);
  const { t } = usePageTranslation(PAGE);
  const navigate = useLanguageNavigate();

  // Utilise useApi pour récupérer les sections
  const sections = useApi("/api/sections");
  const dayWar = useApi("/api/day-war");

  return (
    <section className="home">
      <section className="hero">
        <h1>{t("site.title")}</h1>
        <h3>{t(`page.day_war`, { day: dayWar.data })}</h3>
        <p>{t(`page.intro`)} </p>
      </section>

      {/* ChartState gère les états loading/error automatiquement */}
      <ChartState query={sections}>
        {(sectionsData) => (
          <Cards
            sections={sectionsData}
            t={t}
            navigate={navigate}
            typeLink={"internal"}
          />
        )}
      </ChartState>
    </section>
  );
}
