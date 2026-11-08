import { useEffect } from "react";
import {
  NavLink,
  Outlet,
  useLocation,
  useOutletContext,
} from "react-router-dom";
import { usePageTranslation } from "../hooks/usePageTranslation.js";
import { getPageFromPath } from "../utils/common.js";

export default function TabbedPage({
  pageName,
  titleKey,
  descriptionKey,
  tabs,
}) {
  const location = useLocation();
  const PAGE = getPageFromPath(location.pathname);
  const { t, i18n } = usePageTranslation(pageName);
  const nf = new Intl.NumberFormat(i18n.language);

  const activeTab = tabs.find((tab) =>
    location.pathname.endsWith(`/${tab.path}`),
  );

  // SEO: per-tab <title>, canonical URL and meta description
  useEffect(() => {
    const pageTitle = t(titleKey);
    const tabLabel = activeTab ? t(activeTab.label) : "";
    document.title = tabLabel ? `${tabLabel} | ${pageTitle}` : pageTitle;

    let canonical = document.querySelector('link[rel="canonical"]');
    if (!canonical) {
      canonical = document.createElement("link");
      canonical.rel = "canonical";
      document.head.appendChild(canonical);
    }
    canonical.href = window.location.origin + location.pathname;

    let description = document.querySelector('meta[name="description"]');
    if (!description) {
      description = document.createElement("meta");
      description.name = "description";
      document.head.appendChild(description);
    }
    description.content = t(descriptionKey);
  }, [
    location.pathname,
    i18n.language,
    activeTab,
    t,
    titleKey,
    descriptionKey,
  ]);

  return (
    <section className="page">
      <header className="page-head">
        <h1>{t(titleKey)}</h1>
        <p>{t(descriptionKey)}</p>
      </header>

      <div className="tabs-container">
        <div className="tabs-header" role="tablist">
          {tabs.map((tab) => {
            const isActive = location.pathname.endsWith(`/${tab.path}`);
            return (
              <NavLink
                key={tab.path}
                to={tab.path}
                end
                role="tab"
                aria-selected={isActive}
                className={`tab-button ${isActive ? "active" : ""}`}
              >
                {t(tab.label)}
              </NavLink>
            );
          })}
        </div>

        {/* Content - only the tab matching the current route is mounted */}
        <div className="tabs-content">
          <div className="tab-panel active">
            <Outlet context={{ PAGE, t, i18n, nf }} />
          </div>
        </div>
      </div>
    </section>
  );
}

export function OutletContextWrapper({ Component }) {
  const context = useOutletContext();
  return <Component {...context} />;
}
