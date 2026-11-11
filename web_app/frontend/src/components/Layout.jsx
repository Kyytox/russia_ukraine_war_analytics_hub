import { useEffect, useState } from "react";
import { Outlet, useLocation, useParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Menu, X, Github, Home } from "lucide-react";
import LanguageSwitcher from "./LanguageSwitcher.jsx";
import SEO from "./SEO.jsx";
import Footer from "./Footer.jsx";
import { SIDEBAR_NAV } from "../config/navigation.js";
import { getSEOConfig } from "../config/seo.js";
import { getPageFromPath } from "../utils/common.js";
import { useLanguageNavigate } from "../hooks/useLanguage.js";

// Re-export for backward compatibility
export const NAV = SIDEBAR_NAV;

export default function Layout() {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const { pathname } = useLocation();
  const navigate = useLanguageNavigate();

  // Get page name for SEO
  const PAGE = getPageFromPath(pathname);
  const seoConfig = getSEOConfig(PAGE, pathname, t);

  // Get home icon from NAV
  const homeItem = SIDEBAR_NAV.find((item) => item.id === "home");
  const HomeIcon = homeItem?.icon || Home;

  useEffect(() => setOpen(false), [pathname]);

  return (
    <>
      <SEO
        title={seoConfig.title}
        description={seoConfig.description}
        type={seoConfig.type}
        breadcrumb={seoConfig.breadcrumb}
      />
      <header className="topbar">
        <button
          className="icon-btn"
          onClick={() => setOpen(true)}
          aria-label={t("nav.openMenu")}
        >
          <Menu size={22} />
        </button>
        <button
          onClick={() => navigate("/")}
          className="brand"
          style={{ border: "none", background: "none", cursor: "pointer" }}
        >
          {t("site.brand")}
        </button>
        <LanguageSwitcher />
      </header>

      <div
        className={`scrim ${open ? "show" : ""}`}
        onClick={() => setOpen(false)}
      />
      <aside className={`sidebar ${open ? "open" : ""}`}>
        <button
          className="icon-btn"
          onClick={() => setOpen(false)}
          aria-label={t("nav.closeMenu")}
        >
          <X size={20} />
        </button>
        <nav>
          <button
            onClick={() => navigate("/")}
            className="nav-item"
            style={{
              border: "none",
              background: "none",
              cursor: "pointer",
              width: "100%",
              textAlign: "left",
            }}
          >
            <HomeIcon size={18} />
            {t("nav.home")}
          </button>
          {SIDEBAR_NAV.filter((item) => item.id !== "home").map(
            ({ id, icon: Icon }) => (
              <button
                key={id}
                onClick={() => navigate(`/${id}`)}
                className="nav-item"
                style={{
                  border: "none",
                  background: "none",
                  cursor: "pointer",
                  width: "100%",
                  textAlign: "left",
                }}
              >
                <Icon size={18} />
                {t(`sections.${id}.title`)}
              </button>
            ),
          )}
        </nav>
        <footer>
          <a
            href="https://github.com/Kyytox/russia_ukraine_war_analytics_hub"
            aria-label={t("nav.github")}
            target="_blank"
            rel="noopener noreferrer"
          >
            <Github size={20} />
          </a>
        </footer>
      </aside>

      <main>
        <SEO />
        <Outlet />
        <Footer />
      </main>
    </>
  );
}
