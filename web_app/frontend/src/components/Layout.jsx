import { useEffect, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  Menu,
  X,
  Home,
  TrainFrontTunnel,
  Ban,
  Bell,
  Cog,
  Map,
  Cross,
  CircleHelp,
  Github,
} from "lucide-react";
import LanguageSwitcher from "./LanguageSwitcher.jsx";
import {
  useLanguageNavigate,
  useCurrentLanguage,
} from "../hooks/useLanguage.js";

// id = clé de traduction (sections.<id>.title) ET segment d'URL
export const NAV = [
  { id: "incidents-railways", icon: TrainFrontTunnel },
  { id: "blocked-sites", icon: Ban },
  { id: "raid-alerts", icon: Bell },
  { id: "components-weapons", icon: Cog },
  { id: "interactive-maps", icon: Map },
  { id: "military-losses", icon: Cross },
];

export default function Layout() {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const { pathname } = useLocation();
  const navigate = useLanguageNavigate();
  const currentLang = useCurrentLanguage();

  useEffect(() => setOpen(false), [pathname]);

  return (
    <>
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
            <Home size={18} />
            {t("nav.home")}
          </button>
          {NAV.map(({ id, icon: Icon }) => (
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
          ))}
        </nav>
        <footer>
          <a href="#" aria-label={t("nav.help")}>
            <CircleHelp size={20} />
          </a>
          <a
            href="https://github.com/Kyytox/russia_ukraine_war_analytics_hub"
            aria-label={t("nav.github")}
          >
            <Github size={20} />
          </a>
        </footer>
      </aside>

      <main>
        <Outlet />
      </main>
    </>
  );
}
