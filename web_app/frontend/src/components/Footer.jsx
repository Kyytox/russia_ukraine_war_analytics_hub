import { useTranslation } from "react-i18next";
import { Github } from "lucide-react";
import { NavLink } from "react-router-dom";
import { useCurrentLanguage } from "../hooks/useLanguage.js";
import { FOOTER_NAV } from "../config/navigation.js";

export default function Footer() {
  const { t } = useTranslation();
  const currentLang = useCurrentLanguage();

  return (
    <footer className="footer">
      <div className="footer-content">
        <div className="footer-left">
          <p>
            &copy; {new Date().getFullYear()}{" "}
            {t("site.brand", { defaultValue: "UA Data Hub" })}
          </p>
          <p className="footer-madeby">
            {t("footer.madeBy", { defaultValue: "Made by Kytox" })}
          </p>
        </div>

        <div className="footer-center">
          <div className="footer-nav">
            {FOOTER_NAV.map(({ id, label }) => (
              <NavLink
                key={id}
                to={`/${currentLang}/${id}`}
                className="footer-nav-link"
              >
                {t(label, { defaultValue: id })}
              </NavLink>
            ))}
          </div>
        </div>

        <div className="footer-right">
          <a
            href="https://github.com/Kyytox/russia_ukraine_war_analytics_hub"
            target="_blank"
            rel="noopener noreferrer"
            className="footer-link"
            aria-label={t("nav.github", { defaultValue: "GitHub" })}
          >
            <Github size={20} />
          </a>
        </div>
      </div>
    </footer>
  );
}
