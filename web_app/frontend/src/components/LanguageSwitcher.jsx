import { useTranslation } from "react-i18next";
import { LANGUAGES } from "../i18n";
import { useChangeLanguage } from "../hooks/useLanguage.js";

export default function LanguageSwitcher() {
  const { i18n, t } = useTranslation();
  const changeLanguage = useChangeLanguage();

  return (
    <div className="lang" role="group" aria-label={t("nav.language")}>
      {LANGUAGES.map((l) => (
        <button 
          key={l} 
          lang={l} 
          aria-pressed={i18n.resolvedLanguage === l} 
          onClick={() => changeLanguage(l)}
        >
          {l.toUpperCase()}
        </button>
      ))}
    </div>
  );
}
