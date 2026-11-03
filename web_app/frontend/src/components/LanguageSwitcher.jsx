import { useTranslation } from "react-i18next";
import { LANGUAGES } from "../i18n";
import { useChangeLanguage } from "../hooks/useLanguage.js";
import { useState } from "react";

export default function LanguageSwitcher() {
  const { i18n, t } = useTranslation();
  const changeLanguage = useChangeLanguage();
  const [isChanging, setIsChanging] = useState(false);

  const handleLanguageChange = async (lang) => {
    if (isChanging || i18n.resolvedLanguage === lang) return;
    
    setIsChanging(true);
    try {
      await changeLanguage(lang);
    } finally {
      setIsChanging(false);
    }
  };

  return (
    <div className="lang" role="group" aria-label={t("nav.language")}>
      {LANGUAGES.map((l) => (
        <button 
          key={l} 
          lang={l} 
          aria-pressed={i18n.resolvedLanguage === l} 
          onClick={() => handleLanguageChange(l)}
          disabled={isChanging}
        >
          {l.toUpperCase()}
        </button>
      ))}
    </div>
  );
}
