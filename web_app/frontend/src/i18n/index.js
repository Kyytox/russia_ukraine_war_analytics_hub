import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import LanguageDetector from "i18next-browser-languagedetector";

export const LANGUAGES = ["en", "fr", "ua"];
export const DEFAULT_LANGUAGE = "en";

/**
 * Extract language from URL path or use browser/stored language
 * Returns validated language code or fallback
 */
export const getLanguageFromPath = () => {
  const path = window.location.pathname;
  const segments = path.split("/").filter(Boolean);
  const firstSegment = segments[0];

  if (LANGUAGES.includes(firstSegment)) {
    return firstSegment;
  }
  return null;
};

/**
 * Initialize i18n with common translations for all languages
 */
export const initializeI18n = async () => {
  // Load common translations for all languages
  const resources = {};

  for (const lang of LANGUAGES) {
    const commonTranslations = await import(`./${lang}/common.json`).then(
      (m) => m.default,
    );
    resources[lang] = { translation: commonTranslations };
  }

  i18n
    .use(LanguageDetector)
    .use(initReactI18next)
    .init({
      resources,
      supportedLngs: LANGUAGES,
      nonExplicitSupportedLngs: true,
      fallbackLng: DEFAULT_LANGUAGE,
      interpolation: { escapeValue: false },
      react: {
        useSuspense: false,
      },
    });

  const sync = (lng) => {
    document.documentElement.lang = lng;
    document.title = i18n.t("site.title");
  };

  i18n.on("languageChanged", sync);

  const pathLang = getLanguageFromPath();
  if (pathLang) {
    await i18n.changeLanguage(pathLang);
  } else {
    await i18n.changeLanguage(i18n.resolvedLanguage || DEFAULT_LANGUAGE);
  }

  sync(i18n.language);
};

/**
 * Load translations for a specific page
 * Merges page translations with existing common translations
 */
export const loadPageTranslations = async (pageName, lang = i18n.language) => {
  if (!pageName) return;

  try {
    const pageTranslations = await import(`./${lang}/${pageName}.json`).then(
      (m) => m.default,
    );

    // Get current translations
    const currentTranslations = i18n.getResourceBundle(lang, "translation");

    // Merge page translations with common translations but replace page section with new page translations
    const merged = {
      ...currentTranslations,
      page: {
        ...pageTranslations,
      },
    };

    // Update resource bundle
    i18n.addResourceBundle(lang, "translation", merged, true, true);
  } catch (error) {
    console.warn(
      `Page translations not found for ${pageName} in ${lang}`,
      error,
    );
  }
};

export default i18n;
