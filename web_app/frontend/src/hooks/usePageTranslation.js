import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { loadPageTranslations } from "../i18n";

/**
 * Hook to load page-specific translations dynamically
 * Automatically loads the translation file for the current page
 *
 * Usage:
 *   const { t, i18n } = usePageTranslation("blocked-sites");
 *   t("pages.blocked-sites.title") // loads from en/blocked-sites.json, etc.
 */
export const usePageTranslation = (pageName) => {
  const { t, i18n, ready } = useTranslation();
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const loadTranslations = async () => {
      if (pageName && ready && i18n.language) {
        try {
          setIsLoading(true);
          await loadPageTranslations(pageName, i18n.language);
        } catch (err) {
          console.error(`Failed to load page translations for ${pageName}:`, err);
        } finally {
          setIsLoading(false);
        }
      }
    };

    loadTranslations();
  }, [pageName, i18n.language, ready]);

  return { t, i18n, ready, isLoading };
};

export default usePageTranslation;
