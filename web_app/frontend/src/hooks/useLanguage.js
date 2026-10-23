import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { LANGUAGES } from "../i18n";

/**
 * Hook for navigating with language prefix in URL
 * Usage: const navigate = useLanguageNavigate(); navigate("/home")
 * Result: navigates to /en/home (if current language is en)
 */
export function useLanguageNavigate() {
  const navigate = useNavigate();
  const { i18n } = useTranslation();

  return (path) => {
    const lang = i18n.resolvedLanguage || "en";
    const normalizedPath = path.startsWith("/") ? path : `/${path}`;
    navigate(`/${lang}${normalizedPath}`);
  };
}

/**
 * Hook to get current language from URL or i18n state
 */
export function useCurrentLanguage() {
  const { i18n } = useTranslation();
  return i18n.resolvedLanguage || "en";
}

/**
 * Hook to change language and update URL accordingly
 */
export function useChangeLanguage() {
  const navigate = useNavigate();
  const { i18n } = useTranslation();

  return (newLang) => {
    if (!LANGUAGES.includes(newLang)) return;

    i18n.changeLanguage(newLang);

    // Get current path without language prefix
    const path = window.location.pathname;
    const segments = path.split("/").filter(Boolean);

    // Remove language prefix if present
    let pathWithoutLang = segments;
    if (LANGUAGES.includes(segments[0])) {
      pathWithoutLang = segments.slice(1);
    }

    // Navigate to new path with new language
    const newPath =
      pathWithoutLang.length > 0
        ? `/${newLang}/${pathWithoutLang.join("/")}`
        : `/${newLang}`;

    navigate(newPath, { replace: true });
  };
}
