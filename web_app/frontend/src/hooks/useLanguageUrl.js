import { useCurrentLanguage } from "../hooks/useLanguage.js";

/**
 * Composant helper pour créer des URLs avec la langue actuelle
 * Utile pour les liens externes, métadonnées, etc.
 */
export function useLanguageUrl() {
  const lang = useCurrentLanguage();

  /**
   * Crée une URL avec le préfixe de langue
   * @param {string} path - Le chemin sans le préfixe de langue
   * @returns {string} - URL complète avec langue
   */
  const getUrl = (path) => {
    const normalizedPath = path.startsWith("/") ? path : `/${path}`;
    return `/${lang}${normalizedPath}`;
  };

  /**
   * Crée une URL absolue (avec domaine)
   * @param {string} path - Le chemin
   * @param {string} domain - Le domaine (optionnel, sinon utilise window.location.origin)
   * @returns {string} - URL absolue
   */
  const getAbsoluteUrl = (path, domain = window.location.origin) => {
    return `${domain}${getUrl(path)}`;
  };

  return { getUrl, getAbsoluteUrl, lang };
}

/**
 * Composant pour générer des métadonnées avec langue
 * Utile pour Open Graph, canonical links, etc.
 */
export function LanguageMeta() {
  const { getAbsoluteUrl, lang } = useLanguageUrl();

  return {
    canonical: getAbsoluteUrl(window.location.pathname.replace(`/${lang}`, "")),
    ogUrl: getAbsoluteUrl(window.location.pathname),
    hrefLang: (targetLang) => ({
      rel: "alternate",
      hrefLang: targetLang,
      href: getAbsoluteUrl(
        window.location.pathname.replace(`/${lang}`, ""),
        targetLang,
      ),
    }),
  };
}

/**
 * Exemple d'utilisation dans une page :
 *
 * import { useLanguageUrl } from "../hooks/useLanguageUrl.js";
 *
 * export default function MyPage() {
 *   const { getUrl, getAbsoluteUrl, lang } = useLanguageUrl();
 *
 *   return (
 *     <>
 *       <a href={getUrl("/home")}>Lien interne</a>
 *       <a href={getAbsoluteUrl("/home")}>URL absolue</a>
 *       <p>Langue actuelle: {lang}</p>
 *     </>
 *   );
 * }
 */
