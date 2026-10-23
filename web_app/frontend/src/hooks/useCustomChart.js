import { useContext, useMemo } from "react";
import { CustomContext } from "../context/CustomContext.jsx";

/**
 * Hook pour accéder aux configurations custom (colors, sizes, etc.) d'une page précise
 * Fonctionne comme useTranslation() mais pour custom.json
 *
 * Usage:
 * const custom = useCustomChart("blocked-sites");
 * custom?.chart1?.colors // Récupère les colors pour ce chart
 *
 * @param {string} pageName - Le nom de la page (ex: "blocked-sites", "components-weapons")
 * @returns {object} Les configs pour cette page
 */
export function useCustomChart(pageName) {
  const context = useContext(CustomContext);

  return useMemo(() => {
    if (!context) {
      console.warn(
        `useCustomChart("${pageName}") used outside <CustomProvider>. Returning empty object.`,
      );
      return {};
    }

    // Retourne les données de la page spécifique
    return context.data?.[pageName] ?? {};
  }, [context, pageName]);
}

export default useCustomChart;
