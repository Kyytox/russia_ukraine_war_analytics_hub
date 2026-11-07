import { useContext, useMemo } from "react";
import { CustomContext } from "../context/CustomContext.jsx";

/**
 * Hook pour accéder aux configurations custom (colors, sizes, etc.) d'une page précise
 * Fonctionne comme useTranslation() mais pour custom.json
 *
 * Usage:
 * const custom = useConfigCharts("blocked-sites");
 * custom?.chart1?.colors // Récupère les colors pour ce chart
 *
 * @param {string} pageName - Le nom de la page (ex: "blocked-sites", "components-weapons")
 * @returns {object} Les configs pour cette page
 */
export function useConfigCharts(page, tab = null) {
  const context = useContext(CustomContext);
  var pageConfig = null;

  return useMemo(() => {
    if (!context) {
      console.warn(
        `useConfigCharts("${pageName}") used outside <CustomProvider>. Returning empty object.`,
      );
      return {};
    }

    // merge common, page-specific, and colors_metrics
    const commonConfig = context.data?.common ?? {};
    const colorsMetrics = context.data?.colors_metrics ?? {};

    if (tab !== null) {
      pageConfig = context.data?.[page]?.[tab] ?? {};
    } else {
      pageConfig = context.data?.[page] ?? {};
    }

    console.log(`useConfigCharts("${page}", "${tab}")`, {
      pageConfig,
      commonConfig,
      colorsMetrics,
    });

    return {
      ...pageConfig,
      common: commonConfig,
      colors_metrics: colorsMetrics,
    };
  }, [context, page, tab]);
}

export default useConfigCharts;
