import { createContext, useMemo } from "react";

/**
 * Context global pour les configurations custom (charts, colors, etc.)
 * Fonctionne comme i18n mais pour les données de configuration
 */
export const CustomContext = createContext(null);

export function CustomProvider({ children, customData = {} }) {
  const value = useMemo(() => ({
    data: customData,
  }), [customData]);

  return (
    <CustomContext.Provider value={value}>
      {children}
    </CustomContext.Provider>
  );
}
