/**
 * Loader pour custom.json
 * Charge les configurations custom au démarrage de l'app
 */

import configCharts from "./configCharts.json";
let customData = {};

/**
 * Charge custom.json une seule fois
 */
// export async function loadCustomData() {
//   try {
//     const response = await fetch("/src/config_charts/configCharts.json");
//     if (!response.ok) {
//       console.warn(`Failed to load configCharts.json: ${response.status}`);
//       return {};
//     }
//     customData = await response.json();
//     console.log("✅ Custom data loaded:", customData);
//     return customData;
//   } catch (error) {
//     console.error("❌ Error loading configCharts.json:", error);
//     return {};
//   }
// }

export function loadCustomData() {
  customData = configCharts;
  return customData;
}

/**
 * Retourne les données configCharts actuelles
 */
export function getCustomData() {
  return customData;
}

/**
 * Réinitialise les données configCharts (utile pour tests/dev)
 */
export function resetCustomData() {
  customData = {};
}
