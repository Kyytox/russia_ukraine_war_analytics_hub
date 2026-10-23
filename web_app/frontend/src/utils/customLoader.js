/**
 * Loader pour custom.json
 * Charge les configurations custom au démarrage de l'app
 */

let customData = {};

/**
 * Charge custom.json une seule fois
 */
export async function loadCustomData() {
  try {
    const response = await fetch("/custom.json");
    if (!response.ok) {
      console.warn(`Failed to load custom.json: ${response.status}`);
      return {};
    }
    customData = await response.json();
    console.log("✅ Custom data loaded:", customData);
    return customData;
  } catch (error) {
    console.error("❌ Error loading custom.json:", error);
    return {};
  }
}

/**
 * Retourne les données custom actuelles
 */
export function getCustomData() {
  return customData;
}

/**
 * Réinitialise les données custom (utile pour tests/dev)
 */
export function resetCustomData() {
  customData = {};
}
