/**
 * Centralized navigation configuration
 * Contains all navigation items with their id, label (i18n key), and icon
 */

import {
  TrainTrack,
  Ban,
  Bell,
  Cog,
  Map,
  Cross,
  Home,
} from "lucide-react";

// Main navigation items for sidebar
// Each item has: id (URL path), label (i18n key for nav.*), icon (Lucide component)
export const NAV_ITEMS = [
  { id: "home", label: "nav.home", icon: Home },
  { id: "incidents-railways", label: "nav.incidentsRailways", icon: TrainTrack },
  { id: "blocked-sites", label: "nav.blockedSites", icon: Ban },
  { id: "raid-alerts", label: "nav.raidAlerts", icon: Bell },
  { id: "components-weapons", label: "nav.componentsWeapons", icon: Cog },
  { id: "interactive-maps", label: "nav.interactiveMaps", icon: Map },
  { id: "military-losses", label: "nav.militaryLosses", icon: Cross },
];

// Filter out items that should not appear in sidebar
// (e.g., components-weapons is commented out in Layout.jsx)
export const SIDEBAR_NAV = NAV_ITEMS.filter(
  (item) => item.id !== "components-weapons"
);

// Filter out home for footer (home is already in sidebar as a separate button)
export const FOOTER_NAV = NAV_ITEMS.filter(
  (item) => item.id !== "home" && item.id !== "components-weapons"
);

// Get icon by id
export function getIconById(id) {
  const item = NAV_ITEMS.find((item) => item.id === id);
  return item ? item.icon : null;
}

// Get label i18n key by id
export function getLabelById(id) {
  const item = NAV_ITEMS.find((item) => item.id === id);
  return item ? item.label : id;
}
