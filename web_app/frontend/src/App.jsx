import { BrowserRouter, Route, Routes, Navigate } from "react-router-dom";
import Layout from "./components/Layout.jsx";
import { OutletContextWrapper } from "./components/TabbedPage.jsx";
import Home from "./pages/Home.jsx";
import IncidentsRailways, { TABS as RAILWAYS_TABS } from "./pages/IncidentsRailways.jsx";
import BlockedSites, { TABS as BLOCKED_SITES_TABS } from "./pages/BlockedSites.jsx";
import RaidAlerts from "./pages/RaidAlerts.jsx";
import ComponentsWeapons, { TABS as COMPONENTS_WEAPONS_TABS } from "./pages/ComponentsWeapons.jsx";
import InteractiveMaps from "./pages/InteractiveMaps.jsx";
import MilitaryLosses from "./pages/MilitaryLosses.jsx";
import NotFound from "./pages/NotFound.jsx";
import { CustomProvider } from "./context/CustomContext.jsx";
import { getCustomData } from "./utils/customLoader.js";
import {
  LANGUAGES,
  DEFAULT_LANGUAGE,
  getLanguageFromPath,
} from "./i18n/index.js";

export default function App() {
  const customData = getCustomData();

  return (
    <CustomProvider customData={customData}>
      <BrowserRouter>
        <Routes>
          {/* Redirect root to default language */}
          <Route
            path="/"
            element={<Navigate to={`/${DEFAULT_LANGUAGE}`} replace />}
          />

          {/* Routes with language prefix */}
          {LANGUAGES.map((lang) => (
            <Route
              key={lang}
              path={`${lang}/*`}
              element={<LanguageWrapper lang={lang} key={lang} />}
            />
          ))}

          {/* Catch-all for invalid language codes - redirect to default */}
          <Route
            path="*"
            element={<Navigate to={`/${DEFAULT_LANGUAGE}`} replace />}
          />
        </Routes>
      </BrowserRouter>
    </CustomProvider>
  );
}

/**
 * Nested routes for one tabbed page: index redirects to the default tab,
 * each tab is a real URL segment, unknown segments fall back to the default tab.
 */
function tabRoutes(tabs, defaultTab = "overview") {
  return (
    <>
      <Route index element={<Navigate to={defaultTab} replace />} />
      {tabs.map(({ path, Component }) => (
        <Route
          key={path}
          path={path}
          element={<OutletContextWrapper Component={Component} />}
        />
      ))}
      <Route path="*" element={<Navigate to={defaultTab} replace />} />
    </>
  );
}

/**
 * Wrapper that validates language and renders Layout with nested routes
 */
function LanguageWrapper({ lang }) {
  if (!LANGUAGES.includes(lang)) {
    return <Navigate to={`/${DEFAULT_LANGUAGE}`} replace />;
  }

  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Home />} />
        <Route path="incidents-railways" element={<IncidentsRailways />}>
          {tabRoutes(RAILWAYS_TABS)}
        </Route>
        <Route path="blocked-sites" element={<BlockedSites />}>
          {tabRoutes(BLOCKED_SITES_TABS)}
        </Route>
        <Route path="raid-alerts" element={<RaidAlerts />} />
        <Route path="components-weapons" element={<ComponentsWeapons />}>
          {tabRoutes(COMPONENTS_WEAPONS_TABS)}
        </Route>
        <Route path="interactive-maps" element={<InteractiveMaps />} />
        <Route path="military-losses" element={<MilitaryLosses />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
