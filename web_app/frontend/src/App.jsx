import { BrowserRouter, Route, Routes, Navigate } from "react-router-dom";
import Layout from "./components/Layout.jsx";
import Home from "./pages/Home.jsx";
import Railways from "./pages/Railways.jsx";
import BlockedSites from "./pages/BlockedSites.jsx";
import RaidAlerts from "./pages/RaidAlerts.jsx";
import ComponentsWeapons from "./pages/ComponentsWeapons.jsx";
import Maps from "./pages/Maps.jsx";
import Losses from "./pages/Losses.jsx";
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
              element={<LanguageWrapper lang={lang} />}
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
        <Route path="incidents-railways" element={<Railways />} />
        <Route path="blocked-sites" element={<BlockedSites />} />
        <Route path="raid-alerts" element={<RaidAlerts />} />
        <Route path="components-weapons" element={<ComponentsWeapons />} />
        <Route path="interactive-maps" element={<Maps />} />
        <Route path="military-losses" element={<Losses />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
