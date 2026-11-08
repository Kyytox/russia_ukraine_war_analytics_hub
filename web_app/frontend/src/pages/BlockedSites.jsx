import TabbedPage from "../components/TabbedPage.jsx";

// Tab components
import Overview from "./blocked-sites/Overview.jsx";
import Methodology from "./blocked-sites/Methodology.jsx";

// URL path (kebab-case, used in the route) -> i18n label key + component
export const TABS = [
  { path: "overview", label: "page.tabs.overview", Component: Overview },
  { path: "methodology", label: "page.tabs.methodology", Component: Methodology },
];

const PAGE_NAME = "blocked-sites";

export default function BlockedSites() {
  return (
    <TabbedPage
      pageName={PAGE_NAME}
      titleKey={`sections.${PAGE_NAME}.title`}
      descriptionKey={`sections.${PAGE_NAME}.description`}
      tabs={TABS}
    />
  );
}
