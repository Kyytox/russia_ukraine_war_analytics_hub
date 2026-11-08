import TabbedPage from "../components/TabbedPage.jsx";

// Tab components
import Overview from "./components-weapons/Overview.jsx";
import Methodology from "./components-weapons/Methodology.jsx";

// URL path (kebab-case, used in the route) -> i18n label key + component
export const TABS = [
  { path: "overview", label: "page.tabs.overview", Component: Overview },
  { path: "methodology", label: "page.tabs.methodology", Component: Methodology },
];

const PAGE_NAME = "components-weapons";

export default function ComponentsWeapons() {
  return (
    <TabbedPage
      pageName={PAGE_NAME}
      titleKey="page.title"
      descriptionKey="page.description"
      tabs={TABS}
    />
  );
}
