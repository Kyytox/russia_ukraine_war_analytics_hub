import TabbedPage from "../components/TabbedPage.jsx";

// Tab components
import Overview from "./railways/Overview.jsx";
import IncidentTypes from "./railways/IncidentTypes.jsx";
import DamagedEquipments from "./railways/DamagedEquipments.jsx";
import Collisions from "./railways/Collisions.jsx";
import Sabotage from "./railways/Sabotage.jsx";
import PartisansArrest from "./railways/PartisansArrest.jsx";
import Informations from "./railways/Informations.jsx";

// URL path (kebab-case, used in the route) -> i18n label key + component
export const TABS = [
  { path: "overview", label: "page.tabs.overview", Component: Overview },
  { path: "incident-types", label: "page.tabs.incident_types", Component: IncidentTypes },
  { path: "damaged-equipments", label: "page.tabs.damaged_equipments", Component: DamagedEquipments },
  { path: "collisions", label: "page.tabs.collisions", Component: Collisions },
  { path: "sabotage", label: "page.tabs.sabotage", Component: Sabotage },
  { path: "partisans-arrest", label: "page.tabs.partisans_arrest", Component: PartisansArrest },
  { path: "informations", label: "page.tabs.informations", Component: Informations },
];

const PAGE_NAME = "incidents-railways";

export default function IncidentsRailways() {
  return (
    <TabbedPage
      pageName={PAGE_NAME}
      titleKey={`sections.${PAGE_NAME}.title`}
      descriptionKey={`sections.${PAGE_NAME}.description`}
      tabs={TABS}
    />
  );
}
