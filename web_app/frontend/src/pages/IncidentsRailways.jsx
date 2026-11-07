import { useState } from "react";
import { usePageTranslation } from "../hooks/usePageTranslation.js";
import { getPageFromPath } from "../utils/common.js";
import { useConfigCharts } from "../hooks/useConfigCharts.js";

// Importer tous les composants d'onglets
import Overview from "./railways/Overview.jsx";
import IncidentTypes from "./railways/IncidentTypes.jsx";
import DamagedEquipments from "./railways/DamagedEquipments.jsx";
import Collisions from "./railways/Collisions.jsx";
import Sabotage from "./railways/Sabotage.jsx";
import PartisansArrest from "./railways/PartisansArrest.jsx";
import Informations from "./railways/Informations.jsx";

// Map des composants d'onglets
const TAB_COMPONENTS = {
  0: Overview,
  1: IncidentTypes,
  2: DamagedEquipments,
  3: Collisions,
  4: Sabotage,
  5: PartisansArrest,
  6: Informations,
};

const TAB_CONFIG = [
  { id: "overview", label: "page.tabs.overview" },
  { id: "incident_types", label: "page.tabs.incident_types" },
  { id: "damaged_equipments", label: "page.tabs.damaged_equipments" },
  { id: "collisions", label: "page.tabs.collisions" },
  { id: "sabotage", label: "page.tabs.sabotage" },
  { id: "partisans_arrest", label: "page.tabs.partisans_arrest" },
  { id: "informations", label: "page.tabs.informations" },
];

const PAGE_NAME = "incidents-railways";

export default function IncidentsRailways() {
  const PAGE = getPageFromPath(window.location.pathname);
  const { t, i18n } = usePageTranslation(PAGE_NAME);
  const nf = new Intl.NumberFormat(i18n.language);
  const [activeTab, setActiveTab] = useState(0);

  // Récupère le composant actif
  const ActiveComponent = TAB_COMPONENTS[activeTab];

  return (
    <section className="page">
      <header className="page-head">
        <h1>{t(`sections.${PAGE_NAME}.title`)}</h1>
        <p>{t(`sections.${PAGE_NAME}.description`)}</p>
      </header>

      <div className="tabs-container">
        <div className="tabs-header" role="tablist">
          {TAB_CONFIG.map((tab, index) => (
            <button
              key={index}
              role="tab"
              aria-selected={activeTab === index}
              className={`tab-button ${activeTab === index ? "active" : ""}`}
              onClick={() => setActiveTab(index)}
            >
              {t(tab.label)}
            </button>
          ))}
        </div>

        {/* Contenu - UNIQUEMENT le composant actif se monte */}
        <div className="tabs-content">
          <div className="tab-panel active">
            <ActiveComponent PAGE={PAGE} t={t} i18n={i18n} nf={nf} />
          </div>
        </div>
      </div>
    </section>
  );
}
