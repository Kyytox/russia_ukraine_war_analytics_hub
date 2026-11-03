import { useApi } from "../../hooks/useApi.js";
import { usePageTranslation } from "../../hooks/usePageTranslation.js";
import ChartState from "../../components/charts/ChartState.jsx";

const PAGE_NAME = "incidents-railways";

export default function Informations() {
  const { t, i18n } = usePageTranslation(PAGE_NAME);

  // Appel API UNIQUEMENT quand ce composant se monte
  const data = useApi("/api/railways/informations/data");

  return (
    <ChartState query={data} height={400}>
      {(items) => (
        <div className="charts-grid">
          {/* Contenu à ajouter */}
        </div>
      )}
    </ChartState>
  );
}
