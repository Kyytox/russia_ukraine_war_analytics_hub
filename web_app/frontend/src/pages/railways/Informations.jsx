import { useApi } from "../../hooks/useApi.js";
import ChartState from "../../components/charts/ChartState.jsx";

// All page text is translated here via i18n keys (page.* in
// frontend/src/i18n/<lang>/incidents-railways.json).
// Only the source links come from the backend endpoint; the order of the
// name keys below must match the URL arrays returned by the API.
const TWITTER_SOURCES = [
  "page.methodology.data_sources.twitter.LX",
  "page.methodology.data_sources.twitter.Intelschizo",
  "page.methodology.data_sources.twitter.Prune60",
  "page.methodology.data_sources.twitter.NOELreports",
  "page.methodology.data_sources.twitter.Igor_Sushko",
];

const TELEGRAM_SOURCES = [
  "page.methodology.data_sources.telegram.Electrichki",
  "page.methodology.data_sources.telegram.Astra_News",
  "page.methodology.data_sources.telegram.SHOT",
  "page.methodology.data_sources.telegram.Atesh",
  "page.methodology.data_sources.telegram.Legion_of_Freedom",
  "page.methodology.data_sources.telegram.VD_Legion_of_Freedom",
  "page.methodology.data_sources.telegram.Enews112",
  "page.methodology.data_sources.telegram.TeleRZD",
  "page.methodology.data_sources.telegram.Mzd_Rzd",
  "page.methodology.data_sources.telegram.Magistral_Kuvalda",
  "page.methodology.data_sources.telegram.NSKZD",
  "page.methodology.data_sources.telegram.News_ZSZD",
  "page.methodology.data_sources.telegram.D4msk",
  "page.methodology.data_sources.telegram.Rospartizan",
  "page.methodology.data_sources.telegram.Boakom",
  "page.methodology.data_sources.telegram.RDPSRU",
  "page.methodology.data_sources.telegram.Ostanovi_Vagony_2023",
  "page.methodology.data_sources.telegram.Activatica",
  "page.methodology.data_sources.telegram.Russvolcorps",
];

const METHODOLOGY_SECTIONS = {
  classification: [
    "page.methodology.classification.content_1",
    "page.methodology.classification.content_2",
    "page.methodology.classification.content_3",
    "page.methodology.classification.content_4",
    // "page.methodology.classification.content_5",
    // "page.methodology.classification.content_6",
  ],
  data_collection: [
    "page.methodology.data_collection.content_1",
    "page.methodology.data_collection.content_2",
    "page.methodology.data_collection.content_3",
    "page.methodology.data_collection.content_4",
  ],
  data_processing: [
    "page.methodology.data_processing.content_1",
    "page.methodology.data_processing.content_2",
    "page.methodology.data_processing.content_3",
    "page.methodology.data_processing.content_4",
    "page.methodology.data_processing.content_5",
  ],
  geographic_coverage: [
    "page.methodology.geographic_coverage.content_1",
    "page.methodology.geographic_coverage.content_2",
    "page.methodology.geographic_coverage.content_3",
    "page.methodology.geographic_coverage.content_4",
  ],
  temporal_coverage: [
    "page.methodology.temporal_coverage.content_1",
    "page.methodology.temporal_coverage.content_2",
    "page.methodology.temporal_coverage.content_3",
    "page.methodology.temporal_coverage.content_4",
  ],
  limitations: [
    "page.methodology.limitations.content_1",
    "page.methodology.limitations.content_2",
    "page.methodology.limitations.content_3",
    "page.methodology.limitations.content_4",
    "page.methodology.limitations.content_5",
    "page.methodology.limitations.content_6",
  ],
  data_quality: [
    "page.methodology.data_quality.content_1",
    "page.methodology.data_quality.content_2",
    "page.methodology.data_quality.content_3",
    "page.methodology.data_quality.content_4",
    "page.methodology.data_quality.content_5",
  ],
  ethical_considerations: [
    "page.methodology.ethical_considerations.content_1",
    "page.methodology.ethical_considerations.content_2",
    "page.methodology.ethical_considerations.content_3",
    "page.methodology.ethical_considerations.content_4",
    "page.methodology.ethical_considerations.content_5",
  ],
};

const AI_SECTIONS = {
  jev: [
    "page.informations.ai_jev_content_1",
    "page.informations.ai_jev_content_2",
  ],
  ollama: [
    "page.informations.ai_ollama_content_1",
    "page.informations.ai_ollama_content_2",
  ],
};

const MANUAL_VALIDATION = [
  "page.informations.manual_validation_content_1",
  "page.informations.manual_validation_content_2",
  "page.informations.manual_validation_content_3",
];

const INCIDENT_TYPES = [
  "page.content.Derailment",
  "page.content.Collision",
  "page.content.Sabotage",
  "page.content.Fire",
  "page.content.Attack",
  "page.content.Other",
];

const EQUIPMENT_TYPES = [
  "page.content.Electric Box",
  "page.content.Freight Train",
  "page.content.Infrastructure",
  "page.content.Locomotive",
  "page.content.Passengers Train",
  "page.content.Railroad Tracks",
  "page.content.Relay Cabin",
  "page.content.Unknown",
];

export default function Informations({ PAGE, t, i18n, nf }) {
  // Only the source links are fetched from the API
  const sources = useApi("/api/incidents-railways/informations/data");

  const paragraphs = (keys) => keys.map((key) => <p key={key}>{t(key)}</p>);

  return (
    <div className="informations-content">
      {/* Description Section */}
      <section className="info-section">
        <h2>{t(`page.informations.description_title`)}</h2>
        <div className="info-text">
          <p>{t(`page.informations.description_1`)}</p>
          <p>{t(`page.informations.description_2`)}</p>
          <p>{t(`page.informations.description_3`)}</p>
        </div>
      </section>

      {/* Crimea Note */}
      <div className="warning-box">
        <p>
          <strong>{t(`page.informations.crimea_note_title`)}</strong>
          {t(`page.informations.crimea_note`)}
        </p>
      </div>

      {/* Data Sources Section */}
      <section className="info-section">
        <h2>{t(`page.informations.data_sources_title`)}</h2>

        <div className="infos-box">
          <p>{t(`page.informations.data_sources_warning`)}</p>
        </div>

        <div className="source-button-container">
          <a
            href="https://docs.google.com/spreadsheets/d/1jyD1bB0uauqIo-Bsi_qoBqV9JAu7cUXvG0UzZmFrSPk/edit?usp=sharing"
            target="_blank"
            rel="noopener noreferrer"
            className="source-button"
          >
            {t(`page.informations.source_button`)}
          </a>
        </div>

        <ChartState query={sources} height={200}>
          {(data) => (
            <div className="sources-grid">
              <div className="source-column">
                <h3>{t(`page.informations.twitter_title`)}</h3>
                <ul className="source-list">
                  {TWITTER_SOURCES.map((key, index) => (
                    <li key={key}>
                      <a
                        href={data?.twitter_accounts?.[index]}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        {t(key)}
                      </a>
                    </li>
                  ))}
                </ul>
              </div>

              <div className="source-column">
                <h3>{t(`page.informations.telegram_title`)}</h3>
                <ul className="source-list">
                  {TELEGRAM_SOURCES.map((key, index) => (
                    <li key={key}>
                      <a
                        href={data?.telegram_channels?.[index]}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        {t(key)}
                      </a>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          )}
        </ChartState>
      </section>

      {/* Methodology Section */}
      <section className="info-section">
        <h2>{t(`page.informations.methodology_title`)}</h2>

        <div className="methodology-intro">
          <p>{t(`page.informations.methodology_intro`)}</p>
        </div>

        <div className="methodology-subsection">
          <h3>{t(`page.informations.data_collection_title`)}</h3>
          {paragraphs(METHODOLOGY_SECTIONS.data_collection)}
        </div>

        <div className="methodology-subsection">
          <h3>{t(`page.informations.data_processing_title`)}</h3>
          {paragraphs(METHODOLOGY_SECTIONS.data_processing)}
        </div>

        <div className="methodology-subsection">
          <h3>{t(`page.informations.classification_title`)}</h3>
          {paragraphs(METHODOLOGY_SECTIONS.classification)}
        </div>

        {/* AI Usage Section */}
        <div className="methodology-subsection">
          <h3>{t(`page.informations.ai_title`)}</h3>
          <p>{t(`page.informations.ai_description`)}</p>

          <div className="ai-subsection">
            <h4>{t(`page.informations.ai_jev_title`)}</h4>
            {paragraphs(AI_SECTIONS.jev)}
          </div>

          <div className="ai-subsection">
            <h4>{t(`page.informations.ai_ollama_title`)}</h4>
            {paragraphs(AI_SECTIONS.ollama)}
          </div>
        </div>

        {/* Manual Validation Section */}
        <div className="methodology-subsection">
          <h3>{t(`page.informations.manual_validation_title`)}</h3>
          {paragraphs(MANUAL_VALIDATION)}
        </div>
      </section>

      {/* Geographic Coverage */}
      <section className="info-section">
        <h2>{t(`page.informations.geographic_title`)}</h2>
        <div className="info-text">
          {paragraphs(METHODOLOGY_SECTIONS.geographic_coverage)}
        </div>
      </section>

      {/* Temporal Coverage */}
      <section className="info-section">
        <h2>{t(`page.informations.temporal_title`)}</h2>
        <div className="info-text">
          {paragraphs(METHODOLOGY_SECTIONS.temporal_coverage)}
        </div>
      </section>

      {/* Limitations */}
      <section className="info-section">
        <h2>{t(`page.informations.limitations_title`)}</h2>
        <div className="infos-box">
          {paragraphs(METHODOLOGY_SECTIONS.limitations)}
        </div>
      </section>

      {/* Data Quality */}
      <section className="info-section">
        <h2>{t(`page.informations.quality_title`)}</h2>
        <div className="info-text">
          {paragraphs(METHODOLOGY_SECTIONS.data_quality)}
        </div>
      </section>

      {/* Ethical Considerations */}
      <section className="info-section">
        <h2>{t(`page.informations.ethical_title`)}</h2>
        <div className="info-text">
          {paragraphs(METHODOLOGY_SECTIONS.ethical_considerations)}
        </div>
      </section>

      {/* Classification Details 
      <section className="info-section">
        <h2>{t(`page.informations.classification_details_title`)}</h2>

        <div className="classification-grid">
          <div className="classification-column">
            <h3>{t(`page.informations.incident_types_title`)}</h3>
            <ul className="classification-list">
              {INCIDENT_TYPES.map((key) => (
                <li key={key}>{t(key)}</li>
              ))}
            </ul>
          </div>

          <div className="classification-column">
            <h3>{t(`page.informations.equipment_types_title`)}</h3>
            <ul className="classification-list">
              {EQUIPMENT_TYPES.map((key) => (
                <li key={key}>{t(key)}</li>
              ))}
            </ul>
          </div>
        </div>
      </section>
      */}
    </div>
  );
}
