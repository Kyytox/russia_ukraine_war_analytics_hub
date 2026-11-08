// All text is translated via i18n keys (page.methodology.* in
// frontend/src/i18n/<lang>/blocked-sites.json). No backend call needed here.
const METHODOLOGY_SECTIONS = {
  data_source: [
    "page.methodology.data_source_1",
    "page.methodology.data_source_2",
  ],
  processing: ["page.methodology.processing_1", "page.methodology.processing_2"],
  limitations: [
    "page.methodology.limitations_1",
    "page.methodology.limitations_2",
    "page.methodology.limitations_3",
  ],
};

export default function Methodology({ PAGE, t, i18n, nf }) {
  const paragraphs = (keys) => keys.map((key) => <p key={key}>{t(key)}</p>);

  return (
    <div className="informations-content">
      {/* Methodology Section */}
      <section className="info-section">
        <h2>{t("page.methodology.title")}</h2>
        <div className="methodology-intro">
          <p>{t("page.methodology.intro")}</p>
        </div>
      </section>

      {/* Data Source Section */}
      <section className="info-section">
        <h2>{t("page.methodology.data_source_title")}</h2>
        <div className="info-text">{paragraphs(METHODOLOGY_SECTIONS.data_source)}</div>
        <div className="source-button-container">
          <a
            href="https://www.top10vpn.com/research/websites-blocked-in-russia/"
            target="_blank"
            rel="noopener noreferrer"
            className="source-button"
          >
            {t("page.methodology.source_button")}
          </a>
        </div>
      </section>

      {/* Data Processing Section */}
      <section className="info-section">
        <h2>{t("page.methodology.processing_title")}</h2>
        <div className="info-text">{paragraphs(METHODOLOGY_SECTIONS.processing)}</div>
      </section>

      {/* Limitations Section */}
      <section className="info-section">
        <h2>{t("page.methodology.limitations_title")}</h2>
        <div className="warning-box">{paragraphs(METHODOLOGY_SECTIONS.limitations)}</div>
      </section>
    </div>
  );
}
