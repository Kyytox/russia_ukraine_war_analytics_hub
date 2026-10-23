import { useEffect, useState } from "react";
import { useLanguageNavigate } from "../hooks/useLanguage.js";
import { usePageTranslation } from "../hooks/usePageTranslation.js";
import { getPageFromPath } from "../utils/common.js";

export const TAG_COLORS = {
  Analytics: "#ff6b6b",
  Graphs: "#4abb15",
  Sources: "#14aca4",
  External: "#b36bff",
  Maps: "#4d7cff",
  "Data Sources": "#ffb36b",
  "External Data": "#ac4dff",
};

export default function Home() {
  const PAGE = getPageFromPath(window.location.pathname);
  const { t } = usePageTranslation(PAGE);
  const navigate = useLanguageNavigate();
  const [sections, setSections] = useState([]);
  const [error, setError] = useState(false);

  useEffect(() => {
    fetch("/api/sections")
      .then((r) => r.json())
      .then(setSections)
      .catch(() => setError(true));
  }, []);

  return (
    <>
      <section className="home">
        <section className="hero">
          <h1>{t("site.title")}</h1>
          <p>{t(`page.${PAGE}.intro`)} </p>
        </section>

        {error && <p className="error">{t("error.api")}</p>}

        <section className="grid">
          {sections.map((s) => (
            <button
              key={s.id}
              className="card"
              onClick={() => navigate(`/${s.id}`)}
              style={{
                border: "none",
                background: "none",
                cursor: "pointer",
                textAlign: "left",
              }}
            >
              <div
                className="thumb"
                style={{ backgroundImage: `url(${s.image})` }}
              />
              <div className="content">
                <h3>{t(`sections.${s.id}.title`)}</h3>
                <p>{t(`sections.${s.id}.description`)}</p>
                <ul className="tags">
                  {s.tags.map((tag) => (
                    <li
                      key={tag}
                      className="tag"
                      style={{ "--tag": TAG_COLORS[tag] }}
                    >
                      {t(`${tag}`)}
                    </li>
                  ))}
                </ul>
              </div>
            </button>
          ))}
        </section>
      </section>
    </>
  );
}
