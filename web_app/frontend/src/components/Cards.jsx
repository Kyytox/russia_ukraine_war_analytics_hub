export default function Cards({ sections, t, navigate, typeLink }) {
  return (
    <section className="grid">
      {sections.map((s) => (
        <button
          key={s.id}
          className="card"
          onClick={() => {
            if (typeLink === "internal") {
              navigate(`/${s.id}`);
            }
            if (typeLink === "external") {
              window.open(s.url, "_blank");
            }
          }}
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
            <h3>{t(`page.${s.id}.title`)}</h3>
            <p>{t(`page.${s.id}.description`)}</p>
          </div>
        </button>
      ))}
    </section>
  );
}
