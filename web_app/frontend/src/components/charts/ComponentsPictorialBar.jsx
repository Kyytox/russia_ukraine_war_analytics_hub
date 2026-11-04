import { User } from "lucide-react";

export function ComponentsPictorialBar({
  data,
  iconColor = "#4CAF50",
  iconSize = 20,
}) {
  return (
    <div
      style={{
        display: "flex",
        gap: "40px",
        alignItems: "center",
        flexWrap: "wrap",
        padding: "20px",
        justifyContent: "center",
      }}
    >
      {data.map((d) => (
        <div
          key={d.metric}
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
          }}
        >
          <div
            style={{
              color: "#fff",
              fontSize: "12px",
              textAlign: "center",
              marginBottom: "6px",
            }}
          >
            Age: {d.metric}
          </div>
          <div
            style={{
              display: "flex",
              flexDirection: "row",
              flexWrap: "wrap",
              gap: "3px",
              marginBottom: "8px",
              maxWidth: "400px",
              justifyContent: "center",
            }}
          >
            {Array.from({ length: d.value }).map((_, i) => (
              <User
                key={i}
                size={iconSize}
                color={iconColor}
                style={{ flexShrink: 0 }}
              />
            ))}
          </div>
          <div style={{ color: "#fff", fontSize: "12px", textAlign: "center" }}>
            <strong>{d.value}</strong>
          </div>
        </div>
      ))}
    </div>
  );
}
