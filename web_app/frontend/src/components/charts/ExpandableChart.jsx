import { useState } from "react";

export default function ExpandableChart({
  children,
  id,
  title,
  t,
  initialHeight = 440,
  expandedHeight = 800,
}) {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <div
      className={`expandable-chart-container ${isExpanded ? "expanded" : ""}`}
    >
      <div className="expandable-chart-header">
        {title && <h3 className="chart-title">{title}</h3>}
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="expand-toggle-btn"
          aria-label={isExpanded ? t("common.collapse") : t("common.expand")}
        >
          {isExpanded ? t("common.collapse") : t("common.expand")}
        </button>
      </div>
      <div
        className="expandable-chart-content"
        style={{ height: isExpanded ? expandedHeight : initialHeight }}
      >
        {children}
      </div>
    </div>
  );
}
