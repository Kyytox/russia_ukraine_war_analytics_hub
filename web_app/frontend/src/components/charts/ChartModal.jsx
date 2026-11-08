import { useState } from "react";
import { createPortal } from "react-dom";
import { Children, cloneElement, isValidElement } from "react";
import ChartHeader from "./ChartHeader.jsx";

export default function ChartModal({ children, pageKey, chartKey, t, year }) {
  const [isModalOpen, setIsModalOpen] = useState(false);

  return (
    <>
      <div className="chart-modal-wrapper">
        {children}
        <button
          onClick={() => setIsModalOpen(true)}
          className="chart-expand-btn"
          aria-label={t("common.expand")}
          title={t("common.expand")}
        >
          {t("common.expand")}
        </button>
      </div>

      {isModalOpen && (
        <ModalPortal onClose={() => setIsModalOpen(false)}>
          <div
            className="chart-modal-overlay"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="chart-modal-content">
              <div className="chart-modal-header">
                <div className="chart-modal-title">
                  <ChartHeader
                    pageKey={pageKey}
                    chartKey={chartKey}
                    t={t}
                    year={year}
                  />
                </div>
                <button
                  onClick={() => setIsModalOpen(false)}
                  className="chart-modal-close"
                  aria-label={t("common.collapse")}
                >
                  ×
                </button>
              </div>
              <div className="chart-modal-body">
                {Children.map(children, (child) => {
                  if (isValidElement(child)) {
                    return cloneElement(child, {
                      height: "100%",
                    });
                  }
                  return child;
                })}
              </div>
            </div>
          </div>
        </ModalPortal>
      )}
    </>
  );
}

// Composant pour créer un portail vers le body
function ModalPortal({ children, onClose }) {
  return createPortal(
    <div className="chart-modal-backdrop" onClick={onClose}>
      {children}
    </div>,
    document.body,
  );
}
