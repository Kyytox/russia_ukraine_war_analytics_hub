import { useTranslation } from "react-i18next";

export default function ChartState({ query, height, children }) {
  const { t } = useTranslation();
  const placeholderHeight = height || 400;

  if (query.isPending)
    return (
      <div className="placeholder" style={{ height: placeholderHeight }}>
        {t("common.loading")}
      </div>
    );
  if (query.isError)
    return (
      <div className="placeholder" style={{ height: placeholderHeight }}>
        {t("common.chartError")}
      </div>
    );
  return children(query.data, height);
}
