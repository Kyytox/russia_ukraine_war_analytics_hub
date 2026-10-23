import { useTranslation } from "react-i18next";

export default function ChartState({ query, height = 400, children }) {
  const { t } = useTranslation();

  if (query.isPending)
    return (
      <div className="placeholder" style={{ height }}>
        {t("common.loading")}
      </div>
    );
  if (query.isError)
    return (
      <div className="placeholder" style={{ height }}>
        {t("common.chartError")}
      </div>
    );
  return children(query.data);
}
