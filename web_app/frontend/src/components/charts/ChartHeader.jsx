import HelpTip from "../../components/HelpTip.jsx";

export default function ChartHeader({ pageKey, chartKey, t, year }) {
  const yearSuffix = year ? ` ${year}` : "";
  const fullKey = chartKey.includes(".") ? chartKey : `${pageKey}.${chartKey}`;
  const helpKey = `${fullKey}.help`;
  const helpText = t(helpKey);
  const showHelp = helpText !== "";

  return (
    <>
      <h2>
        {t(`${fullKey}.title`) + yearSuffix}
        {showHelp && (
          <HelpTip>
            <p>{helpText + yearSuffix}</p>
          </HelpTip>
        )}
      </h2>
      <h3>{t(`${fullKey}.subtitle`) + yearSuffix}</h3>
    </>
  );
}
