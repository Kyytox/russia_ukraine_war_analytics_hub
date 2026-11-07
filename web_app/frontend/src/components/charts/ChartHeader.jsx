import HelpTip from "../../components/HelpTip.jsx";

export default function ChartHeader({ pageKey, chartKey, t, year }) {
  const yearSuffix = year ? ` ${year}` : "";
  const fullKey = chartKey.includes(".") ? chartKey : `${pageKey}.${chartKey}`;

  // check if halp exists
  const helpText = t(`${fullKey}.help`);
  const showHelp = helpText !== `${fullKey}.help`;

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
