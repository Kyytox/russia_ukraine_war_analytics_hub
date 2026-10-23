import HelpTip from "../../components/HelpTip.jsx";

export default function ChartHeader({ pageKey, chartKey, t, year }) {
  const yearSuffix = year ? ` ${year}` : "";

  return (
    <>
      <h2>
        {t(`${pageKey}.${chartKey}.title`) + yearSuffix}
        <HelpTip>
          <p>{t(`${pageKey}.${chartKey}.help`) + yearSuffix}</p>
        </HelpTip>
      </h2>
      <h3>{t(`${pageKey}.${chartKey}.subtitle`) + yearSuffix}</h3>
    </>
  );
}
