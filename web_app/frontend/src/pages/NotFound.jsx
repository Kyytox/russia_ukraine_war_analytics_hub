import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";

export default function NotFound() {
  const { t } = useTranslation();
  return <section className="hero"><h1>{t("notFound.title")}</h1><p><Link to="/">{t("notFound.back")}</Link></p></section>;
}
