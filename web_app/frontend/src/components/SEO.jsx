import { Helmet } from "react-helmet-async";
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";

export default function SEO({
  title = null,
  description = null,
  type = "WebPage",
  breadcrumb = [],
  image = null,
  url = null,
}) {
  const { t, i18n } = useTranslation();
  const location = useLocation();
  const currentLang = i18n.language || "en";

  // Build full URL
  const baseUrl = "https://ukraine-war-datahub.eu";
  const fullUrl = url || `${baseUrl}${location.pathname}`;

  // Default title and description from i18n
  const siteTitle = t("site.title", { defaultValue: "Ukraine War Data Hub" });
  const siteDescription = t("site.description", {
    defaultValue:
      "Comprehensive data and analytics on the Russia-Ukraine war: railway incidents, blocked websites, weapon components, raid alerts, and more.",
  });

  const pageTitle = title ? `${title} | ${siteTitle}` : siteTitle;
  const pageDescription = description || siteDescription;

  // Default OG image
  const ogImage = image || `${baseUrl}/img/logo-ws.svg`;

  // Build breadcrumb JSON-LD
  const breadcrumbJsonLd =
    breadcrumb.length > 0
      ? {
          "@context": "https://schema.org",
          "@type": "BreadcrumbList",
          itemListElement: breadcrumb.map((item, index) => ({
            "@type": "ListItem",
            position: index + 1,
            name: item.name,
            item: item.item,
          })),
        }
      : null;

  // Build main Schema.org JSON-LD
  const mainJsonLd = {
    "@context": "https://schema.org",
    "@type": type,
    name: pageTitle,
    description: pageDescription,
    url: fullUrl,
    inLanguage: currentLang,
  };

  // Add Organization info
  const organizationJsonLd = {
    "@context": "https://schema.org",
    "@type": "Organization",
    name: siteTitle,
    url: baseUrl,
    logo: `${baseUrl}/img/logo-ws.svg`,
    description: siteDescription,
  };

  // Build alternate links for hreflang
  const langPaths = {
    en: location.pathname.replace(/^\/\w+/, ""),
    fr: location.pathname.replace(/^\/\w+/, ""),
    ua: location.pathname.replace(/^\/\w+/, ""),
  };

  return (
    <>
      {/* Standard Meta Tags */}
      <Helmet>
        {/* Title */}
        <title>{pageTitle}</title>

        {/* Standard Meta */}
        <meta name="description" content={pageDescription} />
        <meta
          name="keywords"
          content="Ukraine, Russia, war, data, analytics, railway incidents, blocked websites, weapon components, raid alerts, military losses"
        />
        <meta name="author" content="Kytox" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />

        {/* OpenGraph / Facebook */}
        <meta property="og:type" content="website" />
        <meta property="og:url" content={fullUrl} />
        <meta property="og:title" content={pageTitle} />
        <meta property="og:description" content={pageDescription} />
        <meta property="og:image" content={ogImage} />
        <meta property="og:site_name" content={siteTitle} />
        <meta property="og:locale" content={currentLang} />

        {/* Twitter */}
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content={pageTitle} />
        <meta name="twitter:description" content={pageDescription} />
        <meta name="twitter:image" content={ogImage} />
        <meta name="twitter:site" content="@UkraineWarData" />

        {/* Canonical */}
        <link rel="canonical" href={fullUrl} />

        {/* Hreflang */}
        {Object.entries(langPaths).map(([lang, path]) => (
          <link
            key={lang}
            rel="alternate"
            hreflang={lang}
            href={`${baseUrl}/${lang}${path}`}
          />
        ))}
        <link
          rel="alternate"
          hreflang="x-default"
          href={`${baseUrl}/en${langPaths.en}`}
        />

        {/* Favicon */}
        <link
          rel="icon"
          type="image/svg+xml"
          href={`${baseUrl}/img/logo-ws.svg`}
        />
      </Helmet>

      {/* Umami */}
      <script
        defer
        src="https://umami.kytox-lab.com/script.js"
        data-website-id="38170119-79a9-4280-9f7d-3a41b9f37692"
      ></script>

      {/* Structured Data (JSON-LD) */}
      <script type="application/ld+json">{JSON.stringify(mainJsonLd)}</script>

      {breadcrumbJsonLd && (
        <script type="application/ld+json">
          {JSON.stringify(breadcrumbJsonLd)}
        </script>
      )}

      <script type="application/ld+json">
        {JSON.stringify(organizationJsonLd)}
      </script>
    </>
  );
}
