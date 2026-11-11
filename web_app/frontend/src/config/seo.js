/**
 * SEO configuration for each page
 * Each page has: title, description, breadcrumb, and type
 */

const BASE_URL = "https://ukraine-war-datahub.eu";

// Breadcrumb helper - generates breadcrumb items for JSON-LD
function generateBreadcrumb(pathname, t) {
  const parts = pathname.split("/").filter(Boolean);
  const breadcrumb = [];

  // Home
  breadcrumb.push({
    name: t("nav.home", { defaultValue: "Home" }),
    item: `${BASE_URL}/${parts[0] || "en"}`,
  });

  // Add language prefix if present
  const lang = parts[0];
  const pathParts = parts.slice(1);

  if (pathParts.length === 0) {
    return breadcrumb;
  }

  // First level (section)
  if (pathParts.length >= 1) {
    const sectionId = pathParts[0];
    breadcrumb.push({
      name: t(`sections.${sectionId}.title`, { defaultValue: sectionId }),
      item: `${BASE_URL}/${lang}/${sectionId}`,
    });
  }

  // Second level (tab)
  if (pathParts.length >= 2) {
    const tabId = pathParts[1];
    breadcrumb.push({
      name: t(`page.tabs.${tabId}`, { defaultValue: tabId }),
      item: `${BASE_URL}/${lang}/${pathParts[0]}/${tabId}`,
    });
  }

  return breadcrumb;
}

// SEO configurations for each page type
export const seoConfig = {
  home: {
    title: "site.title",
    description: "site.description",
    type: "WebPage",
    getBreadcrumb: (pathname, t) => [
      {
        name: t("nav.home", { defaultValue: "Home" }),
        item: BASE_URL + pathname,
      },
    ],
  },
  "incidents-railways": {
    title: "sections.incidents-railways.title",
    description: "sections.incidents-railways.description",
    type: "WebPage",
    getBreadcrumb: generateBreadcrumb,
  },
  "blocked-sites": {
    title: "sections.blocked-sites.title",
    description: "sections.blocked-sites.description",
    type: "WebPage",
    getBreadcrumb: generateBreadcrumb,
  },
  "raid-alerts": {
    title: "sections.raid-alerts.title",
    description: "sections.raid-alerts.intro",
    type: "WebPage",
    getBreadcrumb: generateBreadcrumb,
  },
  "components-weapons": {
    title: "page.title",
    description: "page.description",
    type: "WebPage",
    getBreadcrumb: generateBreadcrumb,
  },
  "interactive-maps": {
    title: "sections.interactive-maps.title",
    description: "sections.interactive-maps.intro",
    type: "WebPage",
    getBreadcrumb: generateBreadcrumb,
  },
  "military-losses": {
    title: "sections.military-losses.title",
    description: "sections.military-losses.intro",
    type: "WebPage",
    getBreadcrumb: generateBreadcrumb,
  },
};

// Get SEO config for current page
export function getSEOConfig(pageName, pathname, t) {
  const config = seoConfig[pageName] || seoConfig.home;

  return {
    title: t(config.title),
    description: t(config.description),
    type: config.type,
    breadcrumb: config.getBreadcrumb(pathname, t),
  };
}

export { BASE_URL };
