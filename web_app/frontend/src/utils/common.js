export function getPageFromPath(path) {
  const parts = path.split("/");
  return parts[2] || "home";
}

export function getMetricColor(custom, name, defaultColor) {
  if (custom?.[name]) {
    return custom[name];
  } else if (defaultColor) {
    return defaultColor;
  } else {
    return "#7F557D";
  }
}

/**
 * Format KPI value based on type
 * - Numeric: formatted with locale
 * - Date (YYYY-MM-DD or YYYY-MM): formatted as date
 * - Other: returned as-is
 */
export function formatKpiValue(value, locale) {
  if (!value) return value;

  // Check if numeric (starts with digit)
  if (/^\d/.test(value)) {
    // Check if it's a date (YYYY-MM-DD or YYYY-MM format)
    if (/^\d{4}-\d{2}(-\d{2})?$/.test(value)) {
      const date = new Date(value + "T00:00:00Z"); // Add time to parse as UTC
      if (!isNaN(date.getTime())) {
        // Format as date
        return new Intl.DateTimeFormat(locale, {
          year: "numeric",
          month: "long",
          day: value.length === 10 ? "numeric" : undefined,
        }).format(date);
      }
    }

    // Otherwise try to format as number
    const num = Number(value);
    if (!isNaN(num)) {
      return new Intl.NumberFormat(locale).format(num);
    }
  }

  return value;
}
