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
