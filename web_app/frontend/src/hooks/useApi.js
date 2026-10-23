import { useQuery } from "@tanstack/react-query";

async function getJson(url, signal) {
  const res = await fetch(url, { signal });
  if (!res.ok) throw new Error(`${res.status} ${url}`);
  return res.json();
}

export function useApi(url, options = {}) {
  return useQuery({
    queryKey: [url],
    queryFn: ({ signal }) => getJson(url, signal),
    ...options,
  });
}
