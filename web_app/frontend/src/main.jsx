import React from "react";
import { createRoot } from "react-dom/client";
import { initializeI18n } from "./i18n/index.js";
import App from "./App.jsx";
import "./styles.css";
import { loadCustomData } from "./utils/customLoader.js";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: Infinity,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
});

// Initialize i18n and load custom data, then render
Promise.all([
  initializeI18n(),
  loadCustomData()
]).then(() => {
  createRoot(document.getElementById("root")).render(
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>,
  );
});
