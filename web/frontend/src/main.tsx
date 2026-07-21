import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ConfigProvider } from "antd";
import "./index.css";
import { App } from "./App";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // 커넥터 상태가 자주 바뀔 수 있어 창 포커스 돌아올 때마다 새로고침.
      refetchOnWindowFocus: true,
      staleTime: 5_000,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <ConfigProvider>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </ConfigProvider>
    </QueryClientProvider>
  </StrictMode>,
);
