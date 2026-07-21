import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    // 로컬 개발(npm run dev)에서만 쓰임. 운영(Docker)에서는 nginx.conf의
    // reverse proxy가 같은 역할을 한다.
    proxy: {
      "/api": {
        target: "http://localhost:8081",
        changeOrigin: true,
      },
    },
  },
});
