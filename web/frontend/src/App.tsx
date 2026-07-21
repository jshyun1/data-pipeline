import { Navigate, Route, Routes } from "react-router-dom";
import { AppLayout } from "./components/AppLayout";
import { ConnectionsPage } from "./pages/ConnectionsPage";
import { PipelinesPage } from "./pages/PipelinesPage";

export function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route index element={<Navigate to="/pipelines" replace />} />
        <Route path="/connections" element={<ConnectionsPage />} />
        <Route path="/pipelines" element={<PipelinesPage />} />
      </Route>
    </Routes>
  );
}
