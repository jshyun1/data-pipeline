import { Navigate, Route, Routes } from "react-router-dom";
import { AppLayout } from "./components/AppLayout";
import { RequireAuth } from "./auth/RequireAuth";
import { ConnectionsPage } from "./pages/ConnectionsPage";
import { CdcLogsPage } from "./pages/CdcLogsPage";
import { AirflowDashboardPage } from "./pages/AirflowDashboardPage";
import { ConsoleFramePage } from "./pages/ConsoleFramePage";
import { AlertQueuePage } from "./pages/AlertQueuePage";
import { DashboardPage } from "./pages/DashboardPage";
import { EtlCreatePage } from "./pages/EtlCreatePage";
import { EtlLogsPage } from "./pages/EtlLogsPage";
import { LoginPage } from "./pages/LoginPage";
import { PipelinesPage } from "./pages/PipelinesPage";

export function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route element={<RequireAuth />}>
        <Route element={<AppLayout />}>
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/alerts" element={<AlertQueuePage />} />
        <Route path="/airflow" element={<Navigate to="/airflow/dashboard" replace />} />
        <Route path="/airflow/dashboard" element={<AirflowDashboardPage />} />
        <Route
          path="/airflow/manage"
          element={
            <ConsoleFramePage
              title="AirFlow 관리"
              src="/airflow/"
              healthcheckSrc="/airflow/"
              waitMessage="AirFlow 관리 콘솔을 준비하는 중입니다"
            />
          }
        />
        <Route path="/etl" element={<Navigate to="/etl/create" replace />} />
        <Route path="/etl/create" element={<EtlCreatePage />} />
        <Route
          path="/etl/manage"
          element={
            <ConsoleFramePage
              title="ETL 관리"
              src="/nifi/"
              healthcheckSrc="/nifi/"
              waitMessage="NiFi 관리 콘솔을 준비하는 중입니다"
              showProcessGroupTree
            />
          }
        />
        <Route path="/etl/logs" element={<EtlLogsPage />} />
        <Route path="/cdc" element={<Navigate to="/cdc/pipelines" replace />} />
        <Route path="/cdc/pipelines" element={<PipelinesPage />} />
        <Route path="/cdc/connections" element={<ConnectionsPage />} />
        <Route path="/cdc/logs" element={<CdcLogsPage />} />
        <Route path="/connections" element={<Navigate to="/cdc/connections" replace />} />
        <Route path="/pipelines" element={<Navigate to="/cdc/pipelines" replace />} />
        </Route>
      </Route>
    </Routes>
  );
}
