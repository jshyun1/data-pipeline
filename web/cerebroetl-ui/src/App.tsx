import { Navigate, Route, Routes } from "react-router-dom";
import { AppLayout } from "./components/AppLayout";
import { ConnectionsPage } from "./pages/ConnectionsPage";
import { ConsoleFramePage } from "./pages/ConsoleFramePage";
import { DashboardPage } from "./pages/DashboardPage";
import { EtlLogsPage } from "./pages/EtlLogsPage";
import { KafkaConnectPage } from "./pages/EtlPage";
import { PipelinesPage } from "./pages/PipelinesPage";

export function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/airflow" element={<Navigate to="/airflow/manage" replace />} />
        <Route
          path="/airflow/manage"
          element={<ConsoleFramePage kicker="WORKFLOW ORCHESTRATION" title="AirFlow 관리" src="http://localhost:8090" />}
        />
        <Route path="/etl" element={<Navigate to="/etl/manage" replace />} />
        <Route
          path="/etl/manage"
          element={
            <ConsoleFramePage
              kicker="ETL FLOW MANAGEMENT"
              title="ETL 관리"
              src="/nifi/"
              healthcheckSrc="/nifi/"
              waitMessage="NiFi 관리 콘솔을 준비하는 중입니다"
            />
          }
        />
        <Route path="/etl/logs" element={<EtlLogsPage />} />
        <Route path="/cdc" element={<Navigate to="/cdc/kafka-connect" replace />} />
        <Route path="/cdc/kafka-connect" element={<KafkaConnectPage />} />
        <Route path="/cdc/pipelines" element={<PipelinesPage />} />
        <Route path="/connections" element={<ConnectionsPage />} />
        <Route path="/pipelines" element={<Navigate to="/cdc/pipelines" replace />} />
      </Route>
    </Routes>
  );
}
