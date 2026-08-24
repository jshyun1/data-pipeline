import { Navigate, Route, Routes, useSearchParams } from "react-router-dom";
import { AppLayout } from "./components/AppLayout";
import { RequireAuth } from "./auth/RequireAuth";
import { ConnectionsPage } from "./pages/ConnectionsPage";
import { CdcLogsPage } from "./pages/CdcLogsPage";
import { CdcCreatePage } from "./pages/CdcCreatePage";
import { AirflowDashboardPage } from "./pages/AirflowDashboardPage";
import { ConsoleFramePage } from "./pages/ConsoleFramePage";
import { DashboardPage } from "./pages/DashboardPage";
import { SettingsPage } from "./pages/SettingsPage";
import { AccountManagementPage } from "./pages/admin/AccountManagementPage";
import { RolePermissionPage } from "./pages/admin/RolePermissionPage";
import { AuditLogPage } from "./pages/admin/AuditLogPage";
import { ChangePasswordPage } from "./pages/ChangePasswordPage";
import { RequirePermissionRoute } from "./auth/RequirePermissionRoute";
import { SelfCheckPage } from "./pages/SelfCheckPage";
import { EtlCreatePage } from "./pages/EtlCreatePage";
import { EtlLogsPage } from "./pages/EtlLogsPage";
import { LoginPage } from "./pages/LoginPage";
import { PipelinesPage } from "./pages/PipelinesPage";

function AirflowManagePage() {
  const [searchParams] = useSearchParams();
  const dagId = searchParams.get("dagId");
  const src = dagId ? `/airflow/dags/${encodeURIComponent(dagId)}` : "/airflow/";

  return (
    <ConsoleFramePage
      title="AirFlow 관리"
      src={src}
      healthcheckSrc="/airflow/"
      waitMessage="AirFlow 관리 콘솔을 준비하는 중입니다"
    />
  );
}

export function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route element={<RequireAuth />}>
        <Route element={<AppLayout />}>
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/settings" element={<SettingsPage />} />
        <Route path="/users" element={<Navigate to="/admin/users" replace />} />
        <Route path="/permissions" element={<Navigate to="/admin/roles" replace />} />
        <Route path="/change-password" element={<ChangePasswordPage />} />
        <Route element={<RequirePermissionRoute system="ADMIN" action="READ" />}>
          <Route path="/admin/users" element={<AccountManagementPage />} />
          <Route path="/admin/roles" element={<RolePermissionPage />} />
          <Route path="/admin/assign" element={<Navigate to="/admin/users" replace />} />
          <Route path="/admin/audit" element={<AuditLogPage />} />
        </Route>
        <Route path="/self-check" element={<SelfCheckPage />} />
        <Route path="/airflow" element={<Navigate to="/airflow/dashboard" replace />} />
        <Route path="/airflow/dashboard" element={<AirflowDashboardPage />} />
        <Route
          path="/airflow/manage"
          element={<AirflowManagePage />}
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
        <Route path="/cdc/create" element={<CdcCreatePage />} />
        <Route path="/cdc/pipelines" element={<PipelinesPage />} />
        <Route path="/settings/connections" element={<ConnectionsPage />} />
        <Route path="/cdc/connections" element={<Navigate to="/settings/connections" replace />} />
        <Route path="/cdc/logs" element={<CdcLogsPage />} />
        <Route path="/connections" element={<Navigate to="/settings/connections" replace />} />
        <Route path="/pipelines" element={<Navigate to="/cdc/pipelines" replace />} />
        </Route>
      </Route>
    </Routes>
  );
}
