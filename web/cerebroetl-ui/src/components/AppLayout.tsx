import { useQuery } from "@tanstack/react-query";
import { getAlertHistory } from "../api/alerts";
import { Button, Layout, Badge, Tooltip } from "antd";
import {
  ApartmentOutlined,
  BellOutlined,
  DashboardOutlined,
  DeploymentUnitOutlined,
  LogoutOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  NodeIndexOutlined,
  SettingOutlined,
} from "@ant-design/icons";
import { Link, NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import { useState, type ReactNode } from "react";
import { useAuth } from "../auth/AuthContext";

const { Header, Sider, Content } = Layout;

interface NavItem {
  path: string;
  label: string;
  icon: ReactNode;
  children?: Array<{ path: string; label: string }>;
}

/**
 * 헤더 종 아이콘 — 원본 5-7 "상단 헤더 종 아이콘 + 전용 화면".
 * 미확인 건수를 배지로 띄워 다른 화면을 보고 있어도 새 알림을 놓치지 않게 한다.
 */
function AlertBell() {
  const navigate = useNavigate();
  const { data } = useQuery({
    queryKey: ["alert-history", "unacked", "bell"],
    queryFn: () => getAlertHistory({ filter: "unacked", days: 7, page: 0, pageSize: 1 }),
    refetchInterval: 30000,
    placeholderData: (prev) => prev,
  });
  const unacked = data?.counts?.unacked ?? 0;
  return (
    <Tooltip title={unacked > 0 ? `미확인 알림 ${unacked}건` : "미확인 알림 없음"}>
      <Badge count={unacked} size="small" offset={[-2, 2]}>
        <Button
          type="text"
          size="small"
          icon={<BellOutlined />}
          aria-label="알림"
          onClick={() => navigate("/settings?tab=history")}
        />
      </Badge>
    </Tooltip>
  );
}

const NAV_ITEMS: NavItem[] = [
  { path: "/dashboard", label: "대시보드", icon: <DashboardOutlined /> },
  {
    path: "/airflow",
    label: "AirFlow",
    icon: <DeploymentUnitOutlined />,
    children: [
      { path: "/airflow/manage", label: "생성/관리" },
      { path: "/airflow/dashboard", label: "대시보드" },
    ],
  },
  {
    path: "/etl/manage",
    label: "ETL",
    icon: <ApartmentOutlined />,
    children: [
      { path: "/etl/create", label: "생성" },
      { path: "/etl/manage", label: "관리" },
      { path: "/etl/logs", label: "로그" },
    ],
  },
  {
    path: "/cdc/pipelines",
    label: "CDC",
    icon: <NodeIndexOutlined />,
    children: [
      { path: "/cdc/pipelines", label: "파이프라인" },
      { path: "/cdc/connections", label: "연결정보" },
      { path: "/cdc/logs", label: "처리 로그" },
    ],
  },
  {
    path: "/settings",
    label: "설정",
    icon: <SettingOutlined />,
    children: [
      { path: "/settings", label: "알림/발송 관리" },
      { path: "/users", label: "사용자 관리" },
      { path: "/permissions", label: "권한 관리" },
    ],
  },
];

export function AppLayout() {
  const location = useLocation();
  const { user, logout } = useAuth();
  const [openGroups, setOpenGroups] = useState<Record<string, boolean>>({});
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  const toggleGroup = (path: string) => {
    setOpenGroups((previous) => ({ ...previous, [path]: !previous[path] }));
  };

  return (
    <Layout className="app-shell">
      <Header className="app-header">
        <Button
          type="text"
          className="sidebar-toggle"
          icon={sidebarCollapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
          aria-label={sidebarCollapsed ? "메뉴 펼치기" : "메뉴 숨기기"}
          aria-expanded={!sidebarCollapsed}
          aria-controls="primary-sidebar"
          onClick={() => setSidebarCollapsed((previous) => !previous)}
        />
        <Link to="/dashboard" className="brand-link">
          <img src="/logo.svg" alt="데이터월드" className="brand-logo" />
          <div className="brand-divider" />
          <div className="brand-title">Cerebro ETL</div>
        </Link>
        <div className="header-account">
          <AlertBell />
          {user && (
            <span className="header-user">
              {user.userNm}
              {user.admin ? " (관리자)" : ""}
            </span>
          )}
          <Button type="text" size="small" icon={<LogoutOutlined />} onClick={logout}>
            로그아웃
          </Button>
        </div>
      </Header>
      <Layout className={sidebarCollapsed ? "app-body sidebar-is-collapsed" : "app-body"}>
        <Sider
          id="primary-sidebar"
          width={270}
          collapsedWidth={0}
          collapsed={sidebarCollapsed}
          collapsible
          trigger={null}
          theme="light"
          className="app-sidebar"
        >
          <nav className="sidebar-nav" aria-label="주요 메뉴">
            {NAV_ITEMS.map((item) => {
              const groupPrefix = `/${item.path.split("/")[1]}`;
              const active =
                location.pathname === item.path ||
                location.pathname.startsWith(`${groupPrefix}/`) ||
                (item.children?.some(
                  (c) => location.pathname === c.path || location.pathname.startsWith(`${c.path}/`),
                ) ??
                  false);
              const expanded = item.children ? Boolean(openGroups[item.path]) || active : false;
              return (
                <div key={item.path} className="sidebar-group">
                  {item.children ? (
                    <button
                      type="button"
                      className={active ? "sidebar-link sidebar-link-button active" : "sidebar-link sidebar-link-button"}
                      aria-expanded={expanded}
                      onClick={() => toggleGroup(item.path)}
                    >
                      <span className="sidebar-link-icon">{item.icon}</span>
                      <span>{item.label}</span>
                    </button>
                  ) : (
                    <NavLink to={item.path} className={active ? "sidebar-link active" : "sidebar-link"}>
                      <span className="sidebar-link-icon">{item.icon}</span>
                      <span>{item.label}</span>
                    </NavLink>
                  )}
                  {item.children && expanded ? (
                    <div className="sidebar-subnav">
                      {item.children.map((child) => (
                        <NavLink
                          key={child.path}
                          to={child.path}
                          className={location.pathname === child.path ? "sidebar-sublink active" : "sidebar-sublink"}
                        >
                          {child.label}
                        </NavLink>
                      ))}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </nav>
        </Sider>
        <Content className="app-content">
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}
