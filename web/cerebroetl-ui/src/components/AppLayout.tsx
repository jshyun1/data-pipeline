import { Button, Layout } from "antd";
import {
  ApartmentOutlined,
  BellOutlined,
  DashboardOutlined,
  DeploymentUnitOutlined,
  LogoutOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  NodeIndexOutlined,
} from "@ant-design/icons";
import { Link, NavLink, Outlet, useLocation } from "react-router-dom";
import { useState, type ReactNode } from "react";
import { useAuth } from "../auth/AuthContext";

const { Header, Sider, Content } = Layout;

interface NavItem {
  path: string;
  label: string;
  icon: ReactNode;
  children?: Array<{ path: string; label: string }>;
}

const NAV_ITEMS: NavItem[] = [
  { path: "/dashboard", label: "대시보드", icon: <DashboardOutlined /> },
  { path: "/alerts", label: "조치 대기열", icon: <BellOutlined /> },
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
              const active = location.pathname === item.path || location.pathname.startsWith(`${groupPrefix}/`);
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
