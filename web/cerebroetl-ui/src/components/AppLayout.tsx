import { Layout } from "antd";
import {
  ApartmentOutlined,
  DashboardOutlined,
  DeploymentUnitOutlined,
  NodeIndexOutlined,
} from "@ant-design/icons";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import type { ReactNode } from "react";

const { Header, Sider, Content } = Layout;

interface NavItem {
  path: string;
  label: string;
  icon: ReactNode;
  children?: Array<{ path: string; label: string; external?: boolean }>;
}

const NAV_ITEMS: NavItem[] = [
  { path: "/dashboard", label: "대시보드", icon: <DashboardOutlined /> },
  {
    path: "/airflow",
    label: "AirFlow",
    icon: <DeploymentUnitOutlined />,
    children: [{ path: "http://localhost:8090", label: "생성/관리", external: true }],
  },
  {
    path: "/etl/manage",
    label: "ETL",
    icon: <ApartmentOutlined />,
    children: [
      { path: "/etl/manage", label: "생성/관리" },
      { path: "/etl/logs", label: "로그" },
    ],
  },
  {
    path: "/cdc/kafka-connect",
    label: "CDC",
    icon: <NodeIndexOutlined />,
    children: [
      { path: "/cdc/kafka-connect", label: "Kafka Connect" },
      { path: "/cdc/pipelines", label: "파이프라인" },
      { path: "/cdc/connections", label: "연결정보" },
    ],
  },
];

export function AppLayout() {
  const location = useLocation();

  return (
    <Layout className="app-shell">
      <Header className="app-header">
        <div className="brand-mark">dw</div>
        <div className="brand-divider" />
        <div className="brand-title">Data Pipeline Console</div>
      </Header>
      <Layout>
        <Sider width={270} theme="light" className="app-sidebar">
          <div className="sidebar-section-kicker">DATA PLATFORM</div>
          <div className="sidebar-title">메뉴</div>
          <nav className="sidebar-nav" aria-label="주요 메뉴">
            {NAV_ITEMS.map((item) => {
              const groupPrefix = `/${item.path.split("/")[1]}`;
              const active = location.pathname === item.path || location.pathname.startsWith(`${groupPrefix}/`);
              return (
                <div key={item.path} className="sidebar-group">
                  {item.children ? (
                    <div className={active ? "sidebar-link sidebar-link-static active" : "sidebar-link sidebar-link-static"}>
                      <span className="sidebar-link-icon">{item.icon}</span>
                      <span>{item.label}</span>
                    </div>
                  ) : (
                    <NavLink to={item.path} className={active ? "sidebar-link active" : "sidebar-link"}>
                      <span className="sidebar-link-icon">{item.icon}</span>
                      <span>{item.label}</span>
                    </NavLink>
                  )}
                  {item.children ? (
                    <div className="sidebar-subnav">
                      {item.children.map((child) =>
                        child.external ? (
                          <a key={child.path} href={child.path} target="_blank" rel="noreferrer" className="sidebar-sublink">
                            {child.label}
                          </a>
                        ) : (
                          <NavLink
                            key={child.path}
                            to={child.path}
                            className={location.pathname === child.path ? "sidebar-sublink active" : "sidebar-sublink"}
                          >
                            {child.label}
                          </NavLink>
                        ),
                      )}
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
