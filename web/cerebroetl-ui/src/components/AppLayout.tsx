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
import { confirmLeave } from "../utils/navigationGuard";
import type { AccessAction, SystemCode } from "../api/authz";

const { Header, Sider, Content } = Layout;

interface NavLeaf {
  path: string;
  label: string;
  system?: SystemCode;
  action?: AccessAction;
  /**
   * 여러 시스템이 공유하는 화면. 나열한 시스템 중 **하나라도** 권한이 있으면 보인다.
   * 예: 연결정보는 CDC(KAFKA)와 ETL(NIFI) 양쪽이 쓰는 공용 정보라 한쪽만 있어도 필요하다.
   * system 과 함께 쓰지 않는다(anySystems 가 있으면 그쪽이 우선).
   */
  anySystems?: SystemCode[];
}

interface NavItem {
  path: string;
  label: string;
  icon: ReactNode;
  system?: SystemCode;
  action?: AccessAction;
  children?: NavLeaf[];
  /**
   * 그룹 자체의 최소 요건. 기본 규칙("자식이 하나라도 보이면 그룹도 보임")만으로는
   * 그룹을 숨길 수 없는 경우에 쓴다 - 지정하면 이 권한이 **먼저** 검사되고,
   * 통과하지 못하면 보이는 자식이 있어도 그룹을 숨긴다.
   */
  requireAny?: SystemCode[];
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

// 각 메뉴에 시스템/요구권한을 붙여 역할에 따라 보이거나 감춘다(설계서 §4.5). 그룹은 노출
// 가능한 자식이 하나라도 있으면 보인다. 권한 로드 전(fail-open)에는 전부 보여준다.
const NAV_ITEMS: NavItem[] = [
  { path: "/dashboard", label: "대시보드", icon: <DashboardOutlined />, system: "COMMON", action: "READ" },
  {
    path: "/airflow",
    // 메뉴는 기술명(NiFi/Kafka/Airflow)이 아니라 하는 일로 부른다(ETL·CDC 와 같은 규칙).
    // "워크플로우" 는 ETL 워크플로우 설계서의 도메인 용어(etl_workflow)와 맞춘 것이다.
    label: "워크플로우",
    icon: <DeploymentUnitOutlined />,
    children: [
      { path: "/workflows/design", label: "스케줄링", system: "NIFI", action: "READ" },
      { path: "/airflow/dashboard", label: "실시간 모니터링", system: "AIRFLOW", action: "READ" },
      { path: "/airflow/manage", label: "관리", system: "AIRFLOW", action: "READ" },
    ],
  },
  {
    path: "/etl/manage",
    label: "ETL",
    icon: <ApartmentOutlined />,
    children: [
      { path: "/etl/create", label: "생성", system: "NIFI", action: "WRITE" },
      { path: "/etl/manage", label: "관리", system: "NIFI", action: "READ" },
      { path: "/etl/settings", label: "관리 설정", system: "NIFI", action: "READ" },
      { path: "/etl/logs", label: "로그", system: "NIFI", action: "READ" },
    ],
  },
  {
    path: "/cdc/pipelines",
    label: "CDC",
    icon: <NodeIndexOutlined />,
    children: [
      { path: "/cdc/create", label: "생성", system: "KAFKA", action: "WRITE" },
      { path: "/cdc/pipelines", label: "관리", system: "KAFKA", action: "READ" },
      { path: "/cdc/logs", label: "로그", system: "KAFKA", action: "READ" },
    ],
  },
  {
    path: "/settings",
    label: "설정",
    icon: <SettingOutlined />,
    children: [
      // 알림/발송 관리의 API(AlertAdminController·NotificationAdminController)는 ADMIN 이다.
      // 여기가 COMMON 이면 모든 역할이 COMMON=1 을 갖고 있어 «설정» 그룹이 절대 안 숨겨지고,
      // 조회자가 눌러도 API 가 403 을 주는 불일치가 생긴다.
      { path: "/settings", label: "알림/발송 관리", system: "ADMIN", action: "READ" },
      // 연결정보는 CDC(KAFKA)와 ETL(NIFI)이 함께 쓰는 공용 정보라 한쪽 권한만 있어도 필요하다.
      { path: "/settings/connections", label: "연결정보", anySystems: ["KAFKA", "NIFI"], action: "READ" },
      { path: "/admin/users", label: "계정 관리", system: "ADMIN", action: "READ" },
      { path: "/admin/roles", label: "역할 및 권한", system: "ADMIN", action: "READ" },
      { path: "/admin/audit", label: "감사 로그", system: "ADMIN", action: "READ" },
    ],
  },
];

export function AppLayout() {
  const location = useLocation();
  const navigate = useNavigate();
  const { user, logout, permissions, permissionsLoaded, can } = useAuth();
  const [openGroups, setOpenGroups] = useState<Record<string, boolean>>({});
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  const toggleGroup = (path: string) => {
    setOpenGroups((previous) => ({ ...previous, [path]: !previous[path] }));
  };

  /**
   * 메뉴 이동. «저장하지 않은 변경»이 있는 화면(워크플로우 캔버스)이 가드를 걸어 두면
   * 먼저 물어보고, 취소하면 이동하지 않는다. 걸린 가드가 없으면 평소와 똑같이 동작한다.
   *
   * <p>새 탭으로 열기(Ctrl/Cmd·가운데 클릭)는 이 화면을 떠나지 않으므로 가로채지 않는다.
   */
  const guardedNavigate = (
    event: React.MouseEvent<HTMLAnchorElement>,
    path: string,
  ) => {
    if (event.defaultPrevented || event.metaKey || event.ctrlKey || event.shiftKey || event.button !== 0) {
      return;
    }
    event.preventDefault();
    void confirmLeave().then((ok) => {
      if (ok) {
        navigate(path);
      }
    });
  };

  // 권한이 아직 안 실렸으면(fail-open) 전부 보여준다. 실린 뒤엔 시스템 권한으로 게이팅.
  const leafVisible = (leaf: NavLeaf) => {
    if (!permissionsLoaded) {
      return true;
    }
    const action = leaf.action ?? "READ";
    if (leaf.anySystems?.length) {
      return leaf.anySystems.some((system) => can(system, action));
    }
    return !leaf.system || can(leaf.system, action);
  };
  const itemChildren = (item: NavItem) => (item.children ?? []).filter(leafVisible);
  const itemVisible = (item: NavItem) => {
    if (!permissionsLoaded) {
      return true;
    }
    // 그룹 자체 요건이 있으면 먼저 본다 - 보이는 자식이 있어도 여기서 막힌다.
    if (item.requireAny?.length && !item.requireAny.some((system) => can(system, "READ"))) {
      return false;
    }
    if (item.children) {
      return itemChildren(item).length > 0;
    }
    return !item.system || can(item.system, item.action ?? "READ");
  };

  const visibleItems = NAV_ITEMS.filter(itemVisible);

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
        <Link to="/dashboard" className="brand-link"
              onClick={(event) => guardedNavigate(event, "/dashboard")}>
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
          <Button
            type={permissions?.pwMustChange ? "primary" : "text"}
            size="small"
            danger={permissions?.pwMustChange}
            onClick={() => navigate("/change-password")}
          >
            {permissions?.pwMustChange ? "비밀번호 변경 필요" : "비밀번호 변경"}
          </Button>
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
            {visibleItems.map((item) => {
              const groupPrefix = `/${item.path.split("/")[1]}`;
              const children = itemChildren(item);
              const active =
                location.pathname === item.path ||
                location.pathname.startsWith(`${groupPrefix}/`) ||
                children.some(
                  (c) => location.pathname === c.path || location.pathname.startsWith(`${c.path}/`),
                );
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
                    <NavLink to={item.path} className={active ? "sidebar-link active" : "sidebar-link"}
                             onClick={(event) => guardedNavigate(event, item.path)}>
                      <span className="sidebar-link-icon">{item.icon}</span>
                      <span>{item.label}</span>
                    </NavLink>
                  )}
                  {item.children && expanded ? (
                    <div className="sidebar-subnav">
                      {children.map((child) => (
                        <NavLink
                          key={child.path}
                          to={child.path}
                          // end: 하위 경로까지 활성으로 치지 않는다. "알림/발송 관리"(/settings)는
                          // "연결정보"(/settings/connections)의 접두어라, 이게 없으면 연결정보를
                          // 열었을 때 둘 다 빨갛게 표시된다. className 이 문자열이면 react-router 가
                          // 자체 판정으로 "active" 를 덧붙이므로 여기서 따로 계산하지 않는다.
                          end
                          className="sidebar-sublink"
                          onClick={(event) => guardedNavigate(event, child.path)}
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
