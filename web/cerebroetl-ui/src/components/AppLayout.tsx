import { useQuery } from "@tanstack/react-query";
import { getAlertHistory } from "../api/alerts";
import { Layout, Tooltip } from "antd";
import { Link, NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import { useState, type ReactNode } from "react";
import { useAuth } from "../auth/AuthContext";
import { confirmLeave } from "../utils/navigationGuard";
import type { AccessAction, SystemCode } from "../api/authz";

const { Header, Content } = Layout;

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

/** 선 아이콘. 디자인 초안(CEREBRO ETL_UI DESIGN/index.html) 상단 메뉴의 24px 격자·2px 선 아이콘을 옮겼다. */
function LineIcon({
  className,
  strokeWidth = 2,
  size,
  children,
}: {
  className?: string;
  strokeWidth?: number;
  size?: number;
  children: ReactNode;
}) {
  return (
    <svg
      className={className}
      xmlns="http://www.w3.org/2000/svg"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={strokeWidth}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {children}
    </svg>
  );
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
      <button
        type="button"
        className="header-alert-btn"
        aria-label="알림"
        onClick={() => navigate("/settings?tab=history")}
      >
        <LineIcon size={18}>
          <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" />
          <path d="M13.73 21a2 2 0 0 1-3.46 0" />
        </LineIcon>
        {/* antd Badge 를 쓰던 때와 같이 0 이면 숨기고 99 를 넘으면 99+ 로 줄인다. */}
        {unacked > 0 ? <span className="alert-badge">{unacked > 99 ? "99+" : unacked}</span> : null}
      </button>
    </Tooltip>
  );
}

// 각 메뉴에 시스템/요구권한을 붙여 역할에 따라 보이거나 감춘다(설계서 §4.5). 그룹은 노출
// 가능한 자식이 하나라도 있으면 보인다. 권한 로드 전(fail-open)에는 전부 보여준다.
const NAV_ITEMS: NavItem[] = [
  {
    path: "/dashboard",
    label: "대시보드",
    icon: (
      <LineIcon className="menu-icon">
        <path d="M3 13a9 9 0 1 0 18 0" />
        <path d="M12 13l3 -3" />
        <circle cx="12" cy="13" r="1" />
      </LineIcon>
    ),
    system: "COMMON",
    action: "READ",
  },
  {
    path: "/airflow",
    // 메뉴는 기술명(NiFi/Kafka/Airflow)이 아니라 하는 일로 부른다(ETL·CDC 와 같은 규칙).
    // "워크플로우" 는 ETL 워크플로우 설계서의 도메인 용어(etl_workflow)와 맞춘 것이다.
    label: "워크플로우",
    icon: (
      <LineIcon className="menu-icon">
        <rect x="3" y="3" width="6" height="6" rx="1" />
        <rect x="15" y="15" width="6" height="6" rx="1" />
        <path d="M6 9v3a3 3 0 0 0 3 3h6" />
      </LineIcon>
    ),
    children: [
      { path: "/workflows/design", label: "스케줄링", system: "NIFI", action: "READ" },
      { path: "/airflow/dashboard", label: "실시간 모니터링", system: "AIRFLOW", action: "READ" },
      { path: "/airflow/manage", label: "관리", system: "AIRFLOW", action: "READ" },
    ],
  },
  {
    path: "/etl/manage",
    label: "ETL",
    icon: (
      <LineIcon className="menu-icon">
        <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" />
      </LineIcon>
    ),
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
    icon: (
      <LineIcon className="menu-icon">
        <ellipse cx="12" cy="5" rx="9" ry="3" />
        <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
        <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
      </LineIcon>
    ),
    children: [
      { path: "/cdc/create", label: "생성", system: "KAFKA", action: "WRITE" },
      { path: "/cdc/pipelines", label: "관리", system: "KAFKA", action: "READ" },
      { path: "/cdc/logs", label: "로그", system: "KAFKA", action: "READ" },
    ],
  },
  {
    path: "/settings",
    label: "설정",
    icon: (
      <LineIcon className="menu-icon">
        <circle cx="12" cy="12" r="3" />
        <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
      </LineIcon>
    ),
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
  // 지금 펼쳐진 상단 드롭다운(그룹 path). 마우스를 올리거나 키보드로 들어오면 열리고, 벗어나거나
  // 하위 메뉴를 고르면 닫힌다. CSS :hover 만으로 열면 메뉴를 고른 뒤에도 커서가 그 자리에 있는 한
  // 목록이 계속 떠서 방금 연 화면을 가린다.
  const [openMenu, setOpenMenu] = useState<string | null>(null);

  const closeMenu = (path: string) => {
    setOpenMenu((current) => (current === path ? null : current));
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

  // 디자인 초안처럼 왼쪽 사이드바를 걷어내고 메뉴를 상단 한 줄(GNB)로 올렸다.
  // 메뉴 구성·권한 게이팅·이탈 확인은 사이드바 때와 같다 - 바뀐 것은 배치뿐이다.
  return (
    <Layout className="app-shell">
      <Header className="app-header">
        <div className="navbar-left">
          <Link to="/dashboard" className="navbar-logo"
                onClick={(event) => guardedNavigate(event, "/dashboard")}>
            CEREBRO <span className="accent">ETL</span>
            <span className="company-tag">(주)데이터월드</span>
          </Link>
          <nav aria-label="주요 메뉴">
            <ul className="navbar-menu">
              {visibleItems.map((item) => {
                const groupPrefix = `/${item.path.split("/")[1]}`;
                const children = itemChildren(item);
                const active =
                  location.pathname === item.path ||
                  location.pathname.startsWith(`${groupPrefix}/`) ||
                  children.some(
                    (c) => location.pathname === c.path || location.pathname.startsWith(`${c.path}/`),
                  );

                if (!item.children) {
                  return (
                    <li key={item.path} className={active ? "menu-tab active" : "menu-tab"}>
                      <NavLink to={item.path} className="menu-link"
                               onClick={(event) => guardedNavigate(event, item.path)}>
                        {item.icon}
                        {item.label}
                      </NavLink>
                    </li>
                  );
                }

                const open = openMenu === item.path;
                return (
                  <li
                    key={item.path}
                    className={`menu-tab${active ? " active" : ""}${open ? " open" : ""}`}
                    onMouseEnter={() => setOpenMenu(item.path)}
                    onMouseLeave={() => closeMenu(item.path)}
                    onBlur={(event) => {
                      // 포커스가 이 그룹 밖으로 나갈 때만 닫는다(그룹 안 하위 메뉴로 옮겨 가는 건 유지).
                      if (!event.currentTarget.contains(event.relatedTarget)) {
                        closeMenu(item.path);
                      }
                    }}
                    onKeyDown={(event) => {
                      if (event.key === "Escape") {
                        closeMenu(item.path);
                      }
                    }}
                  >
                    {/* 그룹 이름은 이동하지 않는다(초안과 같음). 누르면 목록만 연다 - 터치·키보드용. */}
                    <button
                      type="button"
                      className="menu-link"
                      aria-haspopup="true"
                      aria-expanded={open}
                      onClick={() => setOpenMenu(item.path)}
                    >
                      {item.icon}
                      {item.label}
                      <LineIcon className="chevron-icon" strokeWidth={2.5}>
                        <polyline points="6 9 12 15 18 9" />
                      </LineIcon>
                    </button>
                    <ul className="dropdown-menu">
                      {children.map((child) => (
                        <li key={child.path} className="dropdown-item">
                          <NavLink
                            to={child.path}
                            // end: 하위 경로까지 활성으로 치지 않는다. "알림/발송 관리"(/settings)는
                            // "연결정보"(/settings/connections)의 접두어라, 이게 없으면 연결정보를
                            // 열었을 때 둘 다 빨갛게 표시된다. className 이 문자열이면 react-router 가
                            // 자체 판정으로 "active" 를 덧붙이므로 여기서 따로 계산하지 않는다.
                            end
                            onClick={(event) => {
                              setOpenMenu(null);
                              guardedNavigate(event, child.path);
                            }}
                          >
                            {child.label}
                          </NavLink>
                        </li>
                      ))}
                    </ul>
                  </li>
                );
              })}
            </ul>
          </nav>
        </div>
        <div className="navbar-right">
          <AlertBell />
          {user && (
            <span className="user-info-chip">
              {user.userNm}
              {user.admin ? <span className="user-role-badge"> (관리자)</span> : null}
            </span>
          )}
          <button
            type="button"
            className={permissions?.pwMustChange ? "btn-pw-warn" : "btn-header-outline"}
            onClick={() => navigate("/change-password")}
          >
            {permissions?.pwMustChange ? "비밀번호 변경 필요" : "비밀번호 변경"}
          </button>
          <button type="button" className="btn-header-outline btn-logout" onClick={logout}>
            <LineIcon size={14}>
              <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
              <polyline points="16 17 21 12 16 7" />
              <line x1="21" y1="12" x2="9" y2="12" />
            </LineIcon>
            로그아웃
          </button>
        </div>
      </Header>
      <Content className="app-content">
        <Outlet />
      </Content>
    </Layout>
  );
}
