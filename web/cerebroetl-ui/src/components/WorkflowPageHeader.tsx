import type { ReactNode } from "react";
import { NavLink } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import type { AccessAction, SystemCode } from "../api/authz";

export type WfIconName =
  | "calendar" | "pulse" | "gear" | "clock" | "sync" | "plus" | "search" | "folder" | "file"
  | "filter" | "grid" | "play" | "check" | "x" | "flow";

/** 워크플로우 화면들의 선 아이콘. 디자인 초안(workflow-*.html)의 24px 격자 아이콘을 옮겼다. */
const ICON_SHAPES: Record<WfIconName, ReactNode> = {
  calendar: <><rect x="3" y="4" width="18" height="18" rx="2" ry="2" /><line x1="16" y1="2" x2="16" y2="6" /><line x1="8" y1="2" x2="8" y2="6" /><line x1="3" y1="10" x2="21" y2="10" /></>,
  pulse: <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />,
  gear: <><circle cx="12" cy="12" r="3" /><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" /></>,
  clock: <><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></>,
  sync: <><polyline points="23 4 23 10 17 10" /><polyline points="1 20 1 14 7 14" /><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" /></>,
  plus: <><line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" /></>,
  search: <><circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" /></>,
  folder: <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z" />,
  file: <><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /></>,
  filter: <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" />,
  grid: <><rect x="3" y="3" width="7" height="7" rx="1" /><rect x="14" y="3" width="7" height="7" rx="1" /><rect x="14" y="14" width="7" height="7" rx="1" /><rect x="3" y="14" width="7" height="7" rx="1" /></>,
  play: <polygon points="5 3 19 12 5 21 5 3" fill="currentColor" />,
  check: <polyline points="20 6 9 17 4 12" />,
  x: <><line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" /></>,
  flow: <><rect x="3" y="3" width="6" height="6" rx="1" /><rect x="15" y="15" width="6" height="6" rx="1" /><path d="M6 9v3a3 3 0 0 0 3 3h6" /></>,
};

export function WfIcon({
  name,
  size = 14,
  strokeWidth = 2,
  className,
}: {
  name: WfIconName;
  size?: number;
  strokeWidth?: number;
  className?: string;
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
      {ICON_SHAPES[name]}
    </svg>
  );
}

/**
 * 워크플로우 하위 화면 탭. 권한 규칙은 상단 메뉴(AppLayout NAV_ITEMS 의 «워크플로우» 하위)와
 * 같게 둔다 - 메뉴에서 안 보이는 화면이 탭에서만 보이면 눌러도 403 이다.
 */
const WORKFLOW_TABS: Array<{
  path: string;
  label: string;
  icon: WfIconName;
  system: SystemCode;
  action: AccessAction;
}> = [
  { path: "/workflows/design", label: "스케줄링", icon: "clock", system: "NIFI", action: "READ" },
  { path: "/airflow/dashboard", label: "실시간 모니터링", icon: "pulse", system: "AIRFLOW", action: "READ" },
  { path: "/airflow/manage", label: "관리", icon: "gear", system: "AIRFLOW", action: "READ" },
];

/**
 * 워크플로우 화면 공통 머리줄 — 디자인 초안의 «화면 제목 + 하위 탭 ··· 오른쪽 조작».
 * 사이드바를 상단 메뉴로 올리면서 «지금 어느 화면인지»를 이 제목과 탭이 다시 말해 준다.
 */
export function WorkflowPageHeader({
  title,
  icon,
  actions,
}: {
  title: string;
  icon: ReactNode;
  actions?: ReactNode;
}) {
  const { can, permissionsLoaded } = useAuth();
  // 권한이 아직 안 실렸으면(fail-open) 상단 메뉴와 같이 전부 보여준다.
  const tabs = WORKFLOW_TABS.filter((tab) => !permissionsLoaded || can(tab.system, tab.action));
  return (
    <header className="workflow-page-header">
      <div className="workflow-header-left">
        <h1 className="workflow-page-title">
          {icon}
          {title}
        </h1>
        <nav className="workflow-sub-tabs" aria-label="워크플로우 화면">
          {tabs.map((tab) => (
            // end: 스케줄링(/workflows/design)을 캔버스(/workflows/design/:id)에서도 활성으로 치지 않는다.
            <NavLink key={tab.path} to={tab.path} end className="workflow-sub-tab">
              <WfIcon name={tab.icon} size={14} />
              {tab.label}
            </NavLink>
          ))}
        </nav>
      </div>
      {actions ? <div className="workflow-header-actions">{actions}</div> : null}
    </header>
  );
}
