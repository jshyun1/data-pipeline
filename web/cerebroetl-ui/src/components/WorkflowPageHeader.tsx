import type { ReactNode } from "react";
import type { AccessAction, SystemCode } from "../api/authz";

export type WfIconName =
  | "calendar" | "pulse" | "gear" | "clock" | "sync" | "plus" | "search" | "folder" | "file"
  | "filter" | "grid" | "play" | "check" | "x" | "flow"
  | "database" | "bell" | "link" | "user" | "shield" | "doc" | "bot" | "list" | "info";

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
  // 아래는 CDC · 설정 · GPT 화면 머리줄 아이콘(디자인 작업물 cdc-*.html · *-setting.html 과 같은 모양).
  database: <><ellipse cx="12" cy="5" rx="9" ry="3" /><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" /><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" /></>,
  bell: <><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" /><path d="M13.73 21a2 2 0 0 1-3.46 0" /></>,
  link: <><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" /><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" /></>,
  user: <><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" /><circle cx="12" cy="7" r="4" /></>,
  shield: <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />,
  doc: <><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /><line x1="16" y1="13" x2="8" y2="13" /><line x1="16" y1="17" x2="8" y2="17" /></>,
  bot: <><rect x="4" y="7" width="16" height="12" rx="2" /><path d="M12 3v4" /><circle cx="9" cy="13" r="1" /><circle cx="15" cy="13" r="1" /><path d="M9 16.5h6" /></>,
  list: <><line x1="8" y1="6" x2="21" y2="6" /><line x1="8" y1="12" x2="21" y2="12" /><line x1="8" y1="18" x2="21" y2="18" /><line x1="3" y1="6" x2="3.01" y2="6" /><line x1="3" y1="12" x2="3.01" y2="12" /><line x1="3" y1="18" x2="3.01" y2="18" /></>,
  info: <><circle cx="12" cy="12" r="10" /><line x1="12" y1="16" x2="12" y2="12" /><line x1="12" y1="8" x2="12.01" y2="8" /></>,
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

interface MenuTab {
  path: string;
  label: string;
  icon: WfIconName;
  system?: SystemCode;
  /** 여러 시스템 중 하나라도 권한이 있으면 보인다(상단 메뉴 NavLeaf.anySystems 와 같은 규칙). */
  anySystems?: SystemCode[];
  action: AccessAction;
}

/**
 * 메뉴별 하위 화면 탭. 이름 · 순서 · 권한 규칙은 상단 메뉴(AppLayout NAV_ITEMS)와 같게 둔다 -
 * 메뉴에서 안 보이는 화면이 탭에서만 보이면 눌러도 403 이다. 메뉴를 고치면 여기도 함께 고친다.
 */
const MENU_TABS = {
  workflow: [
    { path: "/workflows/create", label: "생성", icon: "plus", system: "NIFI", action: "WRITE" },
    { path: "/workflows/design", label: "관리", icon: "clock", system: "NIFI", action: "READ" },
    { path: "/airflow/dashboard", label: "로그", icon: "pulse", system: "AIRFLOW", action: "READ" },
    { path: "/airflow/manage", label: "상세 관리", icon: "gear", system: "AIRFLOW", action: "READ" },
  ],
  cdc: [
    { path: "/cdc/create", label: "생성", icon: "plus", system: "KAFKA", action: "WRITE" },
    { path: "/cdc/pipelines", label: "관리", icon: "gear", system: "KAFKA", action: "READ" },
    { path: "/cdc/logs", label: "로그", icon: "doc", system: "KAFKA", action: "READ" },
  ],
  gpt: [
    { path: "/gpt/register", label: "등록", icon: "plus", system: "ADMIN", action: "WRITE" },
    { path: "/gpt/manage", label: "관리", icon: "gear", system: "ADMIN", action: "READ" },
  ],
  settings: [
    { path: "/settings", label: "알림/발송", icon: "bell", system: "ADMIN", action: "READ" },
    { path: "/settings/connections", label: "연결정보", icon: "link", anySystems: ["KAFKA", "NIFI"], action: "READ" },
    { path: "/admin/users", label: "계정", icon: "user", system: "ADMIN", action: "READ" },
    { path: "/admin/roles", label: "역할 및 권한", icon: "shield", system: "ADMIN", action: "READ" },
    { path: "/admin/audit", label: "감사", icon: "doc", system: "ADMIN", action: "READ" },
  ],
} satisfies Record<string, MenuTab[]>;

export type MenuKey = keyof typeof MENU_TABS;

/**
 * 화면 공통 머리줄 — 오른쪽 조작만 남는다(제목 · 하위 탭은 2026-09-17 제거).
 * MENU_TABS 는 메뉴 키(MenuKey)를 정하는 데만 쓴다 - 탭을 다시 보이게 하려면 이 표를 그대로 쓰면 된다.
 */
export function MenuPageHeader({
  menu,
  title,
  icon,
  actions,
}: {
  menu: MenuKey;
  title: string;
  icon: ReactNode;
  actions?: ReactNode;
}) {
  // 화면 제목과 같은 메뉴의 하위 탭 줄은 뺐다(2026-09-17 사용자 요청) - 상단 메뉴가 이미 같은 내용을 보여 준다.
  // 오른쪽 조작(동기화 · 새 워크플로우 등)은 기능이라 남긴다. 조작이 없는 화면은 머리줄을 그리지 않는다.
  // menu · title · icon 은 호출부 호환과 화면 낭독기용 이름으로만 쓴다.
  void menu;
  void icon;
  if (!actions) return null;
  return (
    <header className="workflow-page-header workflow-page-header--actions-only" aria-label={title}>
      <div className="workflow-header-actions">{actions}</div>
    </header>
  );
}

/** 워크플로우 화면 머리줄(기존 호출부 호환). */
export function WorkflowPageHeader(props: { title: string; icon: ReactNode; actions?: ReactNode }) {
  return <MenuPageHeader menu="workflow" {...props} />;
}
