import { useCallback, useEffect, useMemo, useRef, useState, type ChangeEvent, type SyntheticEvent } from "react";
import { Alert, Button, Result, Spin, message } from "antd";
import { ReloadOutlined, UploadOutlined } from "@ant-design/icons";
import { useLocation, useNavigate } from "react-router-dom";
import {
  getEtlJob,
  getEtlJobRuns,
  listEtlJobs,
  syncEtlJobs,
  type EtlJobDetailResponse,
  type EtlJobLinkView,
  type EtlJobResponse,
  type EtlJobRunResponse,
  type EtlJobStepView,
} from "../api/etlJobs";
import {
  getNifiProcessor,
  getNifiProcessGroupTree,
  listNifiExecutionLogs,
  listNifiProcessorRuns,
  refreshNifiProcessGroupTree,
  acquireNifiProcessorEditLock,
  heartbeatNifiProcessorEditLock,
  releaseNifiProcessorEditLock,
  uploadNifiInputDirectoryFiles,
  type NifiExecutionLogEntry,
  type NifiProcessorEditLockResponse,
  type NifiProcessorDetailResponse,
  type NifiProcessorRun,
  type NifiProcessGroupTreeNode,
} from "../api/platform";

const HIDE_TOOL_CHROME_STYLE_ID = "cerebro-hide-tool-chrome";
const CANVAS_SELECTION_SYNC_ATTRIBUTE = "data-cerebro-canvas-selection-sync";
const CANVAS_ROUTE_SYNC_ATTRIBUTE = "data-cerebro-canvas-route-sync";
const KOREAN_TOOLTIP_PATCH_ATTRIBUTE = "data-cerebro-korean-tooltip-patch";
const KOREAN_TOOLTIP_INTERVAL_ATTRIBUTE = "data-cerebro-korean-tooltip-interval";
const NIFI_STATUS_HIDDEN_ATTRIBUTE = "data-cerebro-status-hidden";
const NIFI_STATUS_BAR_HIDDEN_ATTRIBUTE = "data-cerebro-status-bar-hidden";
const NIFI_PANEL_LAYOUT_ATTRIBUTE = "data-cerebro-nifi-panel-layout";
const NIFI_PANEL_ATTRIBUTE = "data-cerebro-nifi-panel";
const NIFI_PANEL_EXPANDED_ATTRIBUTE = "data-cerebro-nifi-panel-expanded";
const NIFI_PANEL_BOUND_ATTRIBUTE = "data-cerebro-nifi-panel-bound";
const NIFI_AUTO_OPEN_MENU_ATTRIBUTE = "data-cerebro-auto-open-menu";
const LEGACY_NIFI_MINI_CREATE_TOOLBAR_CLEANUP_ATTRIBUTE = "data-cerebro-mini-create-toolbar-cleanup";
const NIFI_PROCESSOR_LOCK_HEARTBEAT_MS = 20_000;
// NiFi/Airflow 각자의 로고를 감춰서 "따로 노는 느낌" 없이 하나의 Cerebro ETL처럼
// 보이게 한다. 번들 분석으로 실제 렌더링되는 요소를 확인한 선택자:
// - NiFi: 두 곳에 있었다.
//   1) 라우트 가드 로딩 중에만 뜨는 스플래시 오버레이(.splash/.splash-img,
//      nifi-drop-splash*.svg 배경) - 부트 스플래시.
//   2) 캔버스 상단 "navigation" 컴포넌트가 항상 그리는 진짜 툴바 로고:
//      <img ngSrc="assets/icons/nifi-logo.svg" alt="NiFi Logo"> (h-16 w-28
//      고정 크기 div로 감싸져 있음 - .context-logo는 번들 전체(125개 청크)에
//      실제로 안 쓰여서 제거, img[alt="NiFi Logo"]가 진짜 선택자).
// - Airflow: <img alt="Logo">는 커스텀 테마 아이콘을 설정했을 때만 렌더링되는
//   코드 경로라 지금 설정에선 절대 매치되지 않음. 실제로는 네브바 홈 링크에
//   인라인 SVG(viewBox="0 0 35 35", 5색 팬휠)로 항상 그려짐 - 번들 전체에서
//   이 viewBox를 쓰는 요소가 그것 하나뿐이라 안전하게 특정 가능.
// 로고 자체만 감추면 고정 크기로 감싸둔 컨테이너 껍데기가 빈 칸으로 남는다
// (사용자가 스크린샷으로 확인) - :has()로 그 요소를 직접 담고 있는 조상까지
// 같이 접어서 빈 칸 없이 나머지가 당겨 붙게 한다.
// NiFi의 상단 우측 로그인/로그아웃 링크와 사용자명도 감춘다 - 통합 포털이 SSO/계정
// 표시를 전담하므로 NiFi 자체 계정 UI는 오히려 혼란만 준다. 로그인/로그아웃 <a>는
// 고유 class가 없어서(그냥 [click] 바인딩만 있는 <a>) .current-user(사용자명,
// 진짜 class 있음)의 형제 요소라는 구조로 특정한다 - flex-col 컨테이너라
// 감춰도 빈 칸 없이 붙는다.
const HIDE_TOOL_CHROME_CSS = `
  .splash, img[alt="Logo"], img[alt="NiFi Logo"] { display: none !important; }
  svg[viewBox="0 0 35 35"] { display: none !important; }
  :has(> svg[viewBox="0 0 35 35"]) { display: none !important; }
  :has(> img[alt="NiFi Logo"]) { display: none !important; }
  .current-user, .current-user ~ a { display: none !important; }
  [${NIFI_STATUS_HIDDEN_ATTRIBUTE}="true"] { display: none !important; }
  [${NIFI_STATUS_BAR_HIDDEN_ATTRIBUTE}="true"] { display: none !important; }
  [title="Connected nodes / Total number of nodes in the cluster"],
  [title="연결된 노드 / 클러스터 전체 노드"],
  [title="Total queued data"],
  [title="총 대기 데이터"],
  [title="Transmitting Remote Process Groups"],
  [title="전송 중인 원격 프로세스 그룹"],
  [title="Not Transmitting Remote Process Groups"],
  [title="전송 중이 아닌 원격 프로세스 그룹"],
  [title="Running Components"],
  [title="실행 중 컴포넌트"],
  [title="Stopped Components"],
  [title="중지된 컴포넌트"],
  [title="Invalid Components"],
  [title="유효하지 않은 컴포넌트"],
  [title="Disabled Components"],
  [title="비활성 컴포넌트"],
  [title="Valid Components"],
  [title="Up to date Versioned Process Groups"],
  [title="유효한 컴포넌트"],
  [title="Last refresh"],
  [title="Last Refresh"],
  [title="마지막 새로고침"],
  [title="Locally modified Versioned Process Groups"],
  [title="로컬 수정된 버전 프로세스 그룹"],
  [title="Stale Versioned Process Groups"],
  [title="오래된 버전 프로세스 그룹"],
  [title="Locally modified and stale Versioned Process Groups"],
  [title="로컬 수정 및 오래된 버전 프로세스 그룹"],
  [title="Sync failure Versioned Process Groups"],
  [title="동기화 실패 버전 프로세스 그룹"],
  .flex.items-center.gap-x-2:has(.icon-threads),
  .flex.items-center.gap-x-2:has(.fa-list),
  .flex.items-center.gap-x-2:has(.fa-bullseye),
  .flex.items-center.gap-x-2:has(.icon-transmit-false),
  .flex.items-center.gap-x-2:has(.fa-asterisk),
  .flex.items-center.gap-x-2:has(.fa-arrow-circle-up),
  .flex.items-center.gap-x-2:has(.fa-exclamation-circle),
  .flex.items-center.gap-x-2:has(.fa-question) {
    display: none !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"], [${NIFI_PANEL_ATTRIBUTE}="operation"] {
    position: fixed !important;
    z-index: 9000 !important;
    box-shadow: 0 2px 8px rgb(15 23 42 / 18%) !important;
    cursor: pointer !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]),
  [${NIFI_PANEL_ATTRIBUTE}="operation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) {
    width: 38px !important;
    height: 38px !important;
    min-width: 38px !important;
    min-height: 38px !important;
    max-width: 38px !important;
    max-height: 38px !important;
    overflow: hidden !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"][${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"],
  [${NIFI_PANEL_ATTRIBUTE}="operation"][${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"] {
    width: 270px !important;
    height: auto !important;
    min-width: 240px !important;
    min-height: 38px !important;
    max-width: 320px !important;
    max-height: 360px !important;
    overflow: auto !important;
    cursor: move !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) {
    left: 0 !important;
    top: 126px !important;
    right: auto !important;
    bottom: auto !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="operation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) {
    left: 0 !important;
    top: 168px !important;
    right: auto !important;
    bottom: auto !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"][${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"] {
    top: 126px !important;
    right: auto !important;
    bottom: auto !important;
    left: 0 !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="operation"][${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"] {
    left: 0 !important;
    top: 380px !important;
    right: auto !important;
    bottom: auto !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) > :not(:first-child),
  [${NIFI_PANEL_ATTRIBUTE}="operation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) > :not(:first-child) {
    display: none !important;
  }
  [data-cerebro-mini-create-toolbar="true"],
  [data-cerebro-mini-create-tool="true"] {
    display: none !important;
  }
`;

const NIFI_TOOLTIP_TEXT: Record<string, string> = {
  Processor: "프로세서",
  "Process Group": "프로세스 그룹",
  "Remote Process Group": "원격 프로세스 그룹",
  "Input Port": "입력 포트",
  "Output Port": "출력 포트",
  Funnel: "퍼널",
  Label: "라벨",
  Template: "템플릿",
  "Import from Registry": "레지스트리에서 가져오기",
  "Active Threads": "활성 스레드",
  "Total queued data": "총 대기 데이터",
  "Running Components": "실행 중 컴포넌트",
  "Stopped Components": "중지된 컴포넌트",
  "Invalid Components": "유효하지 않은 컴포넌트",
  "Disabled Components": "비활성 컴포넌트",
  "Valid Components": "유효한 컴포넌트",
  "Last refresh": "마지막 새로고침",
  "Last Refresh": "마지막 새로고침",
  "Connected nodes / Total number of nodes in the cluster": "연결된 노드 / 클러스터 전체 노드",
  "Transmitting Remote Process Groups": "전송 중인 원격 프로세스 그룹",
  "Not Transmitting Remote Process Groups": "전송 중이 아닌 원격 프로세스 그룹",
  "Up to date Versioned Process Groups": "유효한 컴포넌트",
  "Locally modified Versioned Process Groups": "로컬 수정된 버전 프로세스 그룹",
  "Stale Versioned Process Groups": "오래된 버전 프로세스 그룹",
  "Locally modified and stale Versioned Process Groups": "로컬 수정 및 오래된 버전 프로세스 그룹",
  "Sync failure Versioned Process Groups": "동기화 실패 버전 프로세스 그룹",
  Canvas: "캔버스",
  Summary: "요약",
  Counter: "카운터",
  "Bulletin Board": "공지 게시판",
  "Data Provenance": "데이터 계보",
  "Controller Settings": "컨트롤러 설정",
  "Parameter Contexts": "파라미터 컨텍스트",
  "Flow Configuration History": "플로우 구성 이력",
  "Node Status History": "노드 상태 이력",
  "System Diagnostics": "시스템 진단",
  Users: "사용자",
  Policies: "정책",
  Help: "도움말",
  About: "정보",
  Appearance: "화면 표시",
  Animations: "애니메이션",
  Refresh: "새로고침",
  "Leave Group": "상위 그룹으로 이동",
  Configure: "설정",
  "Controller Services": "컨트롤러 서비스",
  Start: "시작",
  Stop: "중지",
  Enable: "활성화",
  Disable: "비활성화",
  "Enable All Controller Services": "모든 컨트롤러 서비스 활성화",
  "Disable All Controller Services": "모든 컨트롤러 서비스 비활성화",
  "View Status History": "상태 기록 조회",
  "Manage Access Policies": "접근 권한 관리",
  "Download Flow Definition": "플로우 정의 다운로드",
  "Without External Services": "외부 서비스 제외",
  "With External Services": "외부 서비스 포함",
  "Without Externel Services": "외부 서비스 제외",
  "With Externel Services": "외부 서비스 포함",
  "Empty All Queues": "모든 대기열 비우기",
  Navigation: "탐색",
  Operation: "작업",
  "NiFi Counters": "ETL 카운터",
  "NiFi Counter": "ETL 카운터",
  Counters: "ETL 카운터",
  Filter: "검색어 입력",
  "Filter By": "필터 기준",
  Context: "영역",
  Name: "이름",
  name: "이름",
  Value: "값",
  "NiFi Summary": "요약",
  "Nifi Summary": "요약",
  Processors: "프로세서",
  "Input Ports": "입력 포트",
  "Output Ports": "출력 포트",
  "Remote Process Groups": "원격 프로세스 그룹",
  Connections: "연결",
  "Process Groups": "프로세스 그룹",
  Status: "상태",
  "All Statuses": "전체 상태",
  "Primary Node": "대표 노드",
  Type: "유형",
  "Run Status": "실행 상태",
  "Process Gro...": "프로세스 그룹",
  "Process Gro…": "프로세스 그룹",
  "Read | Write...": "읽기 | 쓰기",
  "Read | Write…": "읽기 | 쓰기",
  "Tasks | Tim...": "작업 수 | 시간",
  "Tasks | Tim…": "작업 수 | 시간",
  "NiFi Bulletin Board": "ETL 알림판",
  "Nifi Bulletin Board": "ETL 알림판",
  message: "메시지",
  Message: "메시지",
  Provenance: "계보",
  "Showing the most recent events.": "최신 이벤트를 표시 중입니다.",
  "component name": "컴포넌트 이름",
  "Component Name": "컴포넌트 이름",
  "Component Type": "컴포넌트 유형",
  "Event Time": "발생 시각",
  "FlowFile UUID": "FlowFile UUID",
  "File Size": "파일 크기",
  "NiFi Settings": "ETL 설정",
  "Nifi Settings": "ETL 설정",
  General: "일반",
  "Management Controller Services": "관리 컨트롤러 서비스",
  "Reporting Tasks": "리포팅 태스크",
  "Flow Analysis Rules": "플로우 분석 규칙",
  "Registry Clients": "레지스트리 클라이언트",
  "Parameter Providers": "파라미터 제공자",
  "Maximum Timer Driven Thread Count": "최대 타이머 기반 스레드 수",
  Provider: "제공자",
  Description: "설명",
  id: "ID",
  Id: "ID",
  ID: "ID",
  "Date Range": "기간",
  "Start Time (KST)*": "시작 시간 (KST)*",
  "End Time (KST)*": "종료 시간 (KST)*",
  "Clear Filter": "필터 초기화",
  "Date/Time": "일시",
  User: "사용자",
  user: "사용자",
  "NiFi Users": "사용자",
  "Nifi Users": "사용자",
  "Add User": "사용자 추가",
  Individual: "개인",
  Group: "그룹",
  "Identity*": "식별 정보*",
  Membership: "멤버십",
  Cancel: "취소",
  Add: "추가",
  Apply: "적용",
};
const NIFI_TOOLTIP_ATTRIBUTES = ["title", "aria-label", "data-tooltip", "matTooltip", "mattooltip", "tooltip", "placeholder"];
const NIFI_STATUS_TOOLTIP_ICON_TEXT: Array<[string, string]> = [
  ["fa-play", "실행 중 컴포넌트"],
  ["fa-stop", "중지된 컴포넌트"],
  ["fa-warning", "유효하지 않은 컴포넌트"],
  ["icon-enable-false", "비활성 컴포넌트"],
  ["fa-check", "유효한 컴포넌트"],
  ["fa-refresh", "마지막 새로고침"],
];
const HIDDEN_NIFI_STATUS_TITLES = new Set([
  "Total queued data",
  "총 대기 데이터",
  "Connected nodes / Total number of nodes in the cluster",
  "연결된 노드 / 클러스터 전체 노드",
  "Transmitting Remote Process Groups",
  "전송 중인 원격 프로세스 그룹",
  "Not Transmitting Remote Process Groups",
  "전송 중이 아닌 원격 프로세스 그룹",
  "Running Components",
  "실행 중 컴포넌트",
  "Stopped Components",
  "중지된 컴포넌트",
  "Invalid Components",
  "유효하지 않은 컴포넌트",
  "Disabled Components",
  "비활성 컴포넌트",
  "Valid Components",
  "Up to date Versioned Process Groups",
  "유효한 컴포넌트",
  "Last refresh",
  "Last Refresh",
  "마지막 새로고침",
  "Locally modified Versioned Process Groups",
  "로컬 수정된 버전 프로세스 그룹",
  "Stale Versioned Process Groups",
  "오래된 버전 프로세스 그룹",
  "Locally modified and stale Versioned Process Groups",
  "로컬 수정 및 오래된 버전 프로세스 그룹",
  "Sync failure Versioned Process Groups",
  "동기화 실패 버전 프로세스 그룹",
]);
const HIDDEN_NIFI_STATUS_ICON_CLASSES = [
  "icon-threads",
  "fa-list",
  "fa-bullseye",
  "icon-transmit-false",
  "fa-asterisk",
  "fa-arrow-circle-up",
  "fa-exclamation-circle",
  "fa-question",
];

function translateNifiTooltip(value: string) {
  const trimmed = value.trim();
  const exact = NIFI_TOOLTIP_TEXT[trimmed];
  if (exact) {
    return exact;
  }
  if (trimmed.startsWith("Active Threads")) {
    return trimmed.replace("Active Threads", NIFI_TOOLTIP_TEXT["Active Threads"]);
  }
  const displayingMatch = trimmed.match(/^Displaying\s+(\d+)\s+of\s+(\d+)$/i);
  if (displayingMatch) {
    return `전체 ${displayingMatch[2]}개 중 ${displayingMatch[1]}개 표시 중`;
  }
  const filterMatchedMatch = trimmed.match(/^Filter\s+matched\s+(\d+)\s+of\s+(\d+)$/i);
  if (filterMatchedMatch) {
    return `필터 조건 일치: ${filterMatchedMatch[1]} / ${filterMatchedMatch[2]}개`;
  }
  const oldestEventMatch = trimmed.match(/^Oldest\s+event\s+available:\s*(.+)$/i);
  if (oldestEventMatch) {
    return `가장 오래된 이벤트: ${oldestEventMatch[1]}`;
  }
  if (/^Maximum\s+Timer\s+Driven\s+Thread\s+Count\b/i.test(trimmed)) {
    return "최대 타이머 기반 스레드 수";
  }
  const sortableNameMatch = trimmed.match(/^Name\s*([↑↓])?$/);
  if (sortableNameMatch) {
    return `이름${sortableNameMatch[1] ? ` ${sortableNameMatch[1]}` : ""}`;
  }
  const sortableProviderMatch = trimmed.match(/^Provider\s*([↑↓])?$/);
  if (sortableProviderMatch) {
    return `제공자${sortableProviderMatch[1] ? ` ${sortableProviderMatch[1]}` : ""}`;
  }
  const sortableDescriptionMatch = trimmed.match(/^Description\s*([↑↓])?$/);
  if (sortableDescriptionMatch) {
    return `설명${sortableDescriptionMatch[1] ? ` ${sortableDescriptionMatch[1]}` : ""}`;
  }
  const sortableIdMatch = trimmed.match(/^Id\s*([↑↓])?$/i);
  if (sortableIdMatch) {
    return `ID${sortableIdMatch[1] ? ` ${sortableIdMatch[1]}` : ""}`;
  }
  const sortableDateTimeMatch = trimmed.match(/^Date\/Time\s*([↑↓])?$/);
  if (sortableDateTimeMatch) {
    return `일시${sortableDateTimeMatch[1] ? ` ${sortableDateTimeMatch[1]}` : ""}`;
  }
  const sortableOperationMatch = trimmed.match(/^Operation\s*([↑↓])?$/);
  if (sortableOperationMatch) {
    return `작업${sortableOperationMatch[1] ? ` ${sortableOperationMatch[1]}` : ""}`;
  }
  const sortableUserMatch = trimmed.match(/^User\s*([↑↓])?$/);
  if (sortableUserMatch) {
    return `사용자${sortableUserMatch[1] ? ` ${sortableUserMatch[1]}` : ""}`;
  }
  if (/^In\s+\(Size\)/i.test(trimmed)) {
    return "In";
  }
  if (/^Out\s+\(Size\)/i.test(trimmed)) {
    return "Out";
  }
  if (/^Read\s*\|\s*Write/i.test(trimmed)) {
    return "읽기 | 쓰기";
  }
  if (/^Tasks\s*\|\s*Tim/i.test(trimmed)) {
    return "작업 수 | 시간";
  }
  return undefined;
}

function elementText(element: Element) {
  return (element.textContent ?? "").replace(/\s+/g, " ").trim();
}

function iframeDocument(frame: HTMLIFrameElement) {
  try {
    return frame.contentDocument;
  } catch {
    // Keycloak 로그인 리다이렉트 등 cross-origin 문서인 동안은 접근이 막힌다 -
    // 같은 출처(NiFi/Airflow 자체 화면)로 돌아오면 다음 onLoad에서 다시 시도된다.
    return null;
  }
}

function isVisibleElement(element: Element) {
  const rect = element.getBoundingClientRect();
  const style = element.ownerDocument.defaultView?.getComputedStyle(element);
  return rect.width > 0 && rect.height > 0 && style?.display !== "none" && style?.visibility !== "hidden";
}

function closestNifiDialogElement(element: Element | null) {
  return element?.closest(
    [
      '[role="dialog"]',
      ".cdk-overlay-pane",
      ".mat-dialog-container",
      ".mat-mdc-dialog-container",
      ".processor-configuration",
    ].join(", "),
  ) ?? null;
}

function hasOpenNifiDialog(doc: Document) {
  return Array.from(
    doc.querySelectorAll('[role="dialog"], .mat-dialog-container, .mat-mdc-dialog-container, .processor-configuration'),
  ).some(isVisibleElement);
}

function hideToolChrome(frame: HTMLIFrameElement) {
  const doc = iframeDocument(frame);
  if (!doc) {
    return;
  }

  try {
    let style = doc.getElementById(HIDE_TOOL_CHROME_STYLE_ID);
    if (!style) {
      style = doc.createElement("style");
      style.id = HIDE_TOOL_CHROME_STYLE_ID;
      doc.head.appendChild(style);
    }
    if (style.textContent !== HIDE_TOOL_CHROME_CSS) {
      style.textContent = HIDE_TOOL_CHROME_CSS;
    }
  } catch {
    // 문서가 아직 교체 중이면 다음 iframe load에서 다시 시도한다.
  }
}

function cleanupLegacyNifiMiniCreateToolbars(frame: HTMLIFrameElement) {
  const doc = iframeDocument(frame);
  if (!doc) {
    return;
  }

  const cleanup = () => {
    doc
      .querySelectorAll('[data-cerebro-mini-create-toolbar="true"], [data-cerebro-mini-create-tool="true"]')
      .forEach((element) => {
        element.remove();
      });
  };

  cleanup();

  if (doc.documentElement.getAttribute(LEGACY_NIFI_MINI_CREATE_TOOLBAR_CLEANUP_ATTRIBUTE) === "true") {
    return;
  }
  doc.documentElement.setAttribute(LEGACY_NIFI_MINI_CREATE_TOOLBAR_CLEANUP_ATTRIBUTE, "true");

  const frameWindow = doc.defaultView;
  let frameId: number | null = null;
  const observer = new (frameWindow?.MutationObserver ?? MutationObserver)(() => {
    if (frameId !== null) {
      return;
    }
    frameId = (frameWindow ?? window).requestAnimationFrame(() => {
      frameId = null;
      cleanup();
    });
  });

  observer.observe(doc.documentElement, { childList: true, subtree: true });
  frameWindow?.addEventListener("beforeunload", () => {
    if (frameId !== null) {
      frameWindow.cancelAnimationFrame(frameId);
    }
    observer.disconnect();
  }, { once: true });
}

function patchKoreanTooltips(frame: HTMLIFrameElement) {
  const doc = iframeDocument(frame);
  if (!doc) {
    return;
  }

  const patchTitleElement = (element: Element) => {
    if (element.tagName.toLowerCase() !== "title") {
      return;
    }
    const value = element.textContent?.trim();
    const translated = value ? translateNifiTooltip(value) : undefined;
    if (translated) {
      element.textContent = translated;
    }
  };

  const patchExactTextElement = (element: Element) => {
    const value = element.textContent?.trim();
    if (!value || value.length > 80 || value.includes("\n")) {
      return;
    }
    const translated = value ? translateNifiTooltip(value) : undefined;
    if (translated) {
      element.textContent = translated;
    }
  };

  const patchTextNode = (node: Text) => {
    const value = node.nodeValue ?? "";
    const trimmed = value.trim();
    if (!trimmed || trimmed.length > 80 || trimmed.includes("\n")) {
      return;
    }
    const translated = translateNifiTooltip(trimmed);
    if (!translated) {
      return;
    }
    node.nodeValue = value.replace(trimmed, translated);
  };

  const patchTextNodes = (root: Document | Element) => {
    const walker = doc.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
      acceptNode: (node) => {
        const parent = node.parentElement;
        const tagName = parent?.tagName.toLowerCase();
        return tagName === "script" || tagName === "style" ? NodeFilter.FILTER_REJECT : NodeFilter.FILTER_ACCEPT;
      },
    });
    let current = walker.nextNode();
    while (current) {
      patchTextNode(current as Text);
      current = walker.nextNode();
    }
  };

  const patchStatusVisibility = (element: Element, value: string | null) => {
    if (!value) {
      return;
    }
    const trimmed = value.trim();
    if (
      HIDDEN_NIFI_STATUS_TITLES.has(trimmed) ||
      trimmed.startsWith("Active Threads") ||
      trimmed.startsWith(NIFI_TOOLTIP_TEXT["Active Threads"])
    ) {
      element.setAttribute(NIFI_STATUS_HIDDEN_ATTRIBUTE, "true");
    }
  };

  const patchStatusIconVisibility = (element: Element) => {
    const className = elementClassName(element);
    const shouldHide = HIDDEN_NIFI_STATUS_ICON_CLASSES.some((iconClass) =>
      className.split(/\s+/).includes(iconClass),
    );
    if (!shouldHide) {
      return;
    }

    let current: Element = element;
    for (let depth = 0; depth < 4 && current.parentElement; depth += 1) {
      const currentClassName = elementClassName(current);
      if (current.hasAttribute("title") || (currentClassName.includes("flex") && currentClassName.includes("gap-x-2"))) {
        current.setAttribute(NIFI_STATUS_HIDDEN_ATTRIBUTE, "true");
        return;
      }
      current = current.parentElement;
    }
  };

  const closestStatusItem = (element: Element) => {
    let current: Element = element;
    for (let depth = 0; depth < 4 && current.parentElement; depth += 1) {
      const className = elementClassName(current);
      if (current.hasAttribute("title") || (className.includes("flex") && className.includes("gap-x-2"))) {
        return current;
      }
      current = current.parentElement;
    }
    return null;
  };

  const patchStatusBarVisibility = (element: Element) => {
    let current: Element | null = element;
    for (let depth = 0; depth < 8 && current; depth += 1) {
      const classNames = elementClassName(current).split(/\s+/);
      const text = current.textContent ?? "";
      const hasRefresh =
        classNames.includes("fa-refresh") ||
        Boolean(current.querySelector(".fa-refresh")) ||
        /\bKST\b/.test(text);
      const hasComponentStatus =
        classNames.some((className) => ["fa-play", "fa-stop", "fa-warning", "icon-enable-false", "fa-check"].includes(className)) ||
        Boolean(current.querySelector(".fa-play, .fa-stop, .fa-warning, .icon-enable-false, .fa-check"));
      const hasPrimaryToolbar =
        current.querySelector(
          [
            '[title="Processor"]',
            '[title="프로세서"]',
            '[title="Input Port"]',
            '[title="입력 포트"]',
            '[title="Output Port"]',
            '[title="출력 포트"]',
            '[title="Process Group"]',
            '[title="프로세스 그룹"]',
            '[title="Funnel"]',
            '[title="퍼널"]',
            '[title="Label"]',
            '[title="라벨"]',
          ].join(", "),
        ) !== null;
      const rect = current.getBoundingClientRect();
      const looksLikeStatusBar = rect.height > 0 && rect.height <= 48 && rect.width >= 240;
      if (hasRefresh && hasComponentStatus && looksLikeStatusBar && !hasPrimaryToolbar) {
        current.setAttribute(NIFI_STATUS_BAR_HIDDEN_ATTRIBUTE, "true");
        return;
      }
      current = current.parentElement;
    }
  };

  const patchStatusTooltipText = (element: Element) => {
    const classNames = elementClassName(element).split(/\s+/);
    const translated = NIFI_STATUS_TOOLTIP_ICON_TEXT.find(([iconClass]) => classNames.includes(iconClass))?.[1];
    if (!translated) {
      return;
    }
    const statusItem = closestStatusItem(element);
    if (statusItem) {
      statusItem.setAttribute("title", translated);
      statusItem.setAttribute("aria-label", translated);
      patchStatusVisibility(statusItem, translated);
    }
  };

  const patchElement = (element: Element) => {
    NIFI_TOOLTIP_ATTRIBUTES.forEach((attributeName) => {
      const value = element.getAttribute(attributeName);
      if (attributeName === "title") {
        patchStatusVisibility(element, value);
      }
      const translated = value ? translateNifiTooltip(value) : undefined;
      if (translated) {
        element.setAttribute(attributeName, translated);
        if (attributeName === "title") {
          patchStatusVisibility(element, translated);
        }
      }
    });
    patchTitleElement(element);
    patchExactTextElement(element);
    patchStatusIconVisibility(element);
    patchStatusTooltipText(element);
    patchStatusBarVisibility(element);
  };

  const patchDocument = () => {
    doc
      .querySelectorAll("[title], [aria-label], [data-tooltip], [matTooltip], [mattooltip], [tooltip], [placeholder], title")
      .forEach(patchElement);
    patchTextNodes(doc);
    HIDDEN_NIFI_STATUS_ICON_CLASSES.forEach((iconClass) => {
      doc.querySelectorAll(`.${iconClass}`).forEach(patchStatusIconVisibility);
    });
    NIFI_STATUS_TOOLTIP_ICON_TEXT.forEach(([iconClass]) => {
      doc.querySelectorAll(`.${iconClass}`).forEach(patchStatusTooltipText);
    });
    doc.querySelectorAll(".fa-refresh, .fa-play, .fa-stop, .fa-warning, .icon-enable-false, .fa-check").forEach(patchStatusBarVisibility);
  };

  patchDocument();

  if (doc.documentElement.getAttribute(KOREAN_TOOLTIP_PATCH_ATTRIBUTE) === "true") {
    return;
  }
  doc.documentElement.setAttribute(KOREAN_TOOLTIP_PATCH_ATTRIBUTE, "true");

  const DocumentMutationObserver = doc.defaultView?.MutationObserver ?? MutationObserver;
  const observer = new DocumentMutationObserver((mutations) => {
    if (hasOpenNifiDialog(doc)) {
      return;
    }
    for (const mutation of mutations) {
      if (mutation.type === "attributes" && mutation.target instanceof Element) {
        patchElement(mutation.target);
      }
      if (mutation.type === "characterData" && mutation.target.parentElement) {
        patchTextNode(mutation.target as Text);
        patchTitleElement(mutation.target.parentElement);
        patchExactTextElement(mutation.target.parentElement);
      }
      mutation.addedNodes.forEach((node) => {
        if (node.nodeType === 3) {
          patchTextNode(node as Text);
          if (node.parentElement) {
            patchExactTextElement(node.parentElement);
          }
          return;
        }
        if (!(node instanceof Element)) {
          return;
        }
        patchElement(node);
        patchTextNodes(node);
        node
          .querySelectorAll("[title], [aria-label], [data-tooltip], [matTooltip], [mattooltip], [tooltip], [placeholder], title")
          .forEach(patchElement);
        node.querySelectorAll("*").forEach(patchExactTextElement);
        HIDDEN_NIFI_STATUS_ICON_CLASSES.forEach((iconClass) => {
          node.querySelectorAll(`.${iconClass}`).forEach(patchStatusIconVisibility);
        });
        NIFI_STATUS_TOOLTIP_ICON_TEXT.forEach(([iconClass]) => {
          node.querySelectorAll(`.${iconClass}`).forEach(patchStatusTooltipText);
        });
        node
          .querySelectorAll(".fa-refresh, .fa-play, .fa-stop, .fa-warning, .icon-enable-false, .fa-check")
          .forEach(patchStatusBarVisibility);
      });
    }
  });

  observer.observe(doc.documentElement, {
    attributeFilter: NIFI_TOOLTIP_ATTRIBUTES,
    attributes: true,
    characterData: true,
    childList: true,
    subtree: true,
  });

  if (doc.documentElement.getAttribute(KOREAN_TOOLTIP_INTERVAL_ATTRIBUTE) !== "true") {
    doc.documentElement.setAttribute(KOREAN_TOOLTIP_INTERVAL_ATTRIBUTE, "true");
    const intervalId = doc.defaultView?.setInterval(() => {
      if (!hasOpenNifiDialog(doc)) {
        patchDocument();
      }
    }, 1000);
    doc.defaultView?.addEventListener("beforeunload", () => {
      if (intervalId) {
        doc.defaultView?.clearInterval(intervalId);
      }
    }, { once: true });
  }
}

function installNifiPanelLayout(frame: HTMLIFrameElement) {
  const doc = iframeDocument(frame);
  if (!doc) {
    return;
  }

  const bindPanelBehavior = (panel: HTMLElement) => {
    if (panel.getAttribute(NIFI_PANEL_BOUND_ATTRIBUTE) === "true") {
      return;
    }
    panel.setAttribute(NIFI_PANEL_BOUND_ATTRIBUTE, "true");
    panel.setAttribute(NIFI_PANEL_EXPANDED_ATTRIBUTE, "false");
    panel.setAttribute("title", "클릭하면 펼치고, 펼친 뒤 빈 영역을 드래그하면 이동합니다.");

    let drag:
      | {
          startX: number;
          startY: number;
          left: number;
          top: number;
          moved: boolean;
        }
      | null = null;

    const move = (event: MouseEvent) => {
      if (!drag) {
        return;
      }
      const dx = event.clientX - drag.startX;
      const dy = event.clientY - drag.startY;
      drag.moved = drag.moved || Math.abs(dx) > 4 || Math.abs(dy) > 4;
      const rect = panel.getBoundingClientRect();
      const maxLeft = Math.max(0, doc.documentElement.clientWidth - rect.width);
      const maxTop = Math.max(0, doc.documentElement.clientHeight - rect.height);
      panel.style.left = `${Math.min(Math.max(drag.left + dx, 0), maxLeft)}px`;
      panel.style.top = `${Math.min(Math.max(drag.top + dy, 0), maxTop)}px`;
      panel.style.right = "auto";
      panel.style.bottom = "auto";
    };

    const stop = () => {
      if (!drag) {
        return;
      }
      const wasMoved = drag.moved;
      drag = null;
      doc.removeEventListener("mousemove", move, true);
      doc.removeEventListener("mouseup", stop, true);
      if (!wasMoved) {
        const panels = Array.from(doc.querySelectorAll(`[${NIFI_PANEL_ATTRIBUTE}]`));
        const isExpanded = panel.getAttribute(NIFI_PANEL_EXPANDED_ATTRIBUTE) === "true";
        panels.forEach((item) => {
          item.setAttribute(NIFI_PANEL_EXPANDED_ATTRIBUTE, item === panel && !isExpanded ? "true" : "false");
        });
      }
    };

    panel.addEventListener("mousedown", (event) => {
      if (event.button !== 0) {
        return;
      }
      const target = event.target instanceof Element ? event.target : null;
      const expanded = panel.getAttribute(NIFI_PANEL_EXPANDED_ATTRIBUTE) === "true";
      if (expanded && target?.closest("button, a, input, select, textarea, [role='button']")) {
        return;
      }
      const rect = panel.getBoundingClientRect();
      drag = {
        startX: event.clientX,
        startY: event.clientY,
        left: rect.left,
        top: rect.top,
        moved: false,
      };
      doc.addEventListener("mousemove", move, true);
      doc.addEventListener("mouseup", stop, true);
      event.preventDefault();
      event.stopPropagation();
    }, true);
  };

  const markPanel = (panelName: "navigation" | "operation") => {
    const label = panelName === "navigation" ? "Navigation" : "Operation";
    const panelRoot = (element: Element) => {
      let current = element;
      while (current.parentElement && current.parentElement !== doc.body) {
        const parent = current.parentElement;
        const parentRect = parent.getBoundingClientRect();
        const parentText = parent.textContent ?? "";
        if (!parentText.includes(label) || parentRect.width > 420 || parentRect.height > 520) {
          break;
        }
        current = parent;
      }
      return current;
    };

    const candidates = Array.from(new Set(Array.from(doc.querySelectorAll("aside, section, div"))
      .filter((element) => {
        const className = elementClassName(element).toLowerCase();
        const text = element.textContent ?? "";
        return className.includes(panelName) || text.includes(label);
      })
      .map(panelRoot)))
      .map((element) => ({
        element,
        rect: element.getBoundingClientRect(),
      }))
      .filter((candidate) =>
        candidate.rect.width >= 30
        && candidate.rect.height >= 30
        && candidate.rect.width <= 420
        && candidate.rect.height <= 520,
      )
      .sort((a, b) => (b.rect.width * b.rect.height) - (a.rect.width * a.rect.height));

    const panel = candidates[0]?.element;
    if (panel instanceof HTMLElement) {
      panel.setAttribute(NIFI_PANEL_ATTRIBUTE, panelName);
      bindPanelBehavior(panel);
    }
  };

  const applyLayout = () => {
    if (hasOpenNifiDialog(doc)) {
      return;
    }
    markPanel("navigation");
    markPanel("operation");
  };

  applyLayout();

  if (doc.documentElement.getAttribute(NIFI_PANEL_LAYOUT_ATTRIBUTE) === "true") {
    return;
  }
  doc.documentElement.setAttribute(NIFI_PANEL_LAYOUT_ATTRIBUTE, "true");

  let layoutFrameId: number | null = null;
  const scheduleLayout = () => {
    if (layoutFrameId !== null) {
      return;
    }
    const frameWindow = doc.defaultView;
    const run = () => {
      layoutFrameId = null;
      applyLayout();
    };
    layoutFrameId = frameWindow?.requestAnimationFrame
      ? frameWindow.requestAnimationFrame(run)
      : window.requestAnimationFrame(run);
  };

  const DocumentMutationObserver = doc.defaultView?.MutationObserver ?? MutationObserver;
  const observer = new DocumentMutationObserver(() => scheduleLayout());
  observer.observe(doc.documentElement, {
    childList: true,
    subtree: true,
  });
  doc.defaultView?.addEventListener("beforeunload", () => {
    if (layoutFrameId !== null) {
      doc.defaultView?.cancelAnimationFrame(layoutFrameId);
      layoutFrameId = null;
    }
    observer.disconnect();
  }, { once: true });
}

function clickNifiMenuItem(frame: HTMLIFrameElement, titles: string[]) {
  const doc = iframeDocument(frame);
  if (!doc || titles.length === 0) {
    return;
  }

  const key = titles.join("|");
  if (doc.documentElement.getAttribute(NIFI_AUTO_OPEN_MENU_ATTRIBUTE) === key) {
    return;
  }
  doc.documentElement.setAttribute(NIFI_AUTO_OPEN_MENU_ATTRIBUTE, key);

  const normalizedTitles = titles.map((title) => title.replace(/\s+/g, " ").trim()).filter(Boolean);
  const matches = (element: Element) => {
    const values = [
      element.getAttribute("title"),
      element.getAttribute("aria-label"),
      element.getAttribute("data-tooltip"),
      element.getAttribute("mattooltip"),
      element.getAttribute("matTooltip"),
      elementText(element),
    ].map((value) => value?.replace(/\s+/g, " ").trim()).filter(Boolean);

    return normalizedTitles.some((title) =>
      values.some((value) => value === title || value?.includes(title)),
    );
  };

  const tryClick = () => {
    const candidates = Array.from(doc.querySelectorAll("button, a, [role='button'], [title], [aria-label]"));
    const matched = candidates.find((element) => matches(element));
    const clickable = matched?.closest("button, a, [role='button']") ?? matched;
    if (clickable instanceof HTMLElement && isVisibleElement(clickable)) {
      clickable.click();
      return true;
    }
    return false;
  };

  if (tryClick()) {
    return;
  }

  let attempts = 0;
  const frameWindow = doc.defaultView;
  const timerId = frameWindow?.setInterval(() => {
    attempts += 1;
    if (tryClick() || attempts >= 20) {
      frameWindow.clearInterval(timerId);
    }
  }, 250);
  frameWindow?.addEventListener("beforeunload", () => {
    if (timerId) {
      frameWindow.clearInterval(timerId);
    }
  }, { once: true });
}

function looksLikeUuid(value: string | null | undefined) {
  return !!value && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
}

function cleanDomId(value: string | null | undefined) {
  if (!value) {
    return null;
  }
  return value.replace(/^(id-|component-|process-group-|pg-)/i, "");
}

function elementClassName(element: Element) {
  return typeof element.className === "string" ? element.className : element.getAttribute("class") ?? "";
}

function readProcessGroupIdFromElement(element: Element | null) {
  let current = element;
  while (current && current instanceof Element) {
    const className = elementClassName(current).toLowerCase();
    const role = `${current.getAttribute("data-component-type") ?? ""} ${current.getAttribute("data-type") ?? ""}`.toLowerCase();
    const isProcessGroupElement = className.includes("process-group") || role.includes("process_group") || role.includes("process group");

    if (isProcessGroupElement) {
      const candidates = [
        current.getAttribute("data-id"),
        current.getAttribute("data-component-id"),
        current.getAttribute("data-identifier"),
        current.getAttribute("component-id"),
        current.id,
      ];
      const matched = candidates.map(cleanDomId).find(looksLikeUuid);
      if (matched) {
        return matched;
      }
    }
    current = current.parentElement;
  }
  return null;
}

function readProcessGroupIdFromOperationPanel(doc: Document) {
  const panels = Array.from(doc.querySelectorAll("aside, section, div")).filter((element) =>
    /operation/i.test(elementClassName(element)) || /(Operation|작업)/.test(element.textContent ?? ""),
  );

  for (const panel of panels) {
    const text = panel.textContent ?? "";
    if (!/Process\s+Group/i.test(text) && !/프로세스\s*그룹/.test(text)) {
      continue;
    }
    const id = text.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i)?.[0];
    if (id) {
      return id;
    }
  }
  return null;
}

function readProcessorIdFromElement(element: Element | null) {
  let current = element;
  while (current && current instanceof Element) {
    const className = elementClassName(current).toLowerCase();
    const role = `${current.getAttribute("data-component-type") ?? ""} ${current.getAttribute("data-type") ?? ""}`.toLowerCase();
    const isProcessorElement =
      !className.includes("process-group") && (className.includes("processor") || /\bprocessor\b/.test(role));

    if (isProcessorElement) {
      const candidates = [
        current.getAttribute("data-id"),
        current.getAttribute("data-component-id"),
        current.getAttribute("data-identifier"),
        current.getAttribute("component-id"),
        current.id,
      ];
      const matched = candidates.map(cleanDomId).find(looksLikeUuid);
      if (matched) {
        return matched;
      }
    }
    current = current.parentElement;
  }
  return null;
}

type CanvasSelection =
  | { type: "processGroup"; id: string }
  | { type: "processor"; id: string };

function readSelectionFromOperationPanel(doc: Document): CanvasSelection | null {
  const panels = Array.from(doc.querySelectorAll("aside, section, div")).filter((element) =>
    /operation/i.test(elementClassName(element)) || /(Operation|작업)/.test(element.textContent ?? ""),
  );

  for (const panel of panels) {
    const text = panel.textContent ?? "";
    const id = text.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i)?.[0];
    if (!id) {
      continue;
    }
    if (/Process\s+Group/i.test(text)) {
      return { type: "processGroup", id };
    }
    if (/\bProcessor\b/i.test(text)) {
      return { type: "processor", id };
    }
  }
  const groupId = readProcessGroupIdFromOperationPanel(doc);
  return groupId ? { type: "processGroup", id: groupId } : null;
}

function installCanvasSelectionSync(
  frame: HTMLIFrameElement,
  onSelectionChange: (selection: CanvasSelection) => void,
  resolveProcessGroupSelection?: (target: Element | null) => string | null,
) {
  const doc = iframeDocument(frame);
  if (!doc || doc.documentElement.getAttribute(CANVAS_SELECTION_SYNC_ATTRIBUTE) === "true") {
    return;
  }
  doc.documentElement.setAttribute(CANVAS_SELECTION_SYNC_ATTRIBUTE, "true");

  const notifyFromDocument = (target: Element | null) => {
    if (closestNifiDialogElement(target)) {
      return;
    }

    const clickedProcessorId = readProcessorIdFromElement(target);
    if (clickedProcessorId) {
      onSelectionChange({ type: "processor", id: clickedProcessorId });
      return;
    }

    const clickedGroupId = readProcessGroupIdFromElement(target);
    if (clickedGroupId) {
      onSelectionChange({ type: "processGroup", id: clickedGroupId });
      return;
    }

    const resolvedGroupId = resolveProcessGroupSelection?.(target);
    if (resolvedGroupId) {
      onSelectionChange({ type: "processGroup", id: resolvedGroupId });
      return;
    }

    window.setTimeout(() => {
      const selected = readSelectionFromOperationPanel(doc);
      if (selected) {
        onSelectionChange(selected);
        return;
      }
      const delayedGroupId = resolveProcessGroupSelection?.(target);
      if (delayedGroupId) {
        onSelectionChange({ type: "processGroup", id: delayedGroupId });
      }
    }, 80);
  };

  doc.addEventListener("click", (event) => notifyFromDocument(event.target as Element | null), true);
  doc.addEventListener("dblclick", (event) => notifyFromDocument(event.target as Element | null), true);
}

function readProcessGroupIdFromFrameRoute(frame: HTMLIFrameElement) {
  const hash = frame.contentWindow?.location.hash ?? "";
  const matched = hash.match(/\/process-groups\/([^/?#]+)/);
  if (!matched?.[1]) {
    return null;
  }
  try {
    return decodeURIComponent(matched[1]);
  } catch {
    return matched[1];
  }
}

function installCanvasRouteSync(frame: HTMLIFrameElement, onProcessGroupChange: (groupId: string | null) => void) {
  const doc = iframeDocument(frame);
  const frameWindow = frame.contentWindow;
  if (!doc || !frameWindow || doc.documentElement.getAttribute(CANVAS_ROUTE_SYNC_ATTRIBUTE) === "true") {
    return;
  }
  doc.documentElement.setAttribute(CANVAS_ROUTE_SYNC_ATTRIBUTE, "true");

  let lastGroupId: string | null | undefined;
  const notify = () => {
    const nextGroupId = readProcessGroupIdFromFrameRoute(frame);
    if (nextGroupId === lastGroupId) {
      return;
    }
    lastGroupId = nextGroupId;
    onProcessGroupChange(nextGroupId);
  };

  notify();
  frameWindow.addEventListener("hashchange", notify);
  const timerId = frameWindow.setInterval(notify, 500);
  frameWindow.addEventListener("beforeunload", () => frameWindow.clearInterval(timerId), { once: true });
}

interface ConsoleFramePageProps {
  title: string;
  src: string;
  healthcheckSrc?: string;
  waitMessage?: string;
  showProcessGroupTree?: boolean;
  nifiAutoOpenMenuTitles?: string[];
}

function withProcessGroupId(url: string, processGroupId: string) {
  // NiFi의 Angular Router는 useHash:true(해시 기반)라 실제 라우트는 서버로
  // 전송되지 않는 URL 프래그먼트(#/...)에 있어야 한다 (canvas 라우팅이
  // router.navigate(["/process-groups", id])로 구현돼 있고, 라우터 모듈이
  // RouterModule.forRoot(routes,{useHash:!0})로 등록된 것을 번들에서 확인).
  // 일반 경로 세그먼트(/nifi/process-groups/{id})로 요청하면 서버가 실제
  // 리소스로 찾다가 없어서 자체 404(#/404)로 리다이렉트해버린다.
  const base = url.endsWith("/") ? url : `${url}/`;
  return `${base}#/process-groups/${encodeURIComponent(processGroupId)}`;
}

interface ProcessGroupTreePanelProps {
  activeGroupId: string | null;
  onTreeChange?: (tree: NifiProcessGroupTreeNode | null) => void;
  onCatalogSynced?: () => void;
}

function ProcessGroupTreePanel({ activeGroupId, onTreeChange, onCatalogSynced }: ProcessGroupTreePanelProps) {
  const navigate = useNavigate();
  const [tree, setTree] = useState<NifiProcessGroupTreeNode | null>(null);
  const treeRef = useRef<NifiProcessGroupTreeNode | null>(null);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");

  const loadTree = useCallback((forceRefresh = false, resetExpanded = false, quietError = false, syncCatalog = false) => {
    let cancelled = false;
    if (forceRefresh) {
      setIsRefreshing(true);
    } else if (!treeRef.current) {
      setIsLoading(true);
    }
    if (!quietError) {
      setError(null);
    }

    const request = forceRefresh ? refreshNifiProcessGroupTree() : getNifiProcessGroupTree();
    request
      .then(async (result) => {
        if (cancelled) {
          return;
        }
        treeRef.current = result;
        setTree(result);
        setError(null);
        onTreeChange?.(result);
        if (resetExpanded) {
          setExpandedIds(new Set());
        }
        if (syncCatalog) {
          try {
            await syncEtlJobs();
            if (!cancelled) {
              onCatalogSynced?.();
            }
          } catch (ex) {
            if (!cancelled && !quietError) {
              setError(ex instanceof Error ? ex.message : "ETL Job 상세 동기화에 실패했습니다.");
            }
          }
        }
      })
      .catch((ex) => {
        if (!cancelled) {
          if (!quietError) {
            setError(ex instanceof Error ? ex.message : "트리를 불러오지 못했습니다.");
            onTreeChange?.(null);
          }
        }
      })
      .finally(() => {
        if (!cancelled) {
          setIsLoading(false);
          setIsRefreshing(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [onCatalogSynced, onTreeChange]);

  useEffect(() => loadTree(true, true, false, true), [loadTree]);

  useEffect(() => {
    let cleanupRequest = () => {};
    const timerId = window.setInterval(() => {
      cleanupRequest();
      cleanupRequest = loadTree(false, false, true);
    }, 5000);
    return () => {
      window.clearInterval(timerId);
      cleanupRequest();
    };
  }, [loadTree]);

  useEffect(() => {
    const selected = findTreeNode(tree, activeGroupId);
    if (!selected) {
      return;
    }
    setExpandedIds((previous) => {
      const next = new Set(previous);
      selected.path.forEach((node) => next.add(node.id));
      return next;
    });
  }, [activeGroupId, tree]);

  const openGroup = (groupId: string) => {
    navigate(`/etl/manage?processGroupId=${encodeURIComponent(groupId)}`);
  };

  const toggle = (groupId: string) => {
    setExpandedIds((previous) => {
      const next = new Set(previous);
      if (next.has(groupId)) {
        next.delete(groupId);
      } else {
        next.add(groupId);
      }
      return next;
    });
  };

  const matchesSearch = (node: NifiProcessGroupTreeNode, normalizedQuery: string): boolean =>
    node.name.toLowerCase().includes(normalizedQuery);

  const filterTree = (
    node: NifiProcessGroupTreeNode,
    normalizedQuery: string,
  ): NifiProcessGroupTreeNode | null => {
    if (!normalizedQuery) {
      return node;
    }
    const filteredChildren = node.children
      .map((child) => filterTree(child, normalizedQuery))
      .filter((child): child is NifiProcessGroupTreeNode => child !== null);
    if (matchesSearch(node, normalizedQuery) || filteredChildren.length > 0) {
      return { ...node, children: filteredChildren };
    }
    return null;
  };

  const renderNode = (node: NifiProcessGroupTreeNode, depth = 0) => {
    const hasChildren = node.children.length > 0;
    const searching = searchQuery.trim().length > 0;
    const expanded = searching || expandedIds.has(node.id);
    const active = activeGroupId === node.id || (!activeGroupId && node.id === "root");
    return (
      <div key={node.id} className="nifi-tree-node">
        <div
          className={active ? "nifi-tree-row active" : "nifi-tree-row"}
          style={{ paddingLeft: 8 + depth * 13 }}
        >
          <button
            type="button"
            className={hasChildren ? "nifi-tree-toggle" : "nifi-tree-toggle empty"}
            aria-label={expanded ? `${node.name} 접기` : `${node.name} 펼치기`}
            onClick={() => hasChildren && toggle(node.id)}
          >
            {hasChildren ? (expanded ? "▼" : "▶") : ""}
          </button>
          <button type="button" className="nifi-tree-label" title={node.name} onClick={() => openGroup(node.id)}>
            <span className="nifi-tree-name">{node.name}</span>
          </button>
        </div>
        {hasChildren && expanded ? node.children.map((child) => renderNode(child, depth + 1)) : null}
      </div>
    );
  };

  const filteredTree = tree ? filterTree(tree, searchQuery.trim().toLowerCase()) : null;

  return (
    <aside className="nifi-tree-panel" aria-label="NiFi 프로세스 그룹 트리">
      <div className="nifi-tree-search-heading">
        <label className="nifi-tree-section-title" htmlFor="nifi-tree-search">
          트리 검색
        </label>
        <Button
          type="text"
          size="small"
          className="nifi-tree-refresh-button"
          icon={<ReloadOutlined />}
          loading={isRefreshing}
          title="트리 캐시 새로고침"
          aria-label="트리 캐시 새로고침"
          onClick={() => loadTree(true, false, false, true)}
        />
      </div>
      <input
        id="nifi-tree-search"
        className="nifi-tree-search-input"
        value={searchQuery}
        onChange={(event) => setSearchQuery(event.target.value)}
        placeholder="그룹명"
      />
      <div className="nifi-tree-scroll">
        {isLoading ? <div className="nifi-tree-message">로딩</div> : null}
        {!isLoading && error ? <div className="nifi-tree-message error">조회 실패</div> : null}
        {!isLoading && !error && filteredTree ? renderNode(filteredTree) : null}
        {!isLoading && !error && tree && !filteredTree ? <div className="nifi-tree-message">검색 결과 없음</div> : null}
        {!isLoading && !error && !tree ? <div className="nifi-tree-message">없음</div> : null}
      </div>
    </aside>
  );
}

function findTreeNode(
  node: NifiProcessGroupTreeNode | null,
  groupId: string | null,
  path: NifiProcessGroupTreeNode[] = [],
): { node: NifiProcessGroupTreeNode; path: NifiProcessGroupTreeNode[] } | null {
  if (!node) {
    return null;
  }
  const nextPath = [...path, node];
  if ((!groupId && node.id === "root") || node.id === groupId) {
    return { node, path: nextPath };
  }
  for (const child of node.children) {
    const found = findTreeNode(child, groupId, nextPath);
    if (found) {
      return found;
    }
  }
  return null;
}

function normalizeProcessGroupName(value: string | null | undefined) {
  return (value ?? "").replace(/\s+/g, " ").trim().toLowerCase();
}

function flattenTree(node: NifiProcessGroupTreeNode | null): NifiProcessGroupTreeNode[] {
  if (!node) {
    return [];
  }
  return [node, ...node.children.flatMap((child) => flattenTree(child))];
}

function processGroupTextCandidates(element: Element | null) {
  const values: string[] = [];
  let current = element;
  for (let depth = 0; current && depth < 8; depth += 1) {
    const rect = current.getBoundingClientRect();
    const cardSized = rect.width >= 60 && rect.width <= 620 && rect.height >= 25 && rect.height <= 240;
    [
      current.getAttribute("title"),
      current.getAttribute("aria-label"),
      current.getAttribute("data-name"),
      current.getAttribute("data-label"),
    ].forEach((value) => {
      if (value?.trim()) {
        values.push(value);
      }
    });
    if (cardSized && current.textContent?.trim()) {
      values.push(current.textContent);
    }
    current = current.parentElement;
  }
  return values.map(normalizeProcessGroupName).filter(Boolean);
}

function resolveProcessGroupIdByCanvasText(
  target: Element | null,
  currentGroupId: string | null,
  tree: NifiProcessGroupTreeNode | null,
) {
  const candidates = processGroupTextCandidates(target);
  if (candidates.length === 0) {
    return null;
  }

  const currentNode = findTreeNode(tree, currentGroupId)?.node ?? tree;
  const orderedNodes = [
    ...(currentNode?.children ?? []),
    ...flattenTree(tree).filter((node) => !(currentNode?.children ?? []).some((child) => child.id === node.id)),
  ];

  const match = orderedNodes.find((node) => {
    const name = normalizeProcessGroupName(node.name);
    return candidates.some((candidate) => candidate === name || candidate.startsWith(`${name} `));
  });

  return match?.id ?? null;
}

function formatDateTime(value?: string | null) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  const hour = String(parsed.getHours()).padStart(2, "0");
  const minute = String(parsed.getMinutes()).padStart(2, "0");
  return `${month}-${day} ${hour}:${minute}`;
}

function statusText(job: EtlJobResponse | null, node: NifiProcessGroupTreeNode | null) {
  if (node?.jobStatus) {
    return jobStatusText(node.jobStatus);
  }
  if (job?.invalidCount || node?.invalidCount) {
    return "실패";
  }
  if (job?.runningCount || node?.runningCount) {
    return "실행중";
  }
  return "완료";
}

function statusClass(job: EtlJobResponse | null, node: NifiProcessGroupTreeNode | null) {
  if (node?.jobStatus) {
    return jobStatusClass(node.jobStatus);
  }
  const status = statusText(job, node);
  if (status === "실패") {
    return "error";
  }
  if (status === "실행중" || status === "실행") {
    return "running";
  }
  if (status === "중지") {
    return "stopped";
  }
  return "done";
}

function jobStatusText(status: NifiProcessGroupTreeNode["jobStatus"]) {
  switch (status) {
    case "STOPPED":
      return "중지";
    case "RUNNING":
      return "실행";
    case "FAILED":
      return "실패";
    case "WAITING":
    default:
      return "대기";
  }
}

function formatCount(value?: number | null) {
  return value == null ? "0" : value.toLocaleString("ko-KR");
}

function waitingCount(node: NifiProcessGroupTreeNode) {
  return Math.max(
    (node.processorCount ?? 0) - (node.runningCount ?? 0) - (node.invalidCount ?? 0) - (node.stoppedCount ?? 0),
    0,
  );
}

function localDateString(date: Date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

const ADDED_FILE_STORAGE_PREFIX = "nifi-added-files";

interface StoredAddedFiles {
  date: string;
  files: string[];
}

function addedFileStorageKey(jobId: string | number | null | undefined, inputDirectory: string | null | undefined) {
  if (!jobId || !inputDirectory) {
    return null;
  }
  return `${ADDED_FILE_STORAGE_PREFIX}:${jobId}:${inputDirectory}`;
}

function loadTodayAddedFiles(storageKey: string | null) {
  if (!storageKey || typeof window === "undefined") {
    return [];
  }
  try {
    const rawValue = window.localStorage.getItem(storageKey);
    if (!rawValue) {
      return [];
    }
    const parsed = JSON.parse(rawValue) as Partial<StoredAddedFiles>;
    if (parsed.date !== localDateString(new Date()) || !Array.isArray(parsed.files)) {
      window.localStorage.removeItem(storageKey);
      return [];
    }
    return parsed.files.filter((fileName): fileName is string => typeof fileName === "string" && fileName.length > 0);
  } catch {
    return [];
  }
}

function saveTodayAddedFiles(storageKey: string | null, files: string[]) {
  if (!storageKey || typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(
      storageKey,
      JSON.stringify({
        date: localDateString(new Date()),
        files,
      } satisfies StoredAddedFiles),
    );
  } catch {
    // 목록 표시 보조 기능이라 저장 실패는 업로드 성공 흐름을 막지 않는다.
  }
}

function shiftedDate(days: number) {
  const date = new Date();
  date.setDate(date.getDate() + days);
  return date;
}

function logRowDateKey(value?: string | null) {
  if (!value) {
    return "";
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value.slice(0, 10) : localDateString(parsed);
}

function detailLogKeyword(job: EtlJobResponse | null, node: NifiProcessGroupTreeNode | null) {
  return job?.jobName ?? node?.name ?? "";
}

function executionLogGroupName(row: NifiExecutionLogEntry) {
  return row.groupName ?? row.rootGroupName ?? "";
}

function matchesRunLogKeyword(row: NifiProcessorRun, keyword: string) {
  const needle = keyword.trim().toLowerCase();
  if (!needle) {
    return true;
  }
  return [row.processorName, row.groupName, row.targetTable, row.processorType].some((text) =>
    (text ?? "").toLowerCase().includes(needle),
  );
}

function matchesExecutionLogKeyword(row: NifiExecutionLogEntry, keyword: string) {
  const needle = keyword.trim().toLowerCase();
  if (!needle) {
    return true;
  }
  return [row.processorName, executionLogGroupName(row), row.jobName, row.status, row.level, row.message].some((text) =>
    (text ?? "").toLowerCase().includes(needle),
  );
}

interface DetailLogSummary {
  key: string;
  label: string;
  from: string;
  to: string;
  processedCount: number;
  errorCount: number;
}

function recentDetailLogPeriods() {
  const today = localDateString(new Date());
  const yesterday = localDateString(shiftedDate(-1));
  const weekStart = localDateString(shiftedDate(-6));
  return {
    queryFrom: weekStart,
    queryTo: today,
    periods: [
      { key: "today", label: "오늘", from: today, to: today },
      { key: "yesterday", label: "어제", from: yesterday, to: yesterday },
      { key: "week", label: "최근 일주일", from: weekStart, to: today },
    ],
  };
}

function buildDetailLogSummaries(
  keyword: string,
  runRows: NifiProcessorRun[],
  eventRows: NifiExecutionLogEntry[],
): DetailLogSummary[] {
  const { periods } = recentDetailLogPeriods();
  const matchedRuns = runRows.filter((row) => matchesRunLogKeyword(row, keyword));
  const matchedEvents = eventRows
    .filter((row) => row.status === "FAILED" && row.level !== "WARNING")
    .filter((row) => matchesExecutionLogKeyword(row, keyword));
  return periods.map((period) => ({
    ...period,
    processedCount: matchedRuns
      .filter((row) => {
        const dateKey = logRowDateKey(row.startedAt);
        return dateKey >= period.from && dateKey <= period.to;
      })
      .reduce((sum, row) => sum + row.insertedCount, 0),
    errorCount: matchedEvents
      .filter((row) => {
        const dateKey = logRowDateKey(row.occurredAt);
        return dateKey >= period.from && dateKey <= period.to;
      })
      .length,
  }));
}

function detailLogUrl(item: DetailLogSummary, tab: "runs" | "events", keyword: string) {
  return `/etl/logs?tab=${tab}&from=${item.from}&to=${item.to}&q=${encodeURIComponent(keyword)}`;
}

function DetailLogRows({
  loading,
  summaries,
  keyword,
  onNavigate,
}: {
  loading: boolean;
  summaries: DetailLogSummary[];
  keyword: string;
  onNavigate: (url: string) => void;
}) {
  if (loading) {
    return <div className="nifi-detail-empty">불러오는 중</div>;
  }
  if (!summaries.length) {
    return <div className="nifi-detail-empty">-</div>;
  }
  return (
    <div className="nifi-detail-alert-list">
      {summaries.map((item) => (
        <div key={item.key} className="nifi-detail-alert-row">
          <span>{item.label}</span>
          <span>
            처리{" "}
            <button
              type="button"
              className="nifi-detail-link"
              onClick={() => onNavigate(detailLogUrl(item, "runs", keyword))}
            >
              {formatCount(item.processedCount)}
            </button>
            건 · 에러{" "}
            <button
              type="button"
              className="nifi-detail-link"
              onClick={() => onNavigate(detailLogUrl(item, "events", keyword))}
            >
              {formatCount(item.errorCount)}
            </button>
            건
          </span>
        </div>
      ))}
    </div>
  );
}

function jobStatusClass(status: NifiProcessGroupTreeNode["jobStatus"]) {
  switch (status) {
    case "STOPPED":
      return "stopped";
    case "RUNNING":
      return "running";
    case "FAILED":
      return "error";
    case "WAITING":
    default:
      return "done";
  }
}

function uniqueValues(values: Array<string | null | undefined>) {
  return Array.from(new Set(values.map((value) => value?.trim()).filter((value): value is string => !!value)));
}

function collectTreeIds(node: NifiProcessGroupTreeNode | null, ids = new Set<string>()) {
  if (!node) {
    return ids;
  }
  ids.add(node.id);
  node.children.forEach((child) => collectTreeIds(child, ids));
  return ids;
}

function displayValue(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (typeof value === "boolean") {
    return value ? "예" : "아니오";
  }
  if (Array.isArray(value)) {
    return value.length ? value.join(", ") : "-";
  }
  return String(value);
}

function directParentName(path?: NifiProcessGroupTreeNode[]) {
  if (!path || path.length < 2) {
    return "-";
  }
  return path[path.length - 2]?.name ?? "-";
}

function sortProcessorsByPosition(steps: EtlJobStepView[]) {
  return [...steps].sort((a, b) => {
    const ax = a.xPos ?? Number.MAX_SAFE_INTEGER;
    const bx = b.xPos ?? Number.MAX_SAFE_INTEGER;
    if (ax !== bx) {
      return ax - bx;
    }
    const ay = a.yPos ?? Number.MAX_SAFE_INTEGER;
    const by = b.yPos ?? Number.MAX_SAFE_INTEGER;
    return ay - by;
  });
}

function firstStartProcessor(steps: EtlJobStepView[], links: EtlJobLinkView[]) {
  const processorIds = new Set(steps.map((step) => step.nifiProcessorId));
  const incomingIds = new Set(
    links
      .map((link) => link.toComponentId)
      .filter((id): id is string => !!id && processorIds.has(id)),
  );
  return sortProcessorsByPosition(steps.filter((step) => !incomingIds.has(step.nifiProcessorId)))[0]
    ?? sortProcessorsByPosition(steps)[0]
    ?? null;
}

function lastTerminalProcessor(steps: EtlJobStepView[], links: EtlJobLinkView[]) {
  const processorIds = new Set(steps.map((step) => step.nifiProcessorId));
  const outgoingIds = new Set(
    links
      .map((link) => link.fromComponentId)
      .filter((id): id is string => !!id && processorIds.has(id)),
  );
  return sortProcessorsByPosition(steps.filter((step) => !outgoingIds.has(step.nifiProcessorId))).at(-1)
    ?? sortProcessorsByPosition(steps).at(-1)
    ?? null;
}

function stepProperties(step?: EtlJobStepView | null): Record<string, string> {
  if (!step?.propsJson) {
    return {};
  }
  try {
    const parsed = JSON.parse(step.propsJson);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? Object.fromEntries(Object.entries(parsed).map(([key, value]) => [key, String(value ?? "")]))
      : {};
  } catch {
    return {};
  }
}

function pickProperty(props: Record<string, string>, keys: string[]) {
  for (const key of keys) {
    const exactValue = props[key]?.trim();
    const matchedKey = Object.keys(props).find((propKey) => propKey.toLowerCase() === key.toLowerCase());
    const value = exactValue || (matchedKey ? props[matchedKey]?.trim() : "");
    if (value) {
      return value;
    }
  }
  return null;
}

function processorTypeIncludes(step: EtlJobStepView, typeName: string) {
  return (step.stepType ?? "").toLowerCase().includes(typeName.toLowerCase());
}

function listFileInputDirectory(step?: EtlJobStepView | null) {
  if (!step || !processorTypeIncludes(step, "ListFile")) {
    return null;
  }
  return pickProperty(stepProperties(step), ["Input Directory", "input-directory"]);
}

function sqlTableName(rawName: string) {
  const cleaned = rawName
    .replace(/[`"\[\]]/g, "")
    .trim();
  if (!cleaned || cleaned.startsWith("(")) {
    return null;
  }
  const parts = cleaned.split(".").map((part) => part.trim()).filter(Boolean);
  return parts.at(-1) ?? null;
}

function sqlSourceTables(sql?: string | null) {
  if (!sql?.trim()) {
    return [];
  }
  const normalized = sql
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/--.*$/gm, " ")
    .replace(/\s+/g, " ");
  const tables: string[] = [];
  const seen = new Set<string>();
  const clausePattern = /\b(from|join)\s+(.+?)(?=\s+\b(?:inner|left|right|full|cross|join|where|on|group|order|having|union|limit|offset|fetch)\b|$)/gi;
  let match: RegExpExecArray | null;
  while ((match = clausePattern.exec(normalized)) !== null) {
    const fragment = match[2] ?? "";
    for (const part of fragment.split(",")) {
      const token = part.trim().split(/\s+/)[0] ?? "";
      const table = sqlTableName(token);
      if (table && !seen.has(table)) {
        seen.add(table);
        tables.push(table);
      }
    }
  }
  return tables;
}

function sourceTargetText(step: EtlJobStepView | null, kind: "source" | "target") {
  if (!step) {
    return "-";
  }
  const props = stepProperties(step);
  const databaseType = pickProperty(props, ["Database Type", "db-type"]);
  if (kind === "source") {
    const sql = pickProperty(props, ["SQL select query"]) ?? step.sqlText;
    const tables = sqlSourceTables(sql);
    const tableText = tables.length > 0
      ? tables.join(", ")
      : pickProperty(props, ["Table Name", "TABLE NAME", "table-name"]);
    return [databaseType, tableText].filter(Boolean).join(" / ") || "-";
  }
  const schema = pickProperty(props, ["SCHEMA NAME", "Schema Name", "put-db-record-schema-name"]);
  const table = pickProperty(props, ["TABLE NAME", "Table Name", "put-db-record-table-name"]);
  return [databaseType, schema, table].filter(Boolean).join(" / ") || "-";
}

function shortProcessorType(type?: string) {
  if (!type) {
    return "-";
  }
  const lastDot = type.lastIndexOf(".");
  return lastDot < 0 ? type : type.slice(lastDot + 1);
}

function processorStateClass(processor: NifiProcessorDetailResponse | null) {
  const runStatus = processor?.status?.aggregateSnapshot?.runStatus ?? processor?.status?.runStatus ?? processor?.component?.state;
  const validationStatus =
    processor?.status?.aggregateSnapshot?.validationStatus ??
    processor?.status?.validationStatus ??
    processor?.component?.validationStatus;
  if (validationStatus?.toUpperCase() === "INVALID") {
    return "error";
  }
  if (runStatus?.toUpperCase() === "RUNNING") {
    return "running";
  }
  return "done";
}

function processorStateText(processor: NifiProcessorDetailResponse | null) {
  const runStatus = processor?.status?.aggregateSnapshot?.runStatus ?? processor?.status?.runStatus ?? processor?.component?.state;
  const validationStatus =
    processor?.status?.aggregateSnapshot?.validationStatus ??
    processor?.status?.validationStatus ??
    processor?.component?.validationStatus;
  if (validationStatus?.toUpperCase() === "INVALID") {
    return "실패";
  }
  if (runStatus?.toUpperCase() === "RUNNING") {
    return "실행중";
  }
  if (runStatus) {
    return runStatus;
  }
  return "완료";
}

function propertyRows(processor: NifiProcessorDetailResponse | null) {
  const properties = processor?.component?.config?.properties ?? {};
  const descriptors =
    processor?.component?.config?.descriptors ??
    processor?.component?.descriptors ??
    processor?.component?.propertyDescriptors ??
    {};
  return Object.entries(properties)
    .map(([key, value]) => ({
      key,
      label: descriptors[key]?.displayName || descriptors[key]?.name || key,
      value,
      sensitive: descriptors[key]?.sensitive,
    }))
    .sort((a, b) => a.label.localeCompare(b.label));
}

function newLockOwnerToken() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

interface ProcessorDetailPanelProps {
  processorId: string;
  onParentGroupFound?: (groupId: string) => void;
}

function ProcessorDetailPanel({ processorId, onParentGroupFound }: ProcessorDetailPanelProps) {
  const navigate = useNavigate();
  const [processor, setProcessor] = useState<NifiProcessorDetailResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [editLock, setEditLock] = useState<NifiProcessorEditLockResponse | null>(null);
  const [lockError, setLockError] = useState<string | null>(null);
  const [logSummaries, setLogSummaries] = useState<DetailLogSummary[]>([]);
  const [logsLoading, setLogsLoading] = useState(false);
  const ownerTokenRef = useRef(newLockOwnerToken());
  const processorNameRef = useRef<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    setError(null);
    setProcessor(null);

    getNifiProcessor(processorId)
      .then((result) => {
        if (cancelled) {
          return;
        }
        setProcessor(result);
        if (result.component?.parentGroupId) {
          onParentGroupFound?.(result.component.parentGroupId);
        }
      })
      .catch((ex) => {
        if (!cancelled) {
          setError(ex instanceof Error ? ex.message : "프로세서 상세를 불러오지 못했습니다.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setIsLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [onParentGroupFound, processorId]);

  useEffect(() => {
    processorNameRef.current = processor?.component?.name ?? null;
  }, [processor?.component?.name]);

  useEffect(() => {
    ownerTokenRef.current = newLockOwnerToken();
    processorNameRef.current = null;
    setEditLock(null);
    setLockError(null);

    let cancelled = false;
    let intervalId: number | undefined;
    const ownerToken = ownerTokenRef.current;
    const request = () => ({
      ownerToken,
      processorName: processorNameRef.current,
    });

    acquireNifiProcessorEditLock(processorId, request())
      .then((result) => {
        if (!cancelled) {
          setEditLock(result);
        }
      })
      .catch((ex) => {
        if (!cancelled) {
          setLockError(ex instanceof Error ? ex.message : "편집 잠금을 확인하지 못했습니다.");
        }
      });

    intervalId = window.setInterval(() => {
      heartbeatNifiProcessorEditLock(processorId, request())
        .then((result) => {
          if (!cancelled) {
            setEditLock(result);
            setLockError(null);
          }
        })
        .catch((ex) => {
          if (!cancelled) {
            setLockError(ex instanceof Error ? ex.message : "편집 잠금 갱신에 실패했습니다.");
          }
        });
    }, NIFI_PROCESSOR_LOCK_HEARTBEAT_MS);

    return () => {
      cancelled = true;
      if (intervalId) {
        window.clearInterval(intervalId);
      }
      void releaseNifiProcessorEditLock(processorId, { ownerToken });
    };
  }, [processorId]);

  const component = processor?.component;
  const config = component?.config;
  const bundle = component?.bundle;
  const rows = propertyRows(processor);
  const relationships = component?.relationships ?? [];
  const autoTerminated = component?.autoTerminatedRelationships ?? config?.autoTerminatedRelationships ?? [];
  const activeThreadCount = processor?.status?.aggregateSnapshot?.activeThreadCount ?? processor?.status?.activeThreadCount;
  const isReadOnlyByLock = Boolean(editLock && !editLock.heldByMe);
  const logKeyword = component?.name ?? "";

  useEffect(() => {
    let cancelled = false;
    setLogSummaries([]);
    if (!logKeyword) {
      setLogsLoading(false);
      return () => {
        cancelled = true;
      };
    }
    const { queryFrom, queryTo } = recentDetailLogPeriods();
    setLogsLoading(true);
    Promise.all([
      listNifiProcessorRuns(queryFrom, queryTo),
      listNifiExecutionLogs(queryFrom, queryTo),
    ])
      .then(([runRows, eventRows]) => {
        if (!cancelled) {
          setLogSummaries(buildDetailLogSummaries(logKeyword, runRows, eventRows));
        }
      })
      .catch(() => {
        if (!cancelled) {
          setLogSummaries([]);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLogsLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [logKeyword]);

  return (
    <aside className="nifi-detail-panel" aria-label="선택한 프로세서 상세">
      <div className="nifi-detail-header">
        <div className="nifi-detail-title" title={component?.name ?? "프로세서"}>
          {component?.name ?? "프로세서"}
        </div>
        <div className={`nifi-detail-status ${processorStateClass(processor)}`}>
          <span className="nifi-detail-dot" aria-hidden="true" />
          <span>{processorStateText(processor)}</span>
          <span>{shortProcessorType(component?.type)}</span>
        </div>
      </div>

      {isLoading ? <div className="nifi-detail-message">불러오는 중</div> : null}
      {!isLoading && error ? <div className="nifi-detail-message error">조회 실패</div> : null}
      {editLock ? (
        <Alert
          className="nifi-edit-lock-alert"
          type={isReadOnlyByLock ? "warning" : "success"}
          showIcon
          message={isReadOnlyByLock ? "읽기 전용" : "편집 가능"}
          description={
            isReadOnlyByLock
              ? `${editLock.lockedByUserNm}(${editLock.lockedByUserId}) 사용자가 이 Processor 편집 잠금을 보유 중입니다.`
              : "이 브라우저가 Processor 편집 잠금을 보유 중입니다."
          }
        />
      ) : null}
      {lockError ? (
        <Alert
          className="nifi-edit-lock-alert"
          type="error"
          showIcon
          message="편집 잠금 확인 실패"
          description={lockError}
        />
      ) : null}

      <section className="nifi-detail-section">
        <h3>기본 정보</h3>
        <dl>
          <div>
            <dt>대상 유형</dt>
            <dd>Processor</dd>
          </div>
          <div>
            <dt>ID</dt>
            <dd>{component?.id ?? processor?.id ?? processorId}</dd>
          </div>
          <div>
            <dt>상위 그룹</dt>
            <dd>{component?.parentGroupId ?? "-"}</dd>
          </div>
          <div>
            <dt>타입</dt>
            <dd title={component?.type}>{shortProcessorType(component?.type)}</dd>
          </div>
          <div>
            <dt>검증</dt>
            <dd>{component?.validationStatus ?? processor?.status?.validationStatus ?? "-"}</dd>
          </div>
          <div>
            <dt>설명</dt>
            <dd>{component?.comments ?? config?.comments ?? "-"}</dd>
          </div>
        </dl>
      </section>

      <section className="nifi-detail-section">
        <h3>실행</h3>
        <dl>
          <div>
            <dt>상태</dt>
            <dd>{processor?.status?.aggregateSnapshot?.runStatus ?? processor?.status?.runStatus ?? component?.state ?? "-"}</dd>
          </div>
          <div>
            <dt>스케줄</dt>
            <dd>{config?.schedulingStrategy ?? "-"} / {config?.schedulingPeriod ?? "-"}</dd>
          </div>
          <div>
            <dt>태스크</dt>
            <dd>{displayValue(config?.concurrentlySchedulableTaskCount)}</dd>
          </div>
          <div>
            <dt>스레드</dt>
            <dd>{displayValue(activeThreadCount)}</dd>
          </div>
          <div>
            <dt>실행 노드</dt>
            <dd>{config?.executionNode ?? "-"}</dd>
          </div>
          <div>
            <dt>패널티</dt>
            <dd>{config?.penaltyDuration ?? "-"}</dd>
          </div>
          <div>
            <dt>Yield</dt>
            <dd>{config?.yieldDuration ?? "-"}</dd>
          </div>
          <div>
            <dt>Bulletin</dt>
            <dd>{config?.bulletinLevel ?? "-"}</dd>
          </div>
        </dl>
      </section>

      <section className="nifi-detail-section">
        <h3>번들 / 위치</h3>
        <dl>
          <div>
            <dt>그룹</dt>
            <dd>{component?.bundleGroup ?? bundle?.group ?? "-"}</dd>
          </div>
          <div>
            <dt>아티팩트</dt>
            <dd>{component?.bundleArtifact ?? bundle?.artifact ?? "-"}</dd>
          </div>
          <div>
            <dt>버전</dt>
            <dd>{component?.bundleVersion ?? bundle?.version ?? "-"}</dd>
          </div>
          <div>
            <dt>좌표</dt>
            <dd>
              {component?.position ? `${component.position.x ?? "-"}, ${component.position.y ?? "-"}` : "-"}
            </dd>
          </div>
          <div>
            <dt>Revision</dt>
            <dd>{displayValue(processor?.revision?.version)}</dd>
          </div>
          <div>
            <dt>권한</dt>
            <dd>
              읽기 {displayValue(processor?.permissions?.canRead)} / 쓰기{" "}
              {displayValue(processor?.permissions?.canWrite)}
            </dd>
          </div>
        </dl>
      </section>

      <section className="nifi-detail-section">
        <h3>관계</h3>
        {relationships.length || autoTerminated.length ? (
          <dl>
            {relationships.map((relationship) => (
              <div key={relationship.name ?? relationship.description}>
                <dt>{relationship.name ?? "-"}</dt>
                <dd>{relationship.autoTerminate ? "자동 종료" : relationship.description ?? "-"}</dd>
              </div>
            ))}
            {autoTerminated.length ? (
              <div>
                <dt>자동 종료</dt>
                <dd>{autoTerminated.join(", ")}</dd>
              </div>
            ) : null}
          </dl>
        ) : (
          <div className="nifi-detail-empty">-</div>
        )}
      </section>

      <section className="nifi-detail-section">
        <h3>변수</h3>
        {rows.length ? (
          <dl className="nifi-detail-properties">
            {rows.map((row) => (
              <div key={row.key}>
                <dt title={row.key}>{row.label}</dt>
                <dd>{row.sensitive ? "********" : displayValue(row.value)}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <div className="nifi-detail-empty">-</div>
        )}
      </section>

      <section className="nifi-detail-section">
        <h3>로그</h3>
        <DetailLogRows
          loading={logsLoading}
          summaries={logSummaries}
          keyword={logKeyword}
          onNavigate={navigate}
        />
      </section>
    </aside>
  );
}

interface ProcessGroupDetailPanelProps {
  activeGroupId: string | null;
  tree: NifiProcessGroupTreeNode | null;
  reloadKey: number;
}

/**
 * 워크플로우 설계 캔버스의 노드 패널이 쓰는 job 요약(마지막 실행·상위 경로·작성자·연결 DAG).
 *
 * <p>«대상»(소스/타깃)과 «로그» 구역은 2026-09-03 에 뺐다 - 캔버스에서는 배치와 연결이
 * 관심사라 그 두 구역이 패널만 길게 만들었고, 로그 조회 때문에 노드를 누를 때마다
 * NiFi 실행이력 두 벌을 더 불러오고 있었다. 같은 내용은 ETL 관리 화면의 상세에서 본다.
 */
export function EtlJobSummary({ jobId, nifiPgId }: { jobId: number; nifiPgId?: string | null }) {
  const navigate = useNavigate();
  const [detail, setDetail] = useState<EtlJobDetailResponse | null>(null);
  const [runs, setRuns] = useState<EtlJobRunResponse[]>([]);
  const [group, setGroup] = useState<ReturnType<typeof findTreeNode>>(null);
  const [loading, setLoading] = useState(true);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setFailed(false);
    setDetail(null);
    setRuns([]);
    Promise.all([getEtlJob(jobId), getEtlJobRuns(jobId)])
      .then(([jobDetail, jobRuns]) => {
        if (!cancelled) {
          setDetail(jobDetail);
          setRuns(jobRuns);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setFailed(true);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => { cancelled = true; };
  }, [jobId]);

  // 상위 경로와 작성자는 job 응답에 없고 NiFi 프로세스 그룹 트리에만 있다
  // (job.parentGroupName 은 비어 있는 경우가 있어 ETL 관리 화면도 트리 경로를 쓴다).
  // 실패해도 나머지는 보여준다.
  useEffect(() => {
    let cancelled = false;
    setGroup(null);
    if (!nifiPgId) {
      return () => { cancelled = true; };
    }
    getNifiProcessGroupTree()
      .then((tree) => {
        if (!cancelled) {
          setGroup(findTreeNode(tree, nifiPgId));
        }
      })
      .catch(() => undefined);
    return () => { cancelled = true; };
  }, [nifiPgId]);

  const job = detail?.job ?? null;
  const groupNode = group?.node ?? null;

  if (loading) {
    return <div className="nifi-detail-message">불러오는 중</div>;
  }
  if (failed || !job) {
    return <div className="nifi-detail-message error">조회 실패</div>;
  }

  const latestRun = runs[0] ?? null;
  const lastRunTime = formatDateTime(latestRun?.endedAt ?? latestRun?.startedAt ?? job.lastSyncedAt);
  const lastRunCount = formatCount(latestRun?.totalInserted ?? 0);
  const dagId = job.airflowDagId ?? null;

  return (
    <div className="etl-job-summary">
      <div className="nifi-detail-status stacked">
        <span>마지막 실행 {lastRunTime} · {lastRunCount}건</span>
      </div>
      <dl className="nifi-detail-header-info">
        <div>
          <dt>상위 경로</dt>
          <dd>{directParentName(group?.path)}</dd>
        </div>
        <div>
          <dt>작성자</dt>
          <dd>{groupNode?.createdBy ?? "-"}</dd>
        </div>
        <div>
          <dt>연결 DAG</dt>
          <dd>
            {dagId ? (
              <>
                {dagId}{" "}
                <button
                  type="button"
                  className="nifi-detail-link"
                  onClick={() => navigate(`/airflow/dashboard?dagId=${encodeURIComponent(dagId)}&detail=1`)}
                >
                  [열기]
                </button>
              </>
            ) : (
              "-"
            )}
          </dd>
        </div>
      </dl>
    </div>
  );
}

function ProcessGroupDetailPanel({ activeGroupId, tree, reloadKey }: ProcessGroupDetailPanelProps) {
  const navigate = useNavigate();
  const [jobs, setJobs] = useState<EtlJobResponse[]>([]);
  const [details, setDetails] = useState<EtlJobDetailResponse[]>([]);
  const [runs, setRuns] = useState<EtlJobRunResponse[]>([]);
  const [logSummaries, setLogSummaries] = useState<DetailLogSummary[]>([]);
  const [logsLoading, setLogsLoading] = useState(false);
  const [jobsLoading, setJobsLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [isAddingFiles, setIsAddingFiles] = useState(false);
  const [addedFiles, setAddedFiles] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const addFileInputRef = useRef<HTMLInputElement | null>(null);

  const selected = useMemo(() => findTreeNode(tree, activeGroupId), [activeGroupId, tree]);
  const selectedGroupId = activeGroupId ?? "root";
  const job = jobs.find((entry) => entry.nifiPgId === selectedGroupId) ?? null;
  const selectedGroupIds = useMemo(() => {
    const ids = collectTreeIds(selected?.node ?? null);
    if (activeGroupId) {
      ids.add(activeGroupId);
    }
    return ids;
  }, [activeGroupId, selected?.node]);
  const groupJobs = jobs.filter((entry) => selectedGroupIds.has(entry.nifiPgId));
  const groupJobIdsKey = groupJobs.map((entry) => entry.id).join(",");
  const primaryJob = job ?? groupJobs[0] ?? null;
  const latestRun = runs[0] ?? null;
  const airflowDags = uniqueValues(groupJobs.map((entry) => entry.airflowDagId));
  const isLoading = jobsLoading || detailLoading;

  useEffect(() => {
    let cancelled = false;
    setJobsLoading(true);
    setError(null);

    listEtlJobs()
      .then((result) => {
        if (cancelled) {
          return;
        }
        setJobs(result);
      })
      .catch((ex) => {
        if (!cancelled) {
          setError(ex instanceof Error ? ex.message : "ETL Job 정보를 불러오지 못했습니다.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setJobsLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [reloadKey]);

  useEffect(() => {
    let cancelled = false;
    setDetails([]);
    setRuns([]);
    setError(null);

    if (groupJobs.length === 0) {
      setDetailLoading(false);
      return () => {
        cancelled = true;
      };
    }

    setDetailLoading(true);
    Promise.all([
      Promise.all(groupJobs.map((entry) => getEtlJob(entry.id))),
      primaryJob ? getEtlJobRuns(primaryJob.id) : Promise.resolve([] as EtlJobRunResponse[]),
    ])
      .then(([jobDetails, jobRuns]) => {
        if (cancelled) {
          return;
        }
        setDetails(jobDetails);
        setRuns(jobRuns);
      })
      .catch((ex) => {
        if (!cancelled) {
          setError(ex instanceof Error ? ex.message : "ETL Job 상세를 불러오지 못했습니다.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setDetailLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [groupJobIdsKey, primaryJob?.id]);

  const displayJob = details.find((entry) => entry.job.nifiPgId === selectedGroupId)?.job ?? job;
  const relatedJob = displayJob ?? primaryJob;
  const displayNode = selected?.node ?? null;
  const isJobGroup = displayNode?.groupType === "JOB";
  const isGroupingGroup = displayNode?.groupType === "GROUPING";
  const directParent = directParentName(selected?.path);
  const author = displayNode?.createdBy ?? "-";
  const displayDetail = details.find((entry) => entry.job.nifiPgId === selectedGroupId)
    ?? (displayJob ? details.find((entry) => entry.job.id === displayJob.id) : null)
    ?? null;
  const jobSteps = displayDetail?.steps ?? [];
  const jobLinks = displayDetail?.links ?? [];
  const firstStep = firstStartProcessor(jobSteps, jobLinks);
  const sourceStep = firstStep;
  const targetStep = lastTerminalProcessor(jobSteps, jobLinks);
  const listFileStep = jobSteps.find((step) => processorTypeIncludes(step, "ListFile")) ?? null;
  const inputDirectory = listFileInputDirectory(listFileStep);
  const sourceText = sourceTargetText(sourceStep, "source");
  const targetText = sourceTargetText(targetStep, "target");
  const dagId = relatedJob?.airflowDagId ?? airflowDags[0] ?? null;
  const directGroupingGroups = isGroupingGroup
    ? (displayNode?.children ?? []).filter((entry) => entry.groupType === "GROUPING")
    : [];
  const directJobGroups = isGroupingGroup
    ? (displayNode?.children ?? []).filter((entry) => entry.groupType === "JOB")
    : [];
  const hasDirectJobStatus = directGroupingGroups.length > 0 || directJobGroups.length > 0;
  const showHeaderStatus = isJobGroup;
  const lastRunTime = formatDateTime(latestRun?.endedAt ?? latestRun?.startedAt ?? relatedJob?.lastSyncedAt);
  const lastRunCount = formatCount(latestRun?.totalInserted ?? 0);
  const logKeyword = detailLogKeyword(displayJob, displayNode);
  const addedFilesStorageKey = addedFileStorageKey(displayJob?.id, inputDirectory);
  const titleName = displayJob?.jobName ?? displayNode?.name ?? "선택 없음";
  const headerTitle = isJobGroup
    ? `Job 명: ${titleName}`
    : isGroupingGroup
      ? `그룹명 : ${titleName}`
      : titleName;

  useEffect(() => {
    let cancelled = false;
    setLogSummaries([]);
    if (!isJobGroup || !displayJob) {
      setLogsLoading(false);
      return () => {
        cancelled = true;
      };
    }
    const { queryFrom, queryTo } = recentDetailLogPeriods();

    setLogsLoading(true);
    Promise.all([
      listNifiProcessorRuns(queryFrom, queryTo),
      listNifiExecutionLogs(queryFrom, queryTo),
    ])
      .then(([runRows, eventRows]) => {
        if (cancelled) {
          return;
        }
        setLogSummaries(buildDetailLogSummaries(logKeyword, runRows, eventRows));
      })
      .catch(() => {
        if (!cancelled) {
          setLogSummaries([]);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLogsLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [displayJob?.id, isJobGroup, logKeyword]);

  useEffect(() => {
    setAddedFiles(loadTodayAddedFiles(addedFilesStorageKey));
  }, [addedFilesStorageKey]);

  const handleAddFiles = async (event: ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.currentTarget.files ?? []);
    event.currentTarget.value = "";
    if (!inputDirectory || files.length === 0) {
      return;
    }
    setIsAddingFiles(true);
    try {
      const result = await uploadNifiInputDirectoryFiles(inputDirectory, files);
      setAddedFiles((prev) => {
        const next = [...prev, ...result.storedFiles];
        saveTodayAddedFiles(addedFilesStorageKey, next);
        return next;
      });
      message.success(`${result.storedFiles.length}개 파일을 추가했습니다.`);
    } catch (ex) {
      message.error(ex instanceof Error ? ex.message : "파일 추가에 실패했습니다.");
    } finally {
      setIsAddingFiles(false);
    }
  };

  return (
    <aside className="nifi-detail-panel" aria-label="선택한 프로세스 그룹 상세">
      <div className="nifi-detail-header">
        <div className="nifi-detail-title" title={headerTitle}>
          {headerTitle}
        </div>
        {showHeaderStatus ? (
          <div className={`nifi-detail-status stacked ${statusClass(displayJob, displayNode)}`}>
            <div>
              <span className="nifi-detail-dot" aria-hidden="true" />
              <span>{statusText(displayJob, displayNode)}</span>
            </div>
            <span>마지막 실행 {lastRunTime} · {lastRunCount}건</span>
          </div>
        ) : null}
        <dl className="nifi-detail-header-info">
          <div>
            <dt>상위 경로</dt>
            <dd>{directParent}</dd>
          </div>
          <div>
            <dt>작성자</dt>
            <dd>{author}</dd>
          </div>
          {isJobGroup ? (
            <>
              <div>
                <dt>연결 DAG</dt>
                <dd>
                  {dagId ? (
                    <>
                      {dagId}{" "}
                      <button
                        type="button"
                        className="nifi-detail-link"
                        onClick={() => navigate(`/airflow/dashboard?dagId=${encodeURIComponent(dagId)}&detail=1`)}
                      >
                        [열기]
                      </button>
                    </>
                  ) : (
                    "-"
                  )}
                </dd>
              </div>
            </>
          ) : null}
          {isGroupingGroup ? (
            <div>
              <dt>설명</dt>
              <dd className="nifi-detail-preline">{displayNode?.comments || "-"}</dd>
            </div>
          ) : null}
        </dl>
      </div>

      {isLoading ? <div className="nifi-detail-message">불러오는 중</div> : null}
      {!isLoading && error ? <div className="nifi-detail-message error">조회 실패</div> : null}

      {hasDirectJobStatus ? (
        <section className="nifi-detail-section">
          <h3>JOB 상태</h3>
          <div className="nifi-direct-status-list">
            {directGroupingGroups.length > 0 ? (
              <div className="nifi-grouping-status-table">
                <div className="nifi-grouping-status-row heading">
                  <span />
                  <span>중지</span>
                  <span>대기</span>
                  <span>성공</span>
                  <span>실패</span>
                </div>
                {directGroupingGroups.map((entry) => (
                  <button
                    key={entry.id}
                    type="button"
                    className="nifi-grouping-status-row"
                    onClick={() => navigate(`/etl/manage?processGroupId=${encodeURIComponent(entry.id)}`)}
                  >
                    <span className="nifi-grouping-status-name" title={entry.name}>{entry.name}</span>
                    <span className="nifi-grouping-status-cell">{formatCount(entry.stoppedCount)}</span>
                    <span className="nifi-grouping-status-cell">{formatCount(waitingCount(entry))}</span>
                    <span className="nifi-grouping-status-cell">{formatCount(entry.runningCount)}</span>
                    <span className="nifi-grouping-status-cell">{formatCount(entry.invalidCount)}</span>
                  </button>
                ))}
              </div>
            ) : null}
            {directJobGroups.length > 0 ? (
              <div className="nifi-job-status-list">
                {directJobGroups.map((entry) => (
                  <button
                    key={entry.id}
                    type="button"
                    className="nifi-job-status-row"
                    onClick={() => navigate(`/etl/manage?processGroupId=${encodeURIComponent(entry.id)}`)}
                  >
                    <span className="nifi-job-status-name" title={entry.name}>{entry.name}</span>
                    <span className={`nifi-job-status-badge ${jobStatusClass(entry.jobStatus)}`}>
                      <span className="nifi-detail-dot" aria-hidden="true" />
                      {jobStatusText(entry.jobStatus)}
                    </span>
                  </button>
                ))}
              </div>
            ) : null}
          </div>
        </section>
      ) : null}

      {isJobGroup ? (
        <>
          <section className="nifi-detail-section">
            <h3>대상</h3>
            <dl className="nifi-detail-target-list">
              <div>
                <dt>소스</dt>
                <dd className="nifi-detail-inline-value" title={sourceText}>
                  {sourceText}
                </dd>
              </div>
              <div>
                <dt>타깃</dt>
                <dd className="nifi-detail-inline-value" title={targetText}>
                  {targetText}
                </dd>
              </div>
            </dl>
          </section>

          <section className="nifi-detail-section">
            <h3>로그</h3>
            <DetailLogRows
              loading={logsLoading}
              summaries={logSummaries}
              keyword={logKeyword}
              onNavigate={navigate}
            />
          </section>

          {inputDirectory ? (
            <section className="nifi-detail-section">
              <Button
                icon={<UploadOutlined />}
                loading={isAddingFiles}
                onClick={() => addFileInputRef.current?.click()}
                size="small"
              >
                파일 추가
              </Button>
              <input
                ref={addFileInputRef}
                hidden
                multiple
                type="file"
                onChange={handleAddFiles}
              />
              {addedFiles.length > 0 ? (
                <ul className="nifi-added-file-list">
                  {addedFiles.map((fileName, index) => (
                    <li key={`${fileName}-${index}`} title={fileName}>
                      추가된 파일 : {fileName}
                    </li>
                  ))}
                </ul>
              ) : null}
            </section>
          ) : null}
        </>
      ) : null}
    </aside>
  );
}

export function ConsoleFramePage({
  title,
  src,
  healthcheckSrc,
  waitMessage,
  showProcessGroupTree = false,
  nifiAutoOpenMenuTitles = [],
}: ConsoleFramePageProps) {
  const location = useLocation();
  const [isReady, setIsReady] = useState(!healthcheckSrc);
  const [frameKey, setFrameKey] = useState(0);
  const [processGroupTree, setProcessGroupTree] = useState<NifiProcessGroupTreeNode | null>(null);
  const processGroupId = new URLSearchParams(location.search).get("processGroupId");
  const [selectedProcessGroupId, setSelectedProcessGroupId] = useState<string | null>(processGroupId);
  const [selectedCanvasItem, setSelectedCanvasItem] = useState<CanvasSelection | null>(
    processGroupId ? { type: "processGroup", id: processGroupId } : null,
  );
  const [jobCatalogReloadKey, setJobCatalogReloadKey] = useState(0);
  const processGroupTreeRef = useRef<NifiProcessGroupTreeNode | null>(null);
  const processGroupIdRef = useRef<string | null>(null);
  const frameSrc = processGroupId ? withProcessGroupId(src, processGroupId) : src;
  processGroupTreeRef.current = processGroupTree;
  processGroupIdRef.current = processGroupId;

  useEffect(() => {
    setSelectedProcessGroupId(processGroupId);
    setSelectedCanvasItem(processGroupId ? { type: "processGroup", id: processGroupId } : null);
  }, [processGroupId]);

  useEffect(() => {
    if (!healthcheckSrc) {
      setIsReady(true);
      return;
    }

    let isCancelled = false;
    let timerId: number | undefined;

    const checkReady = async () => {
      try {
        const response = await fetch(healthcheckSrc, {
          cache: "no-store",
          credentials: "include",
        });

        if (!isCancelled && response.status < 500) {
          setIsReady(true);
          return;
        }
      } catch {
        if (!isCancelled) {
          setIsReady(false);
        }
      }

      if (!isCancelled) {
        timerId = window.setTimeout(checkReady, 5000);
      }
    };

    setIsReady(false);
    void checkReady();

    return () => {
      isCancelled = true;
      if (timerId) {
        window.clearTimeout(timerId);
      }
    };
  }, [healthcheckSrc]);

  const reloadFrame = () => {
    setFrameKey((key) => key + 1);
  };

  const handleCatalogSynced = useCallback(() => {
    setJobCatalogReloadKey((key) => key + 1);
  }, []);

  const selectCanvasItem = (selection: CanvasSelection) => {
    setSelectedCanvasItem(selection);
    if (selection.type === "processGroup") {
      setSelectedProcessGroupId(selection.id);
    }
  };

  const selectCanvasProcessGroupRoute = (groupId: string | null) => {
    const normalizedGroupId = groupId === "root" ? null : groupId;
    setSelectedProcessGroupId(normalizedGroupId);
    setSelectedCanvasItem(normalizedGroupId ? { type: "processGroup", id: normalizedGroupId } : null);
  };

  const handleFrameLoad = (event: SyntheticEvent<HTMLIFrameElement>) => {
    const frame = event.currentTarget;
    cleanupLegacyNifiMiniCreateToolbars(frame);
    hideToolChrome(frame);
    patchKoreanTooltips(frame);
    installNifiPanelLayout(frame);
    clickNifiMenuItem(frame, nifiAutoOpenMenuTitles);
    if (showProcessGroupTree) {
      installCanvasRouteSync(frame, selectCanvasProcessGroupRoute);
      installCanvasSelectionSync(
        frame,
        selectCanvasItem,
        (target) => resolveProcessGroupIdByCanvasText(target, processGroupIdRef.current, processGroupTreeRef.current),
      );
    }
  };

  return (
    <div className="console-page">
      {showProcessGroupTree ? (
        <ProcessGroupTreePanel
          activeGroupId={selectedProcessGroupId}
          onTreeChange={setProcessGroupTree}
          onCatalogSynced={handleCatalogSynced}
        />
      ) : null}
      <div className="console-frame-shell">
        {isReady ? (
          <iframe
            key={frameKey}
            className="console-frame"
            title={title}
            src={frameSrc}
            onLoad={handleFrameLoad}
          />
        ) : (
          <Result
            className="console-wait"
            icon={<Spin size="large" />}
            title={waitMessage ?? "관리 콘솔을 준비하는 중입니다"}
            extra={
              <Button icon={<ReloadOutlined />} onClick={reloadFrame}>
                다시 확인
              </Button>
            }
          />
        )}
      </div>
      {showProcessGroupTree ? (
        selectedCanvasItem?.type === "processor" ? (
          <ProcessorDetailPanel processorId={selectedCanvasItem.id} onParentGroupFound={setSelectedProcessGroupId} />
        ) : (
          <ProcessGroupDetailPanel
            activeGroupId={selectedProcessGroupId}
            tree={processGroupTree}
            reloadKey={jobCatalogReloadKey}
          />
        )
      ) : null}
    </div>
  );
}
