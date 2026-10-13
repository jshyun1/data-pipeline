import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type ChangeEvent,
  type ReactNode,
  type MouseEvent as ReactMouseEvent,
  type SyntheticEvent,
} from "react";
import { Alert, Button, Modal, Result, Spin, message } from "antd";
import {
  DoubleLeftOutlined,
  DoubleRightOutlined,
  FileOutlined,
  FolderOutlined,
  ReloadOutlined,
  UploadOutlined,
} from "@ant-design/icons";
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
import { getLlmStatus } from "../api/llm";
import { GptChatPanel } from "../components/gpt/GptChatPanel";
import { getWorkflow, listWorkflows, type WorkflowDetail } from "../api/workflows";

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
const NIFI_TOP_TOOLBAR_ATTRIBUTE = "data-cerebro-top-toolbar";
const NIFI_TOP_TOOLBAR_EXPANDED_ATTRIBUTE = "data-cerebro-top-toolbar-expanded";
const NIFI_RECENT_PANEL_ATTRIBUTE = "data-cerebro-recent-panel";
const NIFI_RECENT_PANEL_TOGGLE_ID = "cerebro-recent-panel-toggle";
const NIFI_TOOL_HIDDEN_ATTRIBUTE = "data-cerebro-tool-hidden";
const NIFI_TOOL_ALLOWED_ATTRIBUTE = "data-cerebro-tool-allowed";
const NIFI_TOOL_ICON_ATTRIBUTE = "data-cerebro-tool-icon";
const NIFI_TOOL_LABEL_ATTRIBUTE = "data-cerebro-tool-label";
const NIFI_MENU_BUTTON_ATTRIBUTE = "data-cerebro-menu-button";
const NIFI_BREADCRUMB_ATTRIBUTE = "data-cerebro-canvas-breadcrumb";
const NIFI_BREADCRUMB_CONTAINER_ATTRIBUTE = "data-cerebro-canvas-breadcrumb-container";
const PROCESSOR_DIALOG_VIEWPORT_STYLE_ID = "cerebro-processor-dialog-viewport";
const PROCESSOR_DIALOG_VIEWPORT_FIT_ATTRIBUTE = "data-cerebro-processor-dialog-viewport-fit";
const PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE = "data-cerebro-processor-dialog-scaled";
const PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE = "data-cerebro-processor-dialog-fit-target";
const NIFI_READ_ONLY_LOCK_ATTRIBUTE = "data-cerebro-readonly-lock";
const NIFI_READ_ONLY_LOCK_INSTALLED_ATTRIBUTE = "data-cerebro-readonly-lock-installed";
const NIFI_READ_ONLY_HOLDER_ATTRIBUTE = "data-cerebro-readonly-lock-holder";
const NIFI_READ_ONLY_NOTICE_ID = "cerebro-nifi-readonly-notice";
const NIFI_LOCK_DISABLED_BEFORE_ATTRIBUTE = "data-cerebro-lock-disabled-before";
const NIFI_LOCK_READONLY_BEFORE_ATTRIBUTE = "data-cerebro-lock-readonly-before";
const NIFI_LOCK_DIALOG_ATTRIBUTE = "data-cerebro-readonly-lock-dialog";
const NIFI_MINI_TOOLBAR_ID = "cerebro-nifi-mini-toolbar";
const LEGACY_NIFI_MINI_CREATE_TOOLBAR_CLEANUP_ATTRIBUTE = "data-cerebro-mini-create-toolbar-cleanup";
const NIFI_PROCESSOR_LOCK_HEARTBEAT_MS = 20_000;
const NIFI_DETAIL_PANEL_DEFAULT_WIDTH = 285;
const NIFI_DETAIL_PANEL_MIN_WIDTH = 235;

function clampDetailPanelWidth(width: number): number {
  const maxWidth = Math.max(NIFI_DETAIL_PANEL_MIN_WIDTH, Math.floor(window.innerWidth * 0.72));
  return Math.min(maxWidth, Math.max(NIFI_DETAIL_PANEL_MIN_WIDTH, width));
}

const PROCESSOR_DIALOG_VIEWPORT_CSS = `
  .cdk-global-overlay-wrapper:has(.processor-configuration),
  .cdk-global-overlay-wrapper:has([${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"]) {
    align-items: center !important;
    justify-content: center !important;
    box-sizing: border-box !important;
    padding: 16px !important;
  }
  .cdk-overlay-pane:has(.processor-configuration),
  .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] {
    width: min(1024px, calc(100vw - 32px)) !important;
    max-width: calc(100vw - 32px) !important;
    max-height: calc(100vh - 32px) !important;
  }
  .processor-configuration,
  .mat-dialog-container:has(.processor-configuration),
  .mat-mdc-dialog-container:has(.processor-configuration),
  [role="dialog"]:has(.processor-configuration),
  .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] .mat-dialog-container,
  .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] .mat-mdc-dialog-container,
  .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] [role="dialog"] {
    box-sizing: border-box !important;
    width: min(1024px, calc(100vw - 32px)) !important;
    max-width: calc(100vw - 32px) !important;
    max-height: calc(100vh - 32px) !important;
    overflow: auto !important;
  }
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-global-overlay-wrapper:has(.processor-configuration),
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-global-overlay-wrapper:has([${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"]) {
    align-items: flex-start !important;
    justify-content: flex-start !important;
    padding: 0 !important;
  }
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane:has(.processor-configuration),
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] {
    position: fixed !important;
    inset: auto !important;
    left: var(--cerebro-processor-dialog-left, 8px) !important;
    top: var(--cerebro-processor-dialog-top, 8px) !important;
    width: var(--cerebro-processor-dialog-width, 1024px) !important;
    max-width: none !important;
    height: var(--cerebro-processor-dialog-height, calc(100vh - 16px)) !important;
    max-height: none !important;
    margin: 0 !important;
    transform: scale(var(--cerebro-processor-dialog-scale, 1)) !important;
    transform-origin: top left !important;
  }
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .processor-configuration,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .mat-dialog-container:has(.processor-configuration),
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .mat-mdc-dialog-container:has(.processor-configuration),
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] [role="dialog"]:has(.processor-configuration),
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] .mat-dialog-container,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] .mat-mdc-dialog-container,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] [role="dialog"] {
    box-sizing: border-box !important;
    width: 100% !important;
    min-width: 0 !important;
    max-width: none !important;
    height: 100% !important;
    max-height: none !important;
    margin: 0 !important;
    overflow: auto !important;
  }
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .processor-configuration .mat-mdc-dialog-content,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .processor-configuration .mat-dialog-content,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .processor-configuration .mat-mdc-tab-body-content,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .processor-configuration .mat-tab-body-content,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] .mat-mdc-dialog-content,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] .mat-dialog-content,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] .mat-mdc-tab-body-content,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] .mat-tab-body-content {
    max-height: none !important;
    overflow: auto !important;
  }
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .processor-configuration .mat-mdc-dialog-actions,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .processor-configuration .mat-dialog-actions,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] .mat-mdc-dialog-actions,
  html[${PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE}="true"] .cdk-overlay-pane[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"] .mat-dialog-actions {
    box-sizing: border-box !important;
    width: 100% !important;
  }
`;
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
  [${NIFI_TOOL_HIDDEN_ATTRIBUTE}="true"] { display: none !important; }
  /*
   * ETL 이 띄우는 안내 글자(«프로세스 그룹» 등)는 이 겹침 칸에 그려진다. 기본 1000 이라 공구막대(아래 z-index)
   * 뒤로 들어가 글자가 가려졌다(2026-10-02 요청). 막대보다 위로 올려 앞에 보이게 한다.
   */
  .cdk-overlay-container {
    z-index: 100010 !important;
  }
  ${PROCESSOR_DIALOG_VIEWPORT_CSS}
  #${NIFI_MINI_TOOLBAR_ID} {
    position: fixed !important;
    left: 14px !important;
    bottom: 14px !important;
    transform: none !important;
    /* 캔버스 쪽 패널이 9100 보다 위에 있어, 막대 안에 그려지는 안내 글자가 가려졌다(2026-10-02 요청).
       막대를 그 위로 올린다 - 왼쪽 아래 구석의 작은 칸이라 다른 조작을 가리지 않는다. */
    z-index: 100000 !important;
    display: flex !important;
    flex-direction: column !important;
    align-items: center !important;
    justify-content: center !important;
    gap: 6px !important;
    width: 42px !important;
    min-width: 42px !important;
    min-height: 0 !important;
    padding: 6px 5px !important;
    border: 1px solid #dbe3ee !important;
    border-radius: 9px !important;
    background: #ffffff !important;
    box-shadow: 0 2px 8px rgb(15 23 42 / 9%) !important;
    backdrop-filter: none !important;
  }
  #${NIFI_MINI_TOOLBAR_ID}:empty {
    display: none !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"] {
    position: relative !important;
    width: 30px !important;
    height: 30px !important;
    min-width: 30px !important;
    min-height: 30px !important;
    max-width: 30px !important;
    max-height: 30px !important;
    margin: 0 !important;
    padding: 0 !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    border: 1px solid #dbe3ee !important;
    border-radius: 6px !important;
    background: #ffffff !important;
    color: #475569 !important;
    box-shadow: 0 1px 2px rgb(15 23 42 / 8%) !important;
    overflow: hidden !important;
    cursor: grab !important;
    user-select: none !important;
    touch-action: none !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"]:active {
    cursor: grabbing !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"]:hover {
    overflow: visible !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"]:hover {
    background: #f8fafc !important;
    color: #0f172a !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"][${NIFI_TOOL_LABEL_ATTRIBUTE}]:hover::after {
    content: attr(${NIFI_TOOL_LABEL_ATTRIBUTE}) !important;
    position: absolute !important;
    left: calc(100% + 8px) !important;
    top: 50% !important;
    transform: translateY(-50%) !important;
    z-index: 100002 !important;
    min-width: max-content !important;
    max-width: 180px !important;
    padding: 5px 8px !important;
    border: 1px solid #cbd5e1 !important;
    border-radius: 6px !important;
    background: #0f172a !important;
    color: #ffffff !important;
    font-size: 12px !important;
    font-weight: 600 !important;
    line-height: 1.2 !important;
    white-space: nowrap !important;
    box-shadow: 0 6px 16px rgb(15 23 42 / 18%) !important;
    pointer-events: none !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"][${NIFI_TOOL_LABEL_ATTRIBUTE}]:hover::before {
    content: "" !important;
    position: absolute !important;
    left: calc(100% + 3px) !important;
    top: 50% !important;
    transform: translateY(-50%) rotate(45deg) !important;
    z-index: 100001 !important;
    width: 8px !important;
    height: 8px !important;
    background: #0f172a !important;
    pointer-events: none !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"][${NIFI_TOOL_LABEL_ATTRIBUTE}]:hover::after {
    content: attr(${NIFI_TOOL_LABEL_ATTRIBUTE}) !important;
    position: absolute !important;
    left: calc(100% + 8px) !important;
    top: 50% !important;
    transform: translateY(-50%) !important;
    z-index: 100002 !important;
    min-width: max-content !important;
    max-width: 180px !important;
    padding: 5px 8px !important;
    border: 1px solid #cbd5e1 !important;
    border-radius: 6px !important;
    background: #0f172a !important;
    color: #ffffff !important;
    font-size: 12px !important;
    font-weight: 600 !important;
    line-height: 1.2 !important;
    white-space: nowrap !important;
    box-shadow: 0 6px 16px rgb(15 23 42 / 18%) !important;
    pointer-events: none !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"][${NIFI_TOOL_LABEL_ATTRIBUTE}]:hover::before {
    content: "" !important;
    position: absolute !important;
    left: calc(100% + 3px) !important;
    top: 50% !important;
    transform: translateY(-50%) rotate(45deg) !important;
    z-index: 100001 !important;
    width: 8px !important;
    height: 8px !important;
    background: #0f172a !important;
    pointer-events: none !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"]:has([title="프로세스 그룹"]),
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"][title="프로세스 그룹"],
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"]:has([aria-label="프로세스 그룹"]),
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"][aria-label="프로세스 그룹"],
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"]:has([aria-label="프로세스 그룹"]),
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"][aria-label="프로세스 그룹"] {
    border-color: #ff3b30 !important;
    background: #ffffff !important;
    color: #ff3b30 !important;
    box-shadow: none !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ICON_ATTRIBUTE}] > :not(.cerebro-tool-icon) {
    opacity: 0 !important;
    z-index: 2 !important;
    pointer-events: auto !important;
    background: transparent !important;
    border-color: transparent !important;
    box-shadow: none !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ICON_ATTRIBUTE}] :not(.cerebro-tool-icon):not(.cerebro-tool-icon *) {
    background: transparent !important;
    background-color: transparent !important;
    border-color: transparent !important;
    box-shadow: none !important;
    outline: none !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ICON_ATTRIBUTE}] > :not(.cerebro-tool-icon) * {
    opacity: 0 !important;
    background: transparent !important;
    border-color: transparent !important;
    box-shadow: none !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} .cerebro-tool-icon {
    position: absolute !important;
    inset: 0 !important;
    width: 18px !important;
    height: 18px !important;
    margin: auto !important;
    color: currentColor !important;
    pointer-events: none !important;
    visibility: visible !important;
    z-index: 1 !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} .cerebro-tool-icon * {
    vector-effect: non-scaling-stroke !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"] i,
  #${NIFI_MINI_TOOLBAR_ID} [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"] svg {
    transform: scale(0.74) !important;
    transform-origin: center !important;
  }
  #${NIFI_MINI_TOOLBAR_ID} .cerebro-tool-icon {
    transform: none !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] {
    position: fixed !important;
    left: 24px !important;
    top: 14px !important;
    bottom: auto !important;
    transform: none !important;
    z-index: 100000 !important;
    width: auto !important;
    max-width: calc(100vw - 96px) !important;
    min-height: 0 !important;
    height: 38px !important;
    display: flex !important;
    flex-direction: row !important;
    padding: 3px 7px !important;
    justify-content: flex-start !important;
    align-items: center !important;
    gap: 5px !important;
    background: #ffffff !important;
    background-color: #ffffff !important;
    border: 1px solid #dbe3ee !important;
    border-radius: 9px !important;
    box-shadow: 0 2px 8px rgb(15 23 42 / 9%) !important;
    overflow: visible !important;
    transition: transform 180ms ease, opacity 160ms ease !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"]:not([${NIFI_TOP_TOOLBAR_EXPANDED_ATTRIBUTE}="true"]) {
    width: 38px !important;
    min-width: 38px !important;
    max-width: 38px !important;
    padding: 0 !important;
    overflow: hidden !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] > :not([${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"]):not(:has([${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"])) {
    display: none !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] > :has([${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"]) {
    width: auto !important;
    max-width: none !important;
    height: auto !important;
    min-width: 0 !important;
    min-height: 0 !important;
    display: flex !important;
    flex-direction: row !important;
    align-items: center !important;
    justify-content: flex-start !important;
    gap: 5px !important;
    flex-wrap: nowrap !important;
    margin: 0 !important;
    padding: 0 !important;
    background: transparent !important;
    border-color: transparent !important;
    box-shadow: none !important;
    outline: none !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"] {
    position: relative !important;
    width: 30px !important;
    height: 30px !important;
    min-width: 30px !important;
    min-height: 30px !important;
    max-width: 30px !important;
    max-height: 30px !important;
    margin: 0 !important;
    padding: 0 !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    border: 1px solid #dbe3ee !important;
    border-radius: 6px !important;
    background: #ffffff !important;
    color: #475569 !important;
    box-shadow: 0 1px 2px rgb(15 23 42 / 8%) !important;
    cursor: grab !important;
    user-select: none !important;
    touch-action: none !important;
    overflow: hidden !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"]:active {
    cursor: grabbing !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"] i,
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"] svg {
    transform: scale(0.76) !important;
    transform-origin: center !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ICON_ATTRIBUTE}] > :not(.cerebro-tool-icon) {
    opacity: 0 !important;
    z-index: 2 !important;
    pointer-events: auto !important;
    background: transparent !important;
    border-color: transparent !important;
    box-shadow: none !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ICON_ATTRIBUTE}] :not(.cerebro-tool-icon):not(.cerebro-tool-icon *) {
    background: transparent !important;
    background-color: transparent !important;
    border-color: transparent !important;
    box-shadow: none !important;
    outline: none !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ICON_ATTRIBUTE}] > :not(.cerebro-tool-icon) * {
    opacity: 0 !important;
    background: transparent !important;
    border-color: transparent !important;
    box-shadow: none !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] .cerebro-tool-icon {
    position: absolute !important;
    inset: 0 !important;
    width: 18px !important;
    height: 18px !important;
    margin: auto !important;
    color: currentColor !important;
    pointer-events: none !important;
    visibility: visible !important;
    transform: none !important;
    z-index: 1 !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_MENU_BUTTON_ATTRIBUTE}="true"] {
    display: none !important;
  }
  [${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"] [${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"] ~ :not([${NIFI_TOOL_ALLOWED_ATTRIBUTE}="true"]) {
    display: none !important;
  }
  [${NIFI_MENU_BUTTON_ATTRIBUTE}="true"] {
    position: fixed !important;
    top: 14px !important;
    right: 16px !important;
    z-index: 9101 !important;
    width: 34px !important;
    height: 34px !important;
    min-width: 34px !important;
    min-height: 34px !important;
    max-width: 34px !important;
    max-height: 34px !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    padding: 0 !important;
    border: 1px solid #cbd5e1 !important;
    border-radius: 8px !important;
    background: rgba(255, 255, 255, 0.98) !important;
    color: #475569 !important;
    box-shadow: 0 2px 8px rgb(15 23 42 / 8%) !important;
    backdrop-filter: blur(8px) !important;
  }
  [${NIFI_MENU_BUTTON_ATTRIBUTE}="true"]:hover {
    background: #f8fafc !important;
    border-color: #94a3b8 !important;
    color: #0f172a !important;
  }
  [${NIFI_MENU_BUTTON_ATTRIBUTE}="true"] i,
  [${NIFI_MENU_BUTTON_ATTRIBUTE}="true"] svg {
    transform: scale(0.9) !important;
    transform-origin: center !important;
  }
  #${NIFI_RECENT_PANEL_TOGGLE_ID} {
    position: fixed !important;
    left: 0 !important;
    top: 14px !important;
    z-index: 100001 !important;
    width: 18px !important;
    height: 38px !important;
    min-width: 18px !important;
    min-height: 38px !important;
    padding: 0 !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    border: 1px solid #dbe3ee !important;
    border-left: 0 !important;
    border-radius: 0 8px 8px 0 !important;
    background: #ffffff !important;
    color: #3b5270 !important;
    box-shadow: 0 2px 8px rgb(15 23 42 / 8%) !important;
    cursor: pointer !important;
  }
  #${NIFI_RECENT_PANEL_TOGGLE_ID}:hover {
    background: #f8fafc !important;
    color: #0f172a !important;
  }
  #${NIFI_RECENT_PANEL_TOGGLE_ID}::before {
    content: "«" !important;
    font-size: 17px !important;
    font-weight: 700 !important;
    line-height: 1 !important;
  }
  [${NIFI_BREADCRUMB_ATTRIBUTE}="true"] {
    display: none !important;
  }
  [${NIFI_BREADCRUMB_ATTRIBUTE}="true"] svg,
  [${NIFI_BREADCRUMB_ATTRIBUTE}="true"] i {
    width: 13px !important;
    height: 13px !important;
    color: #64748b !important;
  }
  [${NIFI_BREADCRUMB_ATTRIBUTE}="true"] a,
  [${NIFI_BREADCRUMB_ATTRIBUTE}="true"] button,
  [${NIFI_BREADCRUMB_ATTRIBUTE}="true"] span {
    color: inherit !important;
    font: inherit !important;
  }
  [${NIFI_BREADCRUMB_CONTAINER_ATTRIBUTE}="true"] > :not([${NIFI_BREADCRUMB_ATTRIBUTE}="true"]) {
    display: none !important;
  }
  [${NIFI_BREADCRUMB_CONTAINER_ATTRIBUTE}="true"] {
    display: none !important;
  }
  #${NIFI_READ_ONLY_NOTICE_ID} {
    margin: 12px 20px 0 !important;
    padding: 10px 12px !important;
    border: 1px solid #fecaca !important;
    border-radius: 6px !important;
    background: #fff1f2 !important;
    color: #991b1b !important;
    font-size: 12px !important;
    font-weight: 700 !important;
    line-height: 1.45 !important;
  }
  [${NIFI_LOCK_DIALOG_ATTRIBUTE}="true"] input[${NIFI_LOCK_READONLY_BEFORE_ATTRIBUTE}],
  [${NIFI_LOCK_DIALOG_ATTRIBUTE}="true"] textarea[${NIFI_LOCK_READONLY_BEFORE_ATTRIBUTE}],
  [${NIFI_LOCK_DIALOG_ATTRIBUTE}="true"] select[${NIFI_LOCK_DISABLED_BEFORE_ATTRIBUTE}],
  [${NIFI_LOCK_DIALOG_ATTRIBUTE}="true"] button[${NIFI_LOCK_DISABLED_BEFORE_ATTRIBUTE}] {
    cursor: not-allowed !important;
  }
  body,
  #canvas-container,
  #canvas,
  .canvas-container,
  .canvas-background,
  .graph-container,
  .flow-canvas {
    background: #ffffff !important;
    background-image: none !important;
  }
  svg g.process-group rect,
  svg g.component.process-group rect {
    shape-rendering: crispEdges !important;
  }
  svg g.process-group .body,
  svg g.process-group .border,
  svg g.process-group .process-group-background,
  svg g.process-group .process-group-body,
  svg g.process-group .component-background,
  svg g.process-group rect.body,
  svg g.process-group rect.border,
  svg g.process-group rect.process-group-background,
  svg g.process-group rect.process-group-body,
  svg g.process-group rect.component-background,
  svg g.component.process-group .body,
  svg g.component.process-group .border,
  svg g.component.process-group .process-group-body,
  svg g.component.process-group .component-background,
  svg g.component.process-group rect.body,
  svg g.component.process-group rect.border,
  svg g.component.process-group rect.process-group-body,
  svg g.component.process-group rect.component-background {
    fill: #ffffff !important;
    stroke: #d8e0ea !important;
    stroke-width: 1px !important;
  }
  svg g.process-group .component-header,
  svg g.process-group .header,
  svg g.process-group .process-group-header,
  svg g.process-group .process-group-header-background,
  svg g.process-group .component-header-background,
  svg g.process-group rect.component-header,
  svg g.process-group rect.header,
  svg g.process-group rect.process-group-header,
  svg g.process-group rect.process-group-header-background,
  svg g.process-group rect.component-header-background,
  svg g.component.process-group .component-header,
  svg g.component.process-group .header,
  svg g.component.process-group .process-group-header,
  svg g.component.process-group .process-group-header-background,
  svg g.component.process-group .component-header-background,
  svg g.component.process-group rect.component-header,
  svg g.component.process-group rect.header,
  svg g.component.process-group rect.process-group-header,
  svg g.component.process-group rect.process-group-header-background,
  svg g.component.process-group rect.component-header-background {
    fill: #34445a !important;
    stroke: #34445a !important;
  }
  svg g.process-group .name,
  svg g.process-group .process-group-name,
  svg g.process-group .component-name,
  svg g.component.process-group .name,
  svg g.component.process-group .process-group-name,
  svg g.component.process-group .component-name {
    fill: #ffffff !important;
    color: #ffffff !important;
    font-weight: 700 !important;
  }
  svg g.process-group .component-type,
  svg g.process-group .process-group-type,
  svg g.component.process-group .component-type,
  svg g.component.process-group .process-group-type {
    fill: #cbd5e1 !important;
    color: #cbd5e1 !important;
    font-size: 10px !important;
  }
  svg g.process-group .stats-label,
  svg g.process-group .status-label,
  svg g.process-group .queued-label,
  svg g.component.process-group .stats-label,
  svg g.component.process-group .status-label,
  svg g.component.process-group .queued-label {
    fill: #3b5270 !important;
    color: #3b5270 !important;
    font-weight: 600 !important;
  }
  svg g.process-group .stats-value,
  svg g.process-group .status-value,
  svg g.process-group .queued-value,
  svg g.component.process-group .stats-value,
  svg g.component.process-group .status-value,
  svg g.component.process-group .queued-value {
    fill: #020617 !important;
    color: #020617 !important;
    font-weight: 700 !important;
  }
  svg g.process-group .process-group-stats rect,
  svg g.process-group .component-stats rect,
  svg g.process-group .status-container rect,
  svg g.component.process-group .process-group-stats rect,
  svg g.component.process-group .component-stats rect,
  svg g.component.process-group .status-container rect {
    fill: #ffffff !important;
    stroke: #e8edf4 !important;
    stroke-width: 1px !important;
  }
  svg g.process-group .bulletin-background,
  svg g.component.process-group .bulletin-background {
    fill: #ffffff !important;
    stroke: #e8edf4 !important;
  }
  svg g.process-group .fa-play,
  svg g.process-group .icon-play,
  svg g.component.process-group .fa-play,
  svg g.component.process-group .icon-play {
    fill: #64748b !important;
    color: #64748b !important;
  }
  svg g.process-group .fa-stop,
  svg g.process-group .icon-stop,
  svg g.component.process-group .fa-stop,
  svg g.component.process-group .icon-stop {
    fill: #ef4444 !important;
    color: #ef4444 !important;
  }
  svg g.process-group .fa-warning,
  svg g.process-group .fa-exclamation-triangle,
  svg g.component.process-group .fa-warning,
  svg g.component.process-group .fa-exclamation-triangle {
    fill: #64748b !important;
    color: #64748b !important;
  }
  svg g.process-group .fa-star,
  svg g.component.process-group .fa-star {
    fill: #64748b !important;
    color: #64748b !important;
  }
  svg g.process-group .refresh-interval,
  svg g.process-group .status-history,
  svg g.component.process-group .refresh-interval,
  svg g.component.process-group .status-history {
    fill: #f1f5f9 !important;
    stroke: #dbe4ee !important;
  }
  mat-toolbar,
  .mat-toolbar,
  .mat-mdc-toolbar,
  .mat-sidenav-content > header,
  .mat-sidenav-content > nav,
  .mat-sidenav-content > .toolbar,
  .mat-sidenav-content > .header,
  .mat-drawer-content > header,
  .mat-drawer-content > nav,
  .mat-drawer-content > .toolbar,
  .mat-drawer-content > .header {
    background: #ffffff !important;
    background-color: #ffffff !important;
  }
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
    left: 24px !important;
    top: 60px !important;
    right: auto !important;
    bottom: auto !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="operation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) {
    left: 24px !important;
    top: 106px !important;
    right: auto !important;
    bottom: auto !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"][${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"] {
    top: 60px !important;
    right: auto !important;
    bottom: auto !important;
    left: 24px !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="operation"][${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"] {
    left: 24px !important;
    top: 106px !important;
    right: auto !important;
    bottom: auto !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) > :not(:first-child),
  [${NIFI_PANEL_ATTRIBUTE}="operation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) > :not(:first-child) {
    display: none !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) > :first-child,
  [${NIFI_PANEL_ATTRIBUTE}="operation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) > :first-child {
    width: 38px !important;
    height: 38px !important;
    min-width: 38px !important;
    min-height: 38px !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    padding: 0 !important;
    margin: 0 !important;
    background: #ffffff !important;
    border: 0 !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) > :first-child :not(i):not(svg):not(path):not([class*="fa"]):not([class*="icon"]),
  [${NIFI_PANEL_ATTRIBUTE}="operation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) > :first-child :not(i):not(svg):not(path):not([class*="fa"]):not([class*="icon"]) {
    display: none !important;
  }
  [${NIFI_PANEL_ATTRIBUTE}="navigation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) i,
  [${NIFI_PANEL_ATTRIBUTE}="navigation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) svg,
  [${NIFI_PANEL_ATTRIBUTE}="operation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) i,
  [${NIFI_PANEL_ATTRIBUTE}="operation"]:not([${NIFI_PANEL_EXPANDED_ATTRIBUTE}="true"]) svg {
    margin: 0 !important;
    color: #0f172a !important;
    font-size: 15px !important;
  }
  .navigation-control:not(:has(> div.w-72)),
  .operation-control:not(:has(> div.w-72)) {
    position: fixed !important;
    left: 24px !important;
    z-index: 9000 !important;
    width: 38px !important;
    height: 38px !important;
    min-width: 38px !important;
    min-height: 38px !important;
    max-width: 38px !important;
    max-height: 38px !important;
    padding: 0 !important;
    overflow: hidden !important;
    border: 1px solid #dbe3ee !important;
    border-radius: 8px !important;
    background: #ffffff !important;
    color: #3b5270 !important;
    box-shadow: 0 1px 3px rgb(15 23 42 / 8%) !important;
  }
  .navigation-control:not(:has(> div.w-72)) {
    top: 60px !important;
  }
  .operation-control:not(:has(> div.w-72)) {
    top: 106px !important;
  }
  .navigation-control:not(:has(> div.w-72)) > :first-child,
  .operation-control:not(:has(> div.w-72)) > :first-child,
  .navigation-control:not(:has(> div.w-72)) .navigation-control-header,
  .operation-control:not(:has(> div.w-72)) .operation-control-header {
    width: 38px !important;
    height: 38px !important;
    min-width: 38px !important;
    min-height: 38px !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    padding: 0 !important;
    margin: 0 !important;
    border: 0 !important;
    background: #ffffff !important;
  }
  .navigation-control:not(:has(> div.w-72)) .text-sm,
  .navigation-control:not(:has(> div.w-72)) .navigation-control-expansion,
  .operation-control:not(:has(> div.w-72)) .text-sm,
  .operation-control:not(:has(> div.w-72)) .operation-control-expansion {
    display: none !important;
  }
  .navigation-control:not(:has(> div.w-72)) i,
  .operation-control:not(:has(> div.w-72)) i {
    margin: 0 !important;
    color: #3b5270 !important;
    font-size: 15px !important;
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
const NIFI_PROCESSOR_EDIT_TEXT: Record<string, string> = {
  Settings: "설정",
  Scheduling: "스케줄링",
  Properties: "속성",
  Relationships: "관계",
  Comments: "코멘트",
  Name: "이름",
  "Name*": "이름*",
  Id: "ID",
  ID: "ID",
  Type: "타입",
  Bundle: "번들",
  "Penalty Duration": "패널티 지속시간",
  "Penalty Duration*": "패널티 지속시간*",
  "Yield Duration": "양보 지속시간",
  "Yield Duration*": "양보 지속시간*",
  "Bulletin Level": "알림 레벨",
  "Bulletin Level*": "알림 레벨*",
  "Scheduling Strategy": "스케줄링 전략",
  "Scheduling Strategy*": "스케줄링 전략*",
  "Concurrent Tasks": "동시 작업 수",
  "Concurrent Tasks*": "동시 작업 수*",
  "Run Schedule": "실행 주기",
  "Run Schedule*": "실행 주기*",
  Execution: "실행 대상",
  "Execution*": "실행 대상*",
  "Required field": "필수 항목",
  Verification: "검증",
  Property: "속성",
  Value: "값",
};
const COMMON_NIFI_PROCESSOR_PROPERTY_TEXT: Record<string, string> = {
  "Database Connection Pooling Service": "데이터베이스 연결 풀링 서비스",
  "Distributed Cache Service": "분산 캐시 서비스",
  "Record Reader": "레코드 리더",
  "Record Writer": "레코드 작성기",
  "Schema Access Strategy": "스키마 접근 방식",
  "Schema Registry": "스키마 레지스트리",
  "Schema Name": "스키마명",
  "Schema Text": "스키마 텍스트",
  "Schema Reference Reader": "스키마 참조 리더",
  "Schema Write Strategy": "스키마 쓰기 방식",
  "Schema Branch": "스키마 브랜치",
  "Schema Version": "스키마 버전",
  "Schema Version Identifier": "스키마 버전 식별자",
  "Schema Protocol Version": "스키마 프로토콜 버전",
  "Cache Schema": "스키마 캐시",
  "Date Format": "날짜 형식",
  "Time Format": "시간 형식",
  "Timestamp Format": "타임스탬프 형식",
  "Character Set": "문자셋",
  "Compression Format": "압축 형식",
  Destination: "대상",
  "Attribute Name": "속성명",
  "Attribute Value": "속성값",
  "Relationship Name": "관계명",
  "Expression Language Scope": "표현 언어 범위",
  "Maximum Batch Size": "최대 배치 크기",
  "Maximum Fragment Size": "최대 조각 크기",
  "Include Zero Record FlowFiles": "빈 레코드 FlowFile 포함",
  "Mime Type": "MIME 타입",
  "Dynamic Property": "동적 속성",
};

const NIFI_PROCESSOR_PROPERTY_TEXT: Record<string, Record<string, string>> = {
  QueryRecord: {
    "Record Reader": "레코드 리더",
    "Record Writer": "레코드 작성기",
    "Include Zero Record FlowFiles": "빈 레코드 FlowFile 포함",
    "Cache Schema": "스키마 캐시",
  },
  PutDatabaseRecord: {
    "Database Connection Pooling Service": "데이터베이스 연결 풀링 서비스",
    "Record Reader": "레코드 리더",
    "Statement Type": "SQL 문 유형",
    "Table Name": "테이블명",
    "Catalog Name": "카탈로그명",
    "Schema Name": "스키마명",
    "Translate Field Names": "필드명 변환",
    "Unmatched Column Behavior": "일치하지 않는 컬럼 처리",
    "Unmatched Field Behavior": "일치하지 않는 필드 처리",
    "Update Keys": "업데이트 키",
    "Quoted Identifiers": "식별자 따옴표 처리",
    "Quoted Table Identifiers": "테이블 식별자 따옴표 처리",
    "Query Timeout": "쿼리 타임아웃",
    "Max Wait Time": "최대 대기 시간",
    "Rollback On Failure": "실패 시 롤백",
    "Obtain Generated Keys": "생성 키 가져오기",
    "Use Avro Logical Types": "Avro 논리 타입 사용",
    "Maximum Batch Size": "최대 배치 크기",
  },
  RouteOnAttribute: {
    "Routing Strategy": "라우팅 방식",
    "Route Strategy": "라우팅 전략",
    "Route to Property name": "속성명으로 라우팅",
    "Matching Strategy": "매칭 방식",
    "Expression Language Scope": "표현 언어 범위",
  },
  UpdateAttribute: {
    "Delete Attributes Expression": "속성 삭제 표현식",
    "Store State": "상태 저장 위치",
    "Stateful Variables Initial Value": "상태 변수 초기값",
    "Cache Entry Identifier": "캐시 항목 식별자",
    "Distributed Cache Service": "분산 캐시 서비스",
  },
  ListFile: {
    "Input Directory": "입력 디렉터리",
    "Listing Strategy": "목록 조회 방식",
    "Recurse Subdirectories": "하위 디렉터리 재귀 탐색",
    "Input Directory Location": "입력 디렉터리 위치",
    "File Filter": "파일 필터",
    "Path Filter": "경로 필터",
    "Include File Attributes": "파일 속성 포함",
    "Minimum File Age": "최소 파일 나이",
    "Maximum File Age": "최대 파일 나이",
    "Minimum File Size": "최소 파일 크기",
    "Maximum File Size": "최대 파일 크기",
    "Ignore Hidden Files": "숨김 파일 무시",
    "Target System Timestamp Precision": "대상 시스템 타임스탬프 정밀도",
    "Tracking Timestamps": "타임스탬프 추적",
    "Entity Tracking State Cache": "엔티티 추적 상태 캐시",
    "Entity Tracking Time Window": "엔티티 추적 시간 범위",
    "Initial Listing Target": "초기 목록 대상",
    "Follow Symbolic Links": "심볼릭 링크 따라가기",
    "Record Writer": "레코드 작성기",
  },
  ExecuteSQLRecord: {
    "Database Connection Pooling Service": "데이터베이스 연결 풀링 서비스",
    "SQL Pre-Query": "SQL 사전 쿼리",
    "SQL select query": "SQL 조회 쿼리",
    "SQL Post-Query": "SQL 사후 쿼리",
    "Max Wait Time": "최대 대기 시간",
    "Record Writer": "레코드 작성기",
    "Normalize Table/Column Names": "테이블/컬럼명 정규화",
    "Use Avro Logical Types": "Avro 논리 타입 사용",
    "Default Decimal Precision": "기본 Decimal 정밀도",
  },
};
const NIFI_PROCESSOR_EDIT_TAB_TEXT = new Set(["Settings", "Scheduling", "Properties", "Relationships", "Comments"]);
const NIFI_PROCESSOR_EDIT_CONTENT_TABS = new Set(["Settings", "설정", "Scheduling", "스케줄링"]);
const NIFI_PROCESSOR_EDIT_PROPERTY_TABS = new Set(["Properties", "속성"]);
const NIFI_PROCESSOR_EDIT_PROPERTY_UI_TEXT = new Set(["Required field", "Verification", "Property", "Value"]);
const NIFI_PROCESSOR_EDIT_TAB_SELECTOR = '[role="tab"], .mat-mdc-tab, .mat-tab-label';
const HIDDEN_NIFI_CONTEXT_MENU_ITEMS = new Set(["About", "정보", "Appearance", "화면 표시", "Animations", "애니메이션"]);
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
const ALLOWED_NIFI_CANVAS_TOOLS = new Set([
  "Processor",
  "프로세서",
  "Process Group",
  "프로세스 그룹",
  "Label",
  "라벨",
]);
const HIDDEN_NIFI_CANVAS_TOOLS = new Set(["Import from Registry", "레지스트리에서 가져오기"]);
const NIFI_MENU_ICON_CLASSES = ["fa-bars", "fa-navicon", "fa-reorder"];

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

function processorTypeKey(value: string | null | undefined) {
  if (!value) {
    return undefined;
  }
  const typeName = value
    .replace(/\s+\d+(?:\.\d+)*(?:[-.\w]*)?\s*$/, "")
    .split(".")
    .pop()
    ?.trim();
  return typeName && NIFI_PROCESSOR_PROPERTY_TEXT[typeName] ? typeName : undefined;
}

function translateProcessorPropertyText(value: string, processorType?: string) {
  const typeKey = processorTypeKey(processorType);
  return (typeKey ? NIFI_PROCESSOR_PROPERTY_TEXT[typeKey]?.[value] : undefined)
    ?? COMMON_NIFI_PROCESSOR_PROPERTY_TEXT[value];
}

function translateProcessorEditText(
  value: string,
  includeFieldLabels = true,
  includePropertyLabels = false,
  processorType?: string,
) {
  const trimmed = value.replace(/\s+/g, " ").trim();
  const exact = NIFI_PROCESSOR_EDIT_TEXT[trimmed];
  if (
    exact
    && (
      includeFieldLabels
      || NIFI_PROCESSOR_EDIT_TAB_TEXT.has(trimmed)
      || (includePropertyLabels && NIFI_PROCESSOR_EDIT_PROPERTY_UI_TEXT.has(trimmed))
    )
  ) {
    return exact;
  }
  const normalized = trimmed.replace(/\s*\*$/, "");
  const normalizedExact = NIFI_PROCESSOR_EDIT_TEXT[normalized];
  if (
    normalizedExact
    && (
      includeFieldLabels
      || NIFI_PROCESSOR_EDIT_TAB_TEXT.has(normalized)
      || (includePropertyLabels && NIFI_PROCESSOR_EDIT_PROPERTY_UI_TEXT.has(normalized))
    )
  ) {
    return `${normalizedExact}${trimmed.endsWith("*") ? "*" : ""}`;
  }
  const property = includePropertyLabels ? translateProcessorPropertyText(trimmed, processorType) : undefined;
  if (property) {
    return `${property}(${trimmed})`;
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

/**
 * iframe 안이 Cerebro 판 NiFi 화면(nifi/ui-custom, CEREBRO_NIFI_UI=custom)인가. 그 화면은 이 파일이 하던 덧칠
 * (배치 바꾸기 · 한글 바꾸기)을 NiFi 안에서 처음 그릴 때부터 한다 - 여기서 또 하면 일이 겹치고, 덧칠이 늦게 붙는 동안 원래 배치가
 * 잠깐 보이던 깜빡임(2026-10-05 실측 약 0.4초)이 남는다. 공식 화면이나 Airflow 화면에는 이 표시가 없어 예전처럼 덧칠한다.
 */
function isCerebroNifiUi(frame: HTMLIFrameElement) {
  return iframeDocument(frame)?.documentElement.hasAttribute("data-cerebro-nifi-ui") ?? false;
}

function isFrameDocumentLoaded(frame: HTMLIFrameElement) {
  const doc = iframeDocument(frame);
  return Boolean(doc && doc.readyState === "complete" && doc.location.href !== "about:blank");
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
    return false;
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
    return true;
  } catch {
    // 문서가 아직 교체 중이면 다음 iframe load에서 다시 시도한다.
    return false;
  }
}

function resetProcessorDialogFitTarget(element: Element) {
  element.removeAttribute(PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE);
  if (!(element instanceof HTMLElement)) {
    return;
  }
  [
    "position",
    "inset",
    "left",
    "top",
    "width",
    "max-width",
    "height",
    "max-height",
    "margin",
    "transform",
    "transform-origin",
  ].forEach((property) => element.style.removeProperty(property));
}

function updateProcessorDialogViewportFit(frame: HTMLIFrameElement) {
  const doc = iframeDocument(frame);
  if (!doc) {
    return;
  }
  const frameWindow = doc.defaultView;
  if (!frameWindow) {
    return;
  }
  const root = doc.documentElement;
  const dialog = findProcessorEditDialogs(doc)[0] ?? doc.querySelector(".processor-configuration");
  doc.querySelectorAll(`[${PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE}="true"]`).forEach((element) => {
    resetProcessorDialogFitTarget(element);
  });
  if (!dialog) {
    root.removeAttribute(PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE);
    root.style.removeProperty("--cerebro-processor-dialog-scale");
    root.style.removeProperty("--cerebro-processor-dialog-width");
    root.style.removeProperty("--cerebro-processor-dialog-height");
    root.style.removeProperty("--cerebro-processor-dialog-left");
    root.style.removeProperty("--cerebro-processor-dialog-top");
    return;
  }
  const overlayPane = dialog.closest(".cdk-overlay-pane") ?? dialog;
  overlayPane.setAttribute(PROCESSOR_DIALOG_FIT_TARGET_ATTRIBUTE, "true");

  const frameRect = frame.getBoundingClientRect();
  const visualViewport = window.visualViewport;
  const parentWidth = visualViewport?.width ?? window.innerWidth;
  const parentHeight = visualViewport?.height ?? window.innerHeight;
  const visibleLeft = Math.max(frameRect.left, 0);
  const visibleTop = Math.max(frameRect.top, 0);
  const visibleRight = Math.min(frameRect.right, parentWidth);
  const visibleBottom = Math.min(frameRect.bottom, parentHeight);
  const visibleFrameWidth = Math.max(0, visibleRight - visibleLeft);
  const visibleFrameHeight = Math.max(0, visibleBottom - visibleTop);
  const margin = 8;
  const baseWidth = 1024;
  const baseHeight = 720;
  const viewportWidth = Math.max(1, Math.min(frameWindow.innerWidth, visibleFrameWidth || frameWindow.innerWidth));
  const viewportHeight = Math.max(1, Math.min(frameWindow.innerHeight, visibleFrameHeight || frameWindow.innerHeight));
  const availableWidth = Math.max(1, viewportWidth - margin * 2);
  const availableHeight = Math.max(1, viewportHeight - margin * 2);
  const scale = Math.max(0.3, Math.min(1, availableWidth / baseWidth, availableHeight / baseHeight));
  if (scale >= 0.999) {
    root.removeAttribute(PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE);
    root.style.removeProperty("--cerebro-processor-dialog-scale");
    root.style.removeProperty("--cerebro-processor-dialog-width");
    root.style.removeProperty("--cerebro-processor-dialog-height");
    root.style.removeProperty("--cerebro-processor-dialog-left");
    root.style.removeProperty("--cerebro-processor-dialog-top");
    return;
  }

  root.setAttribute(PROCESSOR_DIALOG_VIEWPORT_SCALED_ATTRIBUTE, "true");
  root.style.setProperty("--cerebro-processor-dialog-scale", scale.toFixed(4));
  root.style.setProperty("--cerebro-processor-dialog-width", `${baseWidth}px`);
  root.style.setProperty("--cerebro-processor-dialog-height", `${availableHeight / scale}px`);
  root.style.setProperty("--cerebro-processor-dialog-left", `${margin}px`);
  root.style.setProperty("--cerebro-processor-dialog-top", `${margin}px`);

  if (overlayPane instanceof HTMLElement) {
    overlayPane.style.setProperty("position", "fixed", "important");
    overlayPane.style.setProperty("inset", "auto", "important");
    overlayPane.style.setProperty("left", `${margin}px`, "important");
    overlayPane.style.setProperty("top", `${margin}px`, "important");
    overlayPane.style.setProperty("width", `${baseWidth}px`, "important");
    overlayPane.style.setProperty("max-width", "none", "important");
    overlayPane.style.setProperty("height", `${availableHeight / scale}px`, "important");
    overlayPane.style.setProperty("max-height", "none", "important");
    overlayPane.style.setProperty("margin", "0", "important");
    overlayPane.style.setProperty("transform", `scale(${scale.toFixed(4)})`, "important");
    overlayPane.style.setProperty("transform-origin", "top left", "important");
  }
}

function installProcessorDialogViewportFit(frame: HTMLIFrameElement) {
  const doc = iframeDocument(frame);
  const frameWindow = frame.contentWindow;
  if (!doc || !frameWindow) {
    return;
  }

  let style = doc.getElementById(PROCESSOR_DIALOG_VIEWPORT_STYLE_ID);
  if (!style) {
    style = doc.createElement("style");
    style.id = PROCESSOR_DIALOG_VIEWPORT_STYLE_ID;
    doc.head.appendChild(style);
  }
  if (style.textContent !== PROCESSOR_DIALOG_VIEWPORT_CSS) {
    style.textContent = PROCESSOR_DIALOG_VIEWPORT_CSS;
  }

  updateProcessorDialogViewportFit(frame);

  if (doc.documentElement.getAttribute(PROCESSOR_DIALOG_VIEWPORT_FIT_ATTRIBUTE) === "true") {
    return;
  }
  doc.documentElement.setAttribute(PROCESSOR_DIALOG_VIEWPORT_FIT_ATTRIBUTE, "true");

  let frameId: number | null = null;
  const schedule = () => {
    if (frameId !== null) {
      return;
    }
    frameId = frameWindow.requestAnimationFrame(() => {
      frameId = null;
      updateProcessorDialogViewportFit(frame);
    });
  };
  const DialogMutationObserver = doc.defaultView?.MutationObserver ?? MutationObserver;
  const observer = new DialogMutationObserver(schedule);
  observer.observe(doc.documentElement, { childList: true, subtree: true });
  const FrameResizeObserver = window.ResizeObserver;
  const resizeObserver = FrameResizeObserver ? new FrameResizeObserver(schedule) : null;
  resizeObserver?.observe(frame);
  if (frame.parentElement) {
    resizeObserver?.observe(frame.parentElement);
  }
  frameWindow.addEventListener("resize", schedule);
  window.addEventListener("resize", schedule);
  window.visualViewport?.addEventListener("resize", schedule);
  frameWindow.addEventListener("beforeunload", () => {
    if (frameId !== null) {
      frameWindow.cancelAnimationFrame(frameId);
    }
    frameWindow.removeEventListener("resize", schedule);
    window.removeEventListener("resize", schedule);
    window.visualViewport?.removeEventListener("resize", schedule);
    observer.disconnect();
    resizeObserver?.disconnect();
  }, { once: true });
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

  const toolLabel = (element: Element) =>
    [NIFI_TOOL_LABEL_ATTRIBUTE, ...NIFI_TOOLTIP_ATTRIBUTES]
      .map((attributeName) => element.getAttribute(attributeName)?.replace(/\s+/g, " ").trim())
      .find((value): value is string => Boolean(value));

  const hasToolLabel = (element: Element, labels: Set<string>) => {
    if (labels.has(toolLabel(element) ?? "")) {
      return true;
    }
    return Array.from(
      element.querySelectorAll("[title], [aria-label], [data-tooltip], [matTooltip], [mattooltip], [tooltip]"),
    ).some((child) => labels.has(toolLabel(child) ?? ""));
  };

  const toolIconName = (label: string | undefined) => {
    switch (label) {
      case "Refresh":
      case "새로고침":
        return "refresh";
      case "Processor":
      case "프로세서":
        return "processor";
      case "Process Group":
      case "프로세스 그룹":
        return "group";
      case "Remote Process Group":
      case "원격 프로세스 그룹":
        return "remote";
      case "Input Port":
      case "입력 포트":
        return "input";
      case "Output Port":
      case "출력 포트":
        return "output";
      case "Funnel":
      case "퍼널":
        return "funnel";
      case "Label":
      case "라벨":
        return "label";
      case "Template":
      case "템플릿":
        return "template";
      default:
        return null;
    }
  };

  const toolIconSvg = (iconName: string) => {
    const common = 'class="cerebro-tool-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"';
    switch (iconName) {
      case "refresh":
        return `<svg ${common}><path d="M20 6v5h-5"></path><path d="M4 18v-5h5"></path><path d="M18.6 9A7 7 0 0 0 6 7.7L4 10"></path><path d="M5.4 15A7 7 0 0 0 18 16.3L20 14"></path></svg>`;
      case "processor":
        return `<svg ${common}><rect x="7" y="7" width="10" height="10" rx="1.5"></rect><path d="M4 9h3M4 15h3M17 9h3M17 15h3M9 4v3M15 4v3M9 17v3M15 17v3"></path></svg>`;
      case "group":
        return `<svg ${common}><rect x="5" y="6" width="13" height="12" rx="2"></rect><path d="M9 6V4h10v10h-1"></path><path d="M8 11h7M8 14h5"></path></svg>`;
      case "remote":
        return `<svg ${common}><circle cx="12" cy="12" r="8.5"></circle><path d="M3.5 12h17"></path><path d="M12 3.5c2.2 2.3 3.3 5.1 3.3 8.5S14.2 18.2 12 20.5"></path><path d="M12 3.5C9.8 5.8 8.7 8.6 8.7 12s1.1 6.2 3.3 8.5"></path></svg>`;
      case "input":
        return `<svg ${common}><circle cx="12" cy="12" r="8"></circle><path d="M8 12h7"></path><path d="m12 9 3 3-3 3"></path></svg>`;
      case "output":
        return `<svg ${common}><circle cx="12" cy="12" r="8"></circle><path d="M16 12H9"></path><path d="m12 9-3 3 3 3"></path></svg>`;
      case "funnel":
        return `<svg ${common}><path d="M4 5h16l-6 7v5l-4 2v-7L4 5z"></path></svg>`;
      case "label":
        return `<svg ${common}><path d="M4 7.5A2.5 2.5 0 0 1 6.5 5H20l-4 7 4 7H6.5A2.5 2.5 0 0 1 4 16.5v-9z"></path><path d="M8 10h5M8 14h4"></path></svg>`;
      case "template":
        return `<svg ${common}><path d="M5 5h14v14H5z"></path><path d="M9 5v14M5 10h14"></path></svg>`;
      default:
        return "";
    }
  };

  const renderCanvasToolIcon = (toolItem: Element, label: string | undefined) => {
    const iconName = toolIconName(label);
    if (!iconName) {
      return;
    }
    const displayLabel = label ? translateNifiTooltip(label) : label;
    toolItem.setAttribute(NIFI_TOOL_ICON_ATTRIBUTE, iconName);
    toolItem.setAttribute(NIFI_TOOL_LABEL_ATTRIBUTE, displayLabel ?? label ?? "");
    toolItem.setAttribute("aria-label", displayLabel ?? label ?? "");
    toolItem.removeAttribute("title");
    toolItem.querySelectorAll("[title]").forEach((child) => child.removeAttribute("title"));
    toolItem.querySelectorAll(":scope > .cerebro-tool-icon").forEach((icon) => icon.remove());
    toolItem.insertAdjacentHTML("afterbegin", toolIconSvg(iconName));
  };

  const keepCanvasToolVisible = (toolItem: Element) => {
    const label = toolLabel(toolItem)
      ?? Array.from(toolItem.querySelectorAll("[title], [aria-label], [data-tooltip], [matTooltip], [mattooltip], [tooltip]"))
        .map(toolLabel)
        .find((value): value is string => Boolean(value));
    toolItem.setAttribute(NIFI_TOOL_ALLOWED_ATTRIBUTE, "true");
    toolItem.removeAttribute(NIFI_TOOL_HIDDEN_ATTRIBUTE);
    renderCanvasToolIcon(toolItem, label);
    let current = toolItem.parentElement;
    for (let depth = 0; depth < 5 && current && current !== doc.body; depth += 1) {
      if (hasToolLabel(current, ALLOWED_NIFI_CANVAS_TOOLS)) {
        current.removeAttribute(NIFI_TOOL_HIDDEN_ATTRIBUTE);
      }
      current = current.parentElement;
    }
  };

  const closestCanvasToolItem = (element: Element) => {
    let current: Element | null = element;
    for (let depth = 0; depth < 5 && current && current !== doc.body; depth += 1) {
      const rect = current.getBoundingClientRect();
      const role = current.getAttribute("role");
      const tagName = current.tagName.toLowerCase();
      const isButtonLike = tagName === "button" || tagName === "a" || role === "button" || current.hasAttribute("title");
      if (isButtonLike && rect.width > 0 && rect.height > 0 && rect.width <= 96 && rect.height <= 72) {
        return current;
      }
      current = current.parentElement;
    }
    return element;
  };

  const markCanvasToolBar = (toolItem: Element) => {
    let current: Element | null = toolItem.parentElement;
    for (let depth = 0; depth < 8 && current && current !== doc.body; depth += 1) {
      const rect = current.getBoundingClientRect();
      const titleCount = current.querySelectorAll("[title], [aria-label], [data-tooltip], [matTooltip], [mattooltip], [tooltip]").length;
      const nearTop = rect.top >= -4 && rect.top <= 80;
      if (nearTop && rect.width >= 160 && rect.height > 0 && rect.height <= 96 && titleCount >= 2) {
        current.setAttribute(NIFI_TOP_TOOLBAR_ATTRIBUTE, "true");
        if (!current.hasAttribute(NIFI_TOP_TOOLBAR_EXPANDED_ATTRIBUTE)) {
          current.setAttribute(NIFI_TOP_TOOLBAR_EXPANDED_ATTRIBUTE, "true");
          doc.documentElement.setAttribute(NIFI_RECENT_PANEL_ATTRIBUTE, "submenu");
        }
      }
      current = current.parentElement;
    }
  };

  const patchCanvasToolVisibility = (element: Element) => {
    const label = toolLabel(element);
    const classNames = elementClassName(element).split(/\s+/);
    const menuIcon = classNames.some((className) => NIFI_MENU_ICON_CLASSES.includes(className))
      || Boolean(element.querySelector(NIFI_MENU_ICON_CLASSES.map((className) => `.${className}`).join(", ")));
    if (!label && !menuIcon) {
      return;
    }

    const toolItem = closestCanvasToolItem(element);
    const itemRect = toolItem.getBoundingClientRect();
    const alreadyManagedTool = toolItem.getAttribute(NIFI_TOOL_ALLOWED_ATTRIBUTE) === "true"
      || toolItem.getAttribute(NIFI_TOOL_HIDDEN_ATTRIBUTE) === "true"
      || toolItem.getAttribute(NIFI_MENU_BUTTON_ATTRIBUTE) === "true";
    if (!alreadyManagedTool && (itemRect.top < -4 || itemRect.top > 96)) {
      return;
    }

    if (menuIcon) {
      toolItem.setAttribute(NIFI_MENU_BUTTON_ATTRIBUTE, "true");
      toolItem.removeAttribute(NIFI_TOOL_HIDDEN_ATTRIBUTE);
      markCanvasToolBar(toolItem);
      return;
    }

    if (label && HIDDEN_NIFI_CANVAS_TOOLS.has(label)) {
      const hideTarget = hasToolLabel(toolItem, ALLOWED_NIFI_CANVAS_TOOLS) ? element : toolItem;
      hideTarget.setAttribute(NIFI_TOOL_HIDDEN_ATTRIBUTE, "true");
      markCanvasToolBar(toolItem);
      return;
    }

    if (label && ALLOWED_NIFI_CANVAS_TOOLS.has(label)) {
      keepCanvasToolVisible(toolItem);
      markCanvasToolBar(toolItem);
    }
  };

  const patchCanvasBreadcrumb = () => {
    const candidates = Array.from(doc.querySelectorAll("nav, ol, ul, div, span, a, button"))
      .filter((element) => {
        if (element.getAttribute(NIFI_BREADCRUMB_ATTRIBUTE) === "true") {
          return true;
        }
        const rect = element.getBoundingClientRect();
        if (rect.width <= 0 || rect.height <= 0 || rect.width > 520 || rect.height > 56) {
          return false;
        }
        const text = (element.textContent ?? "").replace(/\s+/g, " ").trim();
        if (!text || text.length > 120) {
          return false;
        }
        const className = elementClassName(element).toLowerCase();
        const aria = (element.getAttribute("aria-label") ?? "").toLowerCase();
        const viewportHeight = doc.defaultView?.innerHeight ?? window.innerHeight;
        const isBottomRootPill = rect.left >= 0
          && rect.left <= 420
          && rect.bottom >= viewportHeight - 72
          && (text === "Root" || text === "ETL Root");
        return className.includes("breadcrumb")
          || aria.includes("breadcrumb")
          || isBottomRootPill
          || text === "Root"
          || text === "ETL Root"
          || text.startsWith("Root /")
          || text.startsWith("Root >")
          || text.startsWith("ETL Root");
      })
      .sort((a, b) => {
        const ar = a.getBoundingClientRect();
        const br = b.getBoundingClientRect();
        return br.top - ar.top || ar.left - br.left;
      });
    const candidate = candidates[0];
    if (!candidate) {
      return;
    }
    candidate.setAttribute(NIFI_BREADCRUMB_ATTRIBUTE, "true");
    candidate.setAttribute("aria-hidden", "true");
    candidate.setAttribute("hidden", "true");
    if ("style" in candidate) {
      (candidate as HTMLElement).style.setProperty("display", "none", "important");
    }
    const breadcrumbContainer = candidate.parentElement;
    if (breadcrumbContainer && breadcrumbContainer !== doc.body) {
      breadcrumbContainer.setAttribute(NIFI_BREADCRUMB_CONTAINER_ATTRIBUTE, "true");
      breadcrumbContainer.setAttribute("aria-hidden", "true");
      if ("style" in breadcrumbContainer) {
        (breadcrumbContainer as HTMLElement).style.setProperty("display", "none", "important");
      }
    }
    if ((candidate.textContent ?? "").trim() === "Root") {
      candidate.textContent = "ETL Root";
    }
    if (!candidate.querySelector("[data-cerebro-breadcrumb-home='true'], svg, i")) {
      const icon = doc.createElementNS("http://www.w3.org/2000/svg", "svg");
      icon.setAttribute("data-cerebro-breadcrumb-home", "true");
      icon.setAttribute("viewBox", "0 0 24 24");
      icon.setAttribute("fill", "none");
      icon.setAttribute("stroke", "currentColor");
      icon.setAttribute("stroke-width", "2");
      icon.setAttribute("stroke-linecap", "round");
      icon.setAttribute("stroke-linejoin", "round");
      icon.innerHTML = '<path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path><polyline points="9 22 9 12 15 12 15 22"></polyline>';
      candidate.insertBefore(icon, candidate.firstChild);
    }
  };

  const patchContextMenuItemVisibility = (element: Element) => {
    const text = elementText(element);
    if (!HIDDEN_NIFI_CONTEXT_MENU_ITEMS.has(text)) {
      return;
    }
    const item = element.closest<HTMLElement>(
      '[role="menuitem"], .mat-mdc-menu-item, .mat-menu-item, button, a, li',
    );
    if (!item || !isVisibleElement(item)) {
      return;
    }
    item.setAttribute("aria-hidden", "true");
    item.setAttribute("hidden", "true");
    item.style.setProperty("display", "none", "important");
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
    patchCanvasToolVisibility(element);
    patchContextMenuItemVisibility(element);
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
    doc.querySelectorAll(NIFI_MENU_ICON_CLASSES.map((className) => `.${className}`).join(", ")).forEach(patchCanvasToolVisibility);
    doc.querySelectorAll('[role="menuitem"], .mat-mdc-menu-item, .mat-menu-item, button, a, li').forEach(patchContextMenuItemVisibility);
    patchCanvasBreadcrumb();
  };

  patchDocument();

  if (doc.documentElement.getAttribute(KOREAN_TOOLTIP_PATCH_ATTRIBUTE) === "true") {
    return;
  }
  doc.documentElement.setAttribute(KOREAN_TOOLTIP_PATCH_ATTRIBUTE, "true");

  const DocumentMutationObserver = doc.defaultView?.MutationObserver ?? MutationObserver;
  const observer = new DocumentMutationObserver((mutations) => {
    if (hasOpenNifiDialog(doc)) {
      patchProcessorEditDialogText(doc);
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
        node
          .querySelectorAll(NIFI_MENU_ICON_CLASSES.map((className) => `.${className}`).join(", "))
          .forEach(patchCanvasToolVisibility);
      });
    }
    patchCanvasBreadcrumb();
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

  type RecentPanel = "submenu" | "navigation" | "operation";

  const panels = () => Array.from(doc.querySelectorAll<HTMLElement>(`[${NIFI_PANEL_ATTRIBUTE}]`));
  const topToolbar = () => doc.querySelector<HTMLElement>(`[${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"]`);
  const recentPanel = (): RecentPanel =>
    (doc.documentElement.getAttribute(NIFI_RECENT_PANEL_ATTRIBUTE) as RecentPanel | null) ?? "submenu";

  const setRecentPanel = (panel: RecentPanel) => {
    doc.documentElement.setAttribute(NIFI_RECENT_PANEL_ATTRIBUTE, panel);
  };

  const rawPanel = (panel: RecentPanel) => {
    if (panel === "submenu") {
      return null;
    }
    return doc.querySelector<HTMLElement>(panel === "navigation" ? ".navigation-control" : ".operation-control");
  };

  const setPanelOpen = (panel: RecentPanel, open: boolean) => {
    topToolbar()?.setAttribute(NIFI_TOP_TOOLBAR_EXPANDED_ATTRIBUTE, panel === "submenu" && open ? "true" : "false");
    let matchedPanel = false;
    panels().forEach((item) => {
      const name = item.getAttribute(NIFI_PANEL_ATTRIBUTE);
      matchedPanel = matchedPanel || name === panel;
      item.setAttribute(NIFI_PANEL_EXPANDED_ATTRIBUTE, name === panel && open ? "true" : "false");
    });
    const raw = rawPanel(panel);
    if (!matchedPanel && raw) {
      const rawOpen = Boolean(raw.querySelector(":scope > div.w-72"));
      if (rawOpen !== open) {
        (raw.querySelector<HTMLElement>(".navigation-control-expansion, .operation-control-expansion") ?? raw).click();
      }
    }
    if (open) {
      setRecentPanel(panel);
    }
  };

  const isPanelOpen = (panel: RecentPanel) => {
    if (panel === "submenu") {
      return topToolbar()?.getAttribute(NIFI_TOP_TOOLBAR_EXPANDED_ATTRIBUTE) === "true";
    }
    const raw = rawPanel(panel);
    if (raw) {
      return Boolean(raw.querySelector(":scope > div.w-72"));
    }
    return doc.querySelector(`[${NIFI_PANEL_ATTRIBUTE}="${panel}"]`)
      ?.getAttribute(NIFI_PANEL_EXPANDED_ATTRIBUTE) === "true";
  };

  const installRecentPanelToggle = () => {
    if (doc.getElementById(NIFI_RECENT_PANEL_TOGGLE_ID)) {
      return;
    }
    const button = doc.createElement("button");
    button.id = NIFI_RECENT_PANEL_TOGGLE_ID;
    button.type = "button";
    button.title = "최근에 연 패널 열기/닫기";
    button.setAttribute("aria-label", "최근에 연 패널 열기/닫기");
    button.addEventListener("click", (event) => {
      const target = recentPanel();
      setPanelOpen(target, !isPanelOpen(target));
      event.preventDefault();
      event.stopPropagation();
    }, true);
    doc.body.appendChild(button);

    doc.addEventListener("click", (event) => {
      const target = event.target instanceof Element ? event.target : null;
      const toolbar = target?.closest(`[${NIFI_TOP_TOOLBAR_ATTRIBUTE}="true"]`);
      if (toolbar) {
        setRecentPanel("submenu");
        if (toolbar.getAttribute(NIFI_TOP_TOOLBAR_EXPANDED_ATTRIBUTE) !== "true") {
          setPanelOpen("submenu", true);
          event.preventDefault();
          event.stopPropagation();
        }
      }
    }, true);
  };

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
        const isExpanded = panel.getAttribute(NIFI_PANEL_EXPANDED_ATTRIBUTE) === "true";
        const panelName = panel.getAttribute(NIFI_PANEL_ATTRIBUTE) as RecentPanel;
        setPanelOpen(panelName, !isExpanded);
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
    installRecentPanelToggle();
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

interface ProcessorEditLockFrameState {
  readOnly: boolean;
  holderLabel?: string | null;
}

function dialogTextMatchesProcessorEditor(element: Element) {
  const text = elementText(element);
  if (!text || text.length > 20000) {
    return false;
  }
  return /(Edit\s+Processor|프로세서\s*편집|Processor\s*\|)/i.test(text) && /\bApply\b|적용/.test(text);
}

function processorEditType(dialog: Element) {
  const text = elementText(dialog);
  const titleMatch = text.match(/(?:Edit\s+Processor|프로세서\s*편집|Processor)\s*\|\s*([A-Za-z][\w.]*)(?:\s+\d+(?:\.\d+)*)?/i);
  const fromTitle = processorTypeKey(titleMatch?.[1]);
  if (fromTitle) {
    return fromTitle;
  }
  return Object.keys(NIFI_PROCESSOR_PROPERTY_TEXT).find((typeName) => new RegExp(`\\b${typeName}\\b`, "i").test(text));
}

function findProcessorEditDialogs(doc: Document) {
  const selectors = [
    '[role="dialog"]',
    ".cdk-overlay-pane",
    ".mat-dialog-container",
    ".mat-mdc-dialog-container",
    ".processor-configuration",
  ].join(", ");
  return Array.from(doc.querySelectorAll(selectors)).filter((element) => (
    isVisibleElement(element) && dialogTextMatchesProcessorEditor(element)
  ));
}

function processorEditActiveTab(dialog: Element) {
  const selectedTab = dialog.querySelector(
    '[role="tab"][aria-selected="true"], .mat-mdc-tab.mdc-tab--active, .mat-tab-label-active',
  );
  return selectedTab ? elementText(selectedTab) : "";
}

function isProcessorEditTabLabel(element: Element | null) {
  return Boolean(element?.closest(NIFI_PROCESSOR_EDIT_TAB_SELECTOR));
}

function patchProcessorEditDialogText(doc: Document) {
  findProcessorEditDialogs(doc).forEach((dialog) => {
    const activeTab = processorEditActiveTab(dialog);
    const processorType = processorEditType(dialog);
    const allowFieldTranslation = !activeTab || NIFI_PROCESSOR_EDIT_CONTENT_TABS.has(activeTab);
    const allowPropertyTranslation = NIFI_PROCESSOR_EDIT_PROPERTY_TABS.has(activeTab);
    dialog.querySelectorAll(NIFI_TOOLTIP_ATTRIBUTES.map((name) => `[${name}]`).join(", ")).forEach((element) => {
      NIFI_TOOLTIP_ATTRIBUTES.forEach((attributeName) => {
        const value = element.getAttribute(attributeName);
        const translated = value
          ? translateProcessorEditText(
            value,
            allowFieldTranslation || isProcessorEditTabLabel(element),
            allowPropertyTranslation,
            processorType,
          )
          : undefined;
        if (translated && translated !== value) {
          element.setAttribute(attributeName, translated);
        }
      });
    });

    const walker = doc.createTreeWalker(dialog, NodeFilter.SHOW_TEXT, {
      acceptNode: (node) => {
        const parent = node.parentElement;
        const tagName = parent?.tagName.toLowerCase();
        return tagName === "script" || tagName === "style" || tagName === "textarea"
          ? NodeFilter.FILTER_REJECT
          : NodeFilter.FILTER_ACCEPT;
      },
    });
    for (let current = walker.nextNode(); current; current = walker.nextNode()) {
      const node = current as Text;
      const value = node.nodeValue ?? "";
      const trimmed = value.trim();
      if (!trimmed || trimmed.length > 80 || trimmed.includes("\n")) {
        continue;
      }
      const translated = translateProcessorEditText(
        trimmed,
        allowFieldTranslation || isProcessorEditTabLabel(node.parentElement),
        allowPropertyTranslation,
        processorType,
      );
      if (translated) {
        node.nodeValue = value.replace(trimmed, translated);
      }
    }
  });
}

function restoreLockDisabledState(element: Element) {
  if (!element.hasAttribute(NIFI_LOCK_DISABLED_BEFORE_ATTRIBUTE)) {
    return;
  }
  const control = element as HTMLButtonElement | HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement;
  if ("disabled" in control) {
    control.disabled = element.getAttribute(NIFI_LOCK_DISABLED_BEFORE_ATTRIBUTE) === "true";
  }
  element.removeAttribute(NIFI_LOCK_DISABLED_BEFORE_ATTRIBUTE);
  element.removeAttribute("aria-disabled");
}

function applyLockDisabledState(element: Element) {
  const control = element as HTMLButtonElement | HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement;
  if (!("disabled" in control)) {
    return;
  }
  if (!element.hasAttribute(NIFI_LOCK_DISABLED_BEFORE_ATTRIBUTE)) {
    element.setAttribute(NIFI_LOCK_DISABLED_BEFORE_ATTRIBUTE, String(control.disabled));
  }
  control.disabled = true;
  element.setAttribute("aria-disabled", "true");
}

function restoreLockReadonlyState(element: Element) {
  if (!element.hasAttribute(NIFI_LOCK_READONLY_BEFORE_ATTRIBUTE)) {
    return;
  }
  const control = element as HTMLInputElement | HTMLTextAreaElement;
  if ("readOnly" in control) {
    control.readOnly = element.getAttribute(NIFI_LOCK_READONLY_BEFORE_ATTRIBUTE) === "true";
  }
  element.removeAttribute(NIFI_LOCK_READONLY_BEFORE_ATTRIBUTE);
}

function applyLockReadonlyState(element: Element) {
  const control = element as HTMLInputElement | HTMLTextAreaElement;
  if (!("readOnly" in control)) {
    applyLockDisabledState(element);
    return;
  }
  if (!element.hasAttribute(NIFI_LOCK_READONLY_BEFORE_ATTRIBUTE)) {
    element.setAttribute(NIFI_LOCK_READONLY_BEFORE_ATTRIBUTE, String(control.readOnly));
  }
  control.readOnly = true;
}

function isApplyButton(element: Element) {
  const label = [
    elementText(element),
    element.getAttribute("aria-label") ?? "",
    element.getAttribute("title") ?? "",
  ].join(" ").replace(/\s+/g, " ").trim();
  return /(^|\s)(Apply|적용)(\s|$)/i.test(label);
}

function patchProcessorEditReadonly(doc: Document) {
  const readOnly = doc.documentElement.getAttribute(NIFI_READ_ONLY_LOCK_ATTRIBUTE) === "true";
  const holderLabel = doc.documentElement.getAttribute(NIFI_READ_ONLY_HOLDER_ATTRIBUTE) ?? "";
  const dialogs = findProcessorEditDialogs(doc);

  if (!readOnly) {
    doc.querySelectorAll(`#${NIFI_READ_ONLY_NOTICE_ID}`).forEach((element) => element.remove());
    doc.querySelectorAll(`[${NIFI_LOCK_DISABLED_BEFORE_ATTRIBUTE}]`).forEach(restoreLockDisabledState);
    doc.querySelectorAll(`[${NIFI_LOCK_READONLY_BEFORE_ATTRIBUTE}]`).forEach(restoreLockReadonlyState);
    doc.querySelectorAll(`[${NIFI_LOCK_DIALOG_ATTRIBUTE}]`).forEach((element) => {
      element.removeAttribute(NIFI_LOCK_DIALOG_ATTRIBUTE);
    });
    return;
  }

  dialogs.forEach((dialog) => {
    dialog.setAttribute(NIFI_LOCK_DIALOG_ATTRIBUTE, "true");
    let notice = dialog.querySelector(`#${NIFI_READ_ONLY_NOTICE_ID}`);
    if (!notice) {
      notice = doc.createElement("div");
      notice.id = NIFI_READ_ONLY_NOTICE_ID;
      dialog.insertBefore(notice, dialog.firstChild);
    }
    notice.textContent = holderLabel
      ? `${holderLabel} 사용자가 같은 Processor를 편집 중입니다. 현재 창은 읽기 전용이며 Apply를 사용할 수 없습니다.`
      : "다른 사용자가 같은 Processor를 편집 중입니다. 현재 창은 읽기 전용이며 Apply를 사용할 수 없습니다.";

    dialog.querySelectorAll("input, textarea").forEach(applyLockReadonlyState);
    dialog.querySelectorAll("select").forEach(applyLockDisabledState);
    dialog.querySelectorAll("button, [role='button']").forEach((button) => {
      if (isApplyButton(button)) {
        applyLockDisabledState(button);
      }
    });
  });
}

function installProcessorEditReadonlyGuard(frame: HTMLIFrameElement, state: ProcessorEditLockFrameState) {
  const doc = iframeDocument(frame);
  const frameWindow = frame.contentWindow;
  if (!doc || !frameWindow) {
    return;
  }

  doc.documentElement.setAttribute(NIFI_READ_ONLY_LOCK_ATTRIBUTE, state.readOnly ? "true" : "false");
  doc.documentElement.setAttribute(NIFI_READ_ONLY_HOLDER_ATTRIBUTE, state.holderLabel ?? "");
  patchProcessorEditReadonly(doc);

  if (doc.documentElement.getAttribute(NIFI_READ_ONLY_LOCK_INSTALLED_ATTRIBUTE) === "true") {
    return;
  }
  doc.documentElement.setAttribute(NIFI_READ_ONLY_LOCK_INSTALLED_ATTRIBUTE, "true");

  let frameId: number | null = null;
  const FrameMutationObserver = doc.defaultView?.MutationObserver ?? MutationObserver;
  const observer = new FrameMutationObserver(() => {
    if (frameId !== null) {
      return;
    }
    frameId = frameWindow.requestAnimationFrame(() => {
      frameId = null;
      patchProcessorEditReadonly(doc);
    });
  });

  observer.observe(doc.documentElement, { childList: true, subtree: true });
  frameWindow.addEventListener("beforeunload", () => {
    if (frameId !== null) {
      frameWindow.cancelAnimationFrame(frameId);
    }
    observer.disconnect();
  }, { once: true });
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
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set(["root"]));
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
          setExpandedIds(new Set(["root"]));
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
    let cleanupRequest = () => { };
    const timerId = window.setInterval(() => {
      cleanupRequest();
      cleanupRequest = loadTree(false, false, true);
    }, 5000);
    return () => {
      window.clearInterval(timerId);
      cleanupRequest();
    };
  }, [loadTree]);

  const openGroup = (groupId: string) => {
    navigate(`/etl/manage?processGroupId=${encodeURIComponent(groupId)}`);
  };

  const treeIcon = (node: NifiProcessGroupTreeNode) =>
    node.groupType === "JOB"
      ? <FileOutlined className="nifi-tree-kind nifi-tree-kind--job" />
      : <FolderOutlined className="nifi-tree-kind nifi-tree-kind--group" />;

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

  const highlightedName = (name: string): ReactNode => {
    const query = searchQuery.trim();
    if (!query) {
      return name;
    }

    const normalizedName = name.toLowerCase();
    const normalizedQuery = query.toLowerCase();
    const parts: ReactNode[] = [];
    let cursor = 0;
    let matchIndex = normalizedName.indexOf(normalizedQuery);
    while (matchIndex >= 0) {
      if (matchIndex > cursor) {
        parts.push(name.slice(cursor, matchIndex));
      }
      const end = matchIndex + query.length;
      parts.push(
        <mark key={`${matchIndex}-${end}`} className="nifi-tree-highlight">
          {name.slice(matchIndex, end)}
        </mark>,
      );
      cursor = end;
      matchIndex = normalizedName.indexOf(normalizedQuery, cursor);
    }
    if (cursor < name.length) {
      parts.push(name.slice(cursor));
    }
    return parts.length > 0 ? parts : name;
  };

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

  const renderNode = (node: NifiProcessGroupTreeNode) => {
    const hasChildren = node.children.length > 0;
    const searching = searchQuery.trim().length > 0;
    const expanded = searching || expandedIds.has(node.id);
    const active = activeGroupId === node.id || (!activeGroupId && node.id === "root");
    return (
      <div key={node.id} className={`nifi-tree-node${expanded ? " expanded" : ""}`}>
        <div
          className={active ? "nifi-tree-row active" : "nifi-tree-row"}
        >
          <button
            type="button"
            className={hasChildren ? "nifi-tree-toggle" : "nifi-tree-toggle empty"}
            aria-label={expanded ? `${node.name} 접기` : `${node.name} 펼치기`}
            onClick={() => hasChildren && toggle(node.id)}
          >
            {hasChildren ? (expanded ? "-" : "+") : ""}
          </button>
          <button type="button" className="nifi-tree-label" title={node.name} onClick={() => openGroup(node.id)}>
            {treeIcon(node)}
            <span className="nifi-tree-name">{highlightedName(node.name)}</span>
          </button>
        </div>
        {hasChildren && expanded ? (
          <div className="nifi-tree-sub-group" role="group">
            {node.children.map(renderNode)}
          </div>
        ) : null}
      </div>
    );
  };

  const filteredTree = tree ? filterTree(tree, searchQuery.trim().toLowerCase()) : null;

  return (
    <aside className="nifi-tree-panel" aria-label="ETL 그룹 트리">
      <div className="nifi-tree-search-heading">
        <label className="nifi-tree-section-title" htmlFor="nifi-tree-search">
          <FolderOutlined />
          트리 검색
        </label>
        <div className="nifi-tree-heading-actions">
          <Button
            size="small"
            className="nifi-tree-refresh-button"
            icon={<ReloadOutlined />}
            loading={isRefreshing}
            title="트리 캐시 새로고침"
            aria-label="트리 캐시 새로고침"
            onClick={() => loadTree(true, false, false, true)}
          />
          <Button size="small" className="nifi-tree-collapse-button" onClick={() => setExpandedIds(new Set())}>
            전체 접기
          </Button>
        </div>
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

function normalizeProcessGroupIdForTree(
  groupId: string | null,
  tree: NifiProcessGroupTreeNode | null,
  fallbackRootForUnknown = false,
) {
  const value = groupId ?? "root";
  if (value === "root" || tree?.id === value) {
    return "root";
  }
  if (fallbackRootForUnknown && tree && !findTreeNode(tree, value)) {
    return "root";
  }
  return value;
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

function formatDateOnly(value?: string | null) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value.slice(0, 10).replace(/-/g, ".");
  }
  const year = parsed.getFullYear();
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  return `${year}.${month}.${day}`;
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
    // 파일 목록 표시는 보조 기능이라 저장 실패가 업로드 흐름을 막지 않는다.
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
  runCount: number;
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
      { key: "week", label: "일주일", from: weekStart, to: today },
    ],
  };
}

function buildDetailLogSummaries(
  keyword: string,
  runRows: NifiProcessorRun[],
  eventRows: NifiExecutionLogEntry[],
  jobRuns: EtlJobRunResponse[] = [],
): DetailLogSummary[] {
  const { periods } = recentDetailLogPeriods();
  const matchedRuns = runRows.filter((row) => matchesRunLogKeyword(row, keyword));
  const matchedEvents = eventRows
    .filter((row) => row.status === "FAILED" && row.level !== "WARNING")
    .filter((row) => matchesExecutionLogKeyword(row, keyword));
  return periods.map((period) => ({
    ...period,
    runCount: jobRuns.length > 0 ? jobRuns
      .filter((row) => {
        const dateKey = logRowDateKey(row.startedAt);
        return dateKey >= period.from && dateKey <= period.to;
      })
      .length : matchedRuns.filter((row) => {
        const dateKey = logRowDateKey(row.startedAt);
        return dateKey >= period.from && dateKey <= period.to;
      }).length,
    processedCount: jobRuns.length > 0 ? jobRuns
      .filter((row) => {
        const dateKey = logRowDateKey(row.startedAt);
        return dateKey >= period.from && dateKey <= period.to;
      })
      .reduce((sum, row) => sum + row.totalInserted, 0) : matchedRuns
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
            {formatCount(item.runCount)}번실행, 적재{" "}
            <button
              type="button"
              className="nifi-detail-link"
              onClick={() => onNavigate(detailLogUrl(item, "runs", keyword))}
            >
              {formatCount(item.processedCount)}
            </button>
            건, ERROR{" "}
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

function groupDisplayName(node?: NifiProcessGroupTreeNode | null) {
  if (!node) {
    return null;
  }
  return node.id === "root" || node.name === "Root" ? "ETL Root" : node.name;
}

function fullGroupPath(path?: NifiProcessGroupTreeNode[]) {
  if (!path?.length) {
    return "ETL Root";
  }
  const names = path.map((node, index) => index === 0 ? "ETL Root" : groupDisplayName(node)).filter(Boolean);
  return names.join(" / ");
}

type RelatedWorkflow = Pick<WorkflowDetail, "id" | "name" | "dagId" | "workflowKey" | "published" | "scheduleCron">;

function workflowUsesJob(workflow: WorkflowDetail, job: EtlJobResponse) {
  return workflow.nodes.some((node) =>
    node.nodeType === "JOB"
    && (node.jobId === job.id || (!!node.nifiPgId && node.nifiPgId === job.nifiPgId)),
  );
}

function sortRunsLatestFirst(runs: EtlJobRunResponse[]) {
  return runs.slice().sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime());
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

function processorTypeIncludes(step: EtlJobStepView | null, typeName: string) {
  return (step?.stepType ?? "").toLowerCase().includes(typeName.toLowerCase());
}

function listFileInputDirectory(step?: EtlJobStepView | null) {
  if (!step || !processorTypeIncludes(step, "ListFile")) {
    return null;
  }
  return pickProperty(stepProperties(step), ["Input Directory", "input-directory"]);
}

function fileSourceText(inputDirectory: string | null, files: string[]) {
  const base = inputDirectory?.trim() || "-";
  if (files.length === 0) {
    return base;
  }
  return [base, ...files.map((fileName) => `- ${fileName}`)].join("\n");
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

function shortProcessorType(type?: string | null) {
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
  onEditLockChange?: (state: ProcessorEditLockFrameState) => void;
}

function ProcessorDetailPanel({ processorId, onParentGroupFound, onEditLockChange }: ProcessorDetailPanelProps) {
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
    onEditLockChange?.({
      readOnly: isReadOnlyByLock,
      holderLabel: editLock ? `${editLock.lockedByUserNm}(${editLock.lockedByUserId})` : null,
    });
  }, [
    editLock?.lockedByUserId,
    editLock?.lockedByUserNm,
    isReadOnlyByLock,
    onEditLockChange,
  ]);

  useEffect(() => {
    return () => {
      onEditLockChange?.({ readOnly: false, holderLabel: null });
    };
  }, [onEditLockChange, processorId]);

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
 * 워크플로우 설계 캔버스의 job 노드 속성창 본문. ETL 관리 화면 job 속성창(ProcessGroupDetailPanel)과 같은
 * 구역(정보 · 테이블 · 실행로그)과 같은 계산을 쓰고, 연결 DAG 만 뺀다(2026-09-11 요청).
 * 글꼴·크기는 캔버스 속성창 것을 그대로 쓴다(제목 앞 ■·굵은 글씨 없음) - 모양은 index.css .wf-node-detail.
 *
 * <p>2026-09-03 에는 캔버스 패널을 짧게 하려고 «대상»·«로그» 구역을 뺐었는데, 두 화면의 job 속성이 달라
 * 보이지 않게 다시 같은 구성으로 맞췄다. 계산이 두 벌이 되지 않도록 이 파일의 도우미(statusText ·
 * fullGroupPath · sourceTargetText · buildDetailLogSummaries …)를 그대로 쓴다. 파일 추가 같은 조작은
 * ETL 관리 화면에만 둔다 - 캔버스에서는 보기만 한다.
 *
 * @param onNavigate 실행로그 숫자를 눌렀을 때의 이동. 캔버스는 저장 안 한 변경 확인(leaveTo)을 거쳐야 한다.
 */
export function EtlJobSummary({ jobId, nifiPgId, onNavigate }: {
  jobId: number;
  nifiPgId?: string | null;
  onNavigate?: (path: string) => void;
}) {
  const navigate = useNavigate();
  const go = onNavigate ?? navigate;
  const [detail, setDetail] = useState<EtlJobDetailResponse | null>(null);
  const [runs, setRuns] = useState<EtlJobRunResponse[]>([]);
  const [group, setGroup] = useState<ReturnType<typeof findTreeNode>>(null);
  const [loading, setLoading] = useState(true);
  const [failed, setFailed] = useState(false);
  const [logSummaries, setLogSummaries] = useState<DetailLogSummary[]>([]);
  const [logsLoading, setLogsLoading] = useState(false);

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

  // 상태·경로는 job 응답만으로는 모자라 NiFi 프로세스 그룹 트리를 함께 본다(ETL 관리 화면과 같다).
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
  const logKeyword = detailLogKeyword(job, groupNode);

  // 실행로그: ETL 관리 화면과 같게 최근 일주일치 NiFi 실행·오류 기록과 이 job 의 실행 기록을 모은다.
  useEffect(() => {
    let cancelled = false;
    setLogSummaries([]);
    if (!job) {
      setLogsLoading(false);
      return () => { cancelled = true; };
    }
    const { queryFrom, queryTo } = recentDetailLogPeriods();
    setLogsLoading(true);
    Promise.all([listNifiProcessorRuns(queryFrom, queryTo), listNifiExecutionLogs(queryFrom, queryTo)])
      .then(([runRows, eventRows]) => {
        if (!cancelled) {
          setLogSummaries(buildDetailLogSummaries(logKeyword, runRows, eventRows, runs));
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
    return () => { cancelled = true; };
  }, [job?.id, logKeyword, runs]);

  if (loading) {
    return <div className="nifi-detail-message">불러오는 중</div>;
  }
  if (failed || !job) {
    return <div className="nifi-detail-message error">조회 실패</div>;
  }

  const steps = detail?.steps ?? [];
  const links = detail?.links ?? [];
  const firstStep = firstStartProcessor(steps, links);
  const listFileStep = steps.find((step) => processorTypeIncludes(step, "ListFile")) ?? null;
  const inputDirectory = listFileInputDirectory(listFileStep);
  const sourceText = processorTypeIncludes(firstStep, "ListFile")
    ? fileSourceText(inputDirectory, loadTodayAddedFiles(addedFileStorageKey(job.id, inputDirectory)))
    : sourceTargetText(firstStep, "source");
  const targetText = sourceTargetText(lastTerminalProcessor(steps, links), "target");
  const latestRun = sortRunsLatestFirst(runs)[0] ?? null;
  const lastRunDate = formatDateOnly(latestRun?.endedAt ?? latestRun?.startedAt ?? job.lastSyncedAt);

  return (
    <div className="etl-job-summary">
      <section className="nifi-detail-section">
        <h3>정보</h3>
        <dl>
          <div>
            <dt>상태</dt>
            <dd className={statusClass(job, groupNode)}>{statusText(job, groupNode)}</dd>
          </div>
          <div>
            <dt>경로</dt>
            <dd>{fullGroupPath(group?.path)}</dd>
          </div>
        </dl>
      </section>
      <section className="nifi-detail-section">
        <h3>테이블</h3>
        <dl className="nifi-detail-target-list">
          <div>
            <dt>소스테이블</dt>
            <dd className="nifi-detail-inline-value nifi-detail-preline" title={sourceText}>{sourceText}</dd>
          </div>
          <div>
            <dt>타깃테이블</dt>
            <dd className="nifi-detail-inline-value" title={targetText}>{targetText}</dd>
          </div>
        </dl>
      </section>
      <section className="nifi-detail-section">
        <h3>실행로그</h3>
        <div className="nifi-detail-last-run-date">{lastRunDate}</div>
        <DetailLogRows loading={logsLoading} summaries={logSummaries} keyword={logKeyword} onNavigate={go} />
      </section>
    </div>
  );
}

function ProcessGroupDetailPanel({ activeGroupId, tree, reloadKey }: ProcessGroupDetailPanelProps) {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState<"detail" | "ai">("detail");
  const [aiAssistantEnabled, setAiAssistantEnabled] = useState(false);
  /** AI 어시스턴트 탭을 연 그룹. 상세정보 탭으로 돌아가도 이 그룹의 대화는 숨겨 둔 채 남긴다(다시 열 때 요약을 또 부르지 않게). */
  const [aiOpenedGroupId, setAiOpenedGroupId] = useState<string | null>(null);
  const [jobs, setJobs] = useState<EtlJobResponse[]>([]);
  const [details, setDetails] = useState<EtlJobDetailResponse[]>([]);
  const [runs, setRuns] = useState<EtlJobRunResponse[]>([]);
  const [logSummaries, setLogSummaries] = useState<DetailLogSummary[]>([]);
  const [relatedWorkflows, setRelatedWorkflows] = useState<RelatedWorkflow[]>([]);
  const [workflowLoading, setWorkflowLoading] = useState(false);
  const [workflowModalOpen, setWorkflowModalOpen] = useState(false);
  const [logsLoading, setLogsLoading] = useState(false);
  const [jobsLoading, setJobsLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [isAddingFiles, setIsAddingFiles] = useState(false);
  const [addedFiles, setAddedFiles] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const addFileInputRef = useRef<HTMLInputElement | null>(null);

  const selected = useMemo(() => {
    const found = findTreeNode(tree, activeGroupId);
    if (found || activeGroupId || !tree) {
      return found;
    }
    return { node: tree, path: [tree] };
  }, [activeGroupId, tree]);
  const selectedGroupId = activeGroupId ?? selected?.node.id ?? "root";
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
  const latestRun = useMemo(() => sortRunsLatestFirst(runs)[0] ?? null, [runs]);
  const airflowDags = uniqueValues(groupJobs.map((entry) => entry.airflowDagId));
  const isLoading = jobsLoading || detailLoading;

  useEffect(() => {
    let cancelled = false;
    getLlmStatus()
      .then((status) => {
        if (cancelled) {
          return;
        }
        setAiAssistantEnabled(status.enabled);
      })
      .catch(() => {
        if (!cancelled) {
          setAiAssistantEnabled(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (activeTab === "ai") {
      setAiOpenedGroupId(selectedGroupId);
    }
  }, [activeTab, selectedGroupId]);

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
  const isRootGroup = selectedGroupId === "root";
  const isJobGroup = displayNode?.groupType === "JOB";
  const isGroupingGroup = displayNode?.groupType === "GROUPING";
  const displayPath = fullGroupPath(selected?.path);
  const displayDetail = details.find((entry) => entry.job.nifiPgId === selectedGroupId)
    ?? (displayJob ? details.find((entry) => entry.job.id === displayJob.id) : null)
    ?? null;
  const jobSteps = displayDetail?.steps ?? [];
  const jobLinks = displayDetail?.links ?? [];
  const firstStep = firstStartProcessor(jobSteps, jobLinks);
  const isListFileSource = processorTypeIncludes(firstStep, "ListFile");
  const sourceStep = firstStep;
  const targetStep = lastTerminalProcessor(jobSteps, jobLinks);
  const listFileStep = jobSteps.find((step) => processorTypeIncludes(step, "ListFile")) ?? null;
  const inputDirectory = listFileInputDirectory(listFileStep);
  const sourceText = isListFileSource ? fileSourceText(inputDirectory, addedFiles) : sourceTargetText(sourceStep, "source");
  const targetText = sourceTargetText(targetStep, "target");
  const dagId = relatedWorkflows.length === 1 ? relatedWorkflows[0].dagId : relatedJob?.airflowDagId ?? airflowDags[0] ?? null;
  const directGroupingGroups = isGroupingGroup
    ? (displayNode?.children ?? []).filter((entry) => entry.groupType === "GROUPING")
    : [];
  const showGroupingJobStatus = directGroupingGroups.length > 0;
  const lastRunDate = formatDateOnly(latestRun?.endedAt ?? latestRun?.startedAt ?? relatedJob?.lastSyncedAt);
  const logKeyword = detailLogKeyword(displayJob, displayNode);
  const addedFilesStorageKey = addedFileStorageKey(displayJob?.id, inputDirectory);
  const titleName = displayJob?.jobName ?? groupDisplayName(displayNode) ?? (isRootGroup ? "ETL Root" : "선택 없음");
  const headerTitle = isJobGroup
    ? `Job : ${titleName}`
    : isGroupingGroup || isRootGroup
      ? `Group : ${titleName}`
      : titleName;
  const aiAssistantAvailable = aiAssistantEnabled && !!selectedGroupId;
  /**
   * AI 어시스턴트 대상(2026-10-10): 워크플로우 속성창과 같은 대화 창(GptChatPanel)을 쓴다 - 탭을 열면 GPT 가 요약을 쓴다.
   * 예전에 화면이 직접 만들던 요약(한 줄 요약 · 구성 · 지금 상태 · 등록 및 변경, 그룹의 Job 상위 5개)에 있던 값은
   * 서버(EtlAssistantContextCollector · EtlJobFacts)가 같은 규칙으로 모아 보내므로 답에 그대로 나온다. 화면은 대상 식별자만 보낸다.
   */
  const aiTarget = useMemo<Record<string, string>>(() => ({
    selectedType: isJobGroup && displayJob ? "JOB" : "PROCESS_GROUP",
    selectedId: displayJob?.nifiPgId ?? selectedGroupId,
    selectedName: titleName,
    currentPath: displayPath,
    ...(displayJob ? {
      jobId: String(displayJob.id),
      jobGroupId: displayJob.nifiPgId,
      createdBy: displayNode?.createdBy ?? "",
    } : {}),
  }), [displayJob, displayNode?.createdBy, displayPath, isJobGroup, selectedGroupId, titleName]);
  // 대상이 바뀌면 대화를 새로 시작한다(GptChatPanel 은 처음 그릴 때 요약을 요청한다). job 목록을 받은 뒤에 그린다 - 받는 중에 그리면
  // job 이 아닌 그룹으로 요약을 한 번 부른 뒤 다시 부르게 된다.
  const aiPanelKey = `${selectedGroupId}:${displayJob?.id ?? ""}`;
  const showAiPanel = aiAssistantAvailable && !jobsLoading && (activeTab === "ai" || aiOpenedGroupId === selectedGroupId);

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
        setLogSummaries(buildDetailLogSummaries(logKeyword, runRows, eventRows, runs));
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
  }, [displayJob?.id, isJobGroup, logKeyword, runs]);

  useEffect(() => {
    let cancelled = false;
    setRelatedWorkflows([]);
    setWorkflowModalOpen(false);
    if (!isJobGroup || !displayJob) {
      setWorkflowLoading(false);
      return () => {
        cancelled = true;
      };
    }

    setWorkflowLoading(true);
    listWorkflows()
      .then((workflows) => Promise.all(workflows.map((workflow) => getWorkflow(workflow.id))))
      .then((workflows) => {
        if (!cancelled) {
          setRelatedWorkflows(workflows.filter((workflow) => workflowUsesJob(workflow, displayJob)));
        }
      })
      .catch(() => {
        if (!cancelled) {
          setRelatedWorkflows([]);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setWorkflowLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [displayJob?.id, displayJob?.nifiPgId, isJobGroup]);

  const openWorkflow = (workflow: RelatedWorkflow) => {
    setWorkflowModalOpen(false);
    navigate(`/workflows/design/${workflow.id}`);
  };

  const openDagTarget = () => {
    if (relatedWorkflows.length === 1) {
      openWorkflow(relatedWorkflows[0]);
      return;
    }
    if (relatedWorkflows.length > 1) {
      setWorkflowModalOpen(true);
    }
  };

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
    <>
      <aside
        className={`nifi-detail-panel${activeTab === "ai" ? " nifi-detail-panel--ai" : ""}`}
        aria-label="선택한 프로세스 그룹 상세"
      >
        <div className="nifi-detail-header">
          <div className="nifi-detail-title" title={headerTitle}>
            {headerTitle}
          </div>
        </div>

        {!isLoading && error ? <div className="nifi-detail-message error">조회 실패</div> : null}

        <div className="nifi-detail-tabs" role="tablist" aria-label="상세 패널 탭">
          <button
            type="button"
            className={`nifi-detail-tab${activeTab === "detail" ? " active" : ""}`}
            role="tab"
            aria-selected={activeTab === "detail"}
            onClick={() => setActiveTab("detail")}
          >
            상세정보
          </button>
          {aiAssistantEnabled ? (
            <button
              type="button"
              className={`nifi-detail-tab${activeTab === "ai" ? " active" : ""}`}
              role="tab"
              aria-selected={activeTab === "ai"}
              disabled={!aiAssistantAvailable}
              title={aiAssistantAvailable ? "AI 어시스턴트" : "AI 어시스턴트를 사용할 수 없습니다."}
              onClick={() => {
                if (aiAssistantAvailable) {
                  setActiveTab("ai");
                }
              }}
            >
              AI 어시스턴트
            </button>
          ) : null}
        </div>

        {activeTab === "detail" ? (
          <>
            <section className="nifi-detail-section">
              <h3>■ 정보</h3>
              <dl>
                <div>
                  <dt>상태</dt>
                  <dd className={statusClass(displayJob, displayNode)}>{statusText(displayJob, displayNode)}</dd>
                </div>
                <div>
                  <dt>경로</dt>
                  <dd>{displayPath}</dd>
                </div>
                {isJobGroup ? (
                  <div>
                    <dt>DAG</dt>
                    <dd>
                      {workflowLoading ? (
                        "불러오는 중"
                      ) : relatedWorkflows.length > 0 ? (
                        <button type="button" className="nifi-detail-link" onClick={openDagTarget}>
                          {relatedWorkflows.length === 1
                            ? `${relatedWorkflows[0].name} (${relatedWorkflows[0].dagId})`
                            : `${relatedWorkflows.length}개 워크플로우`}
                        </button>
                      ) : dagId ? (
                        dagId
                      ) : (
                        "-"
                      )}
                    </dd>
                  </div>
                ) : null}
              </dl>
            </section>

            {showGroupingJobStatus ? (
              <section className="nifi-detail-section">
                <h3>JOB 상태</h3>
                <div className="nifi-direct-status-list">
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
                </div>
              </section>
            ) : null}

            {isJobGroup ? (
              <>
                <section className="nifi-detail-section">
                  <h3>■ 테이블</h3>
                  <dl className="nifi-detail-target-list">
                    <div>
                      <dt>소스테이블</dt>
                      <dd className="nifi-detail-inline-value nifi-detail-preline" title={sourceText}>
                        {sourceText}
                      </dd>
                    </div>
                    <div>
                      <dt>타깃테이블</dt>
                      <dd className="nifi-detail-inline-value" title={targetText}>
                        {targetText}
                      </dd>
                    </div>
                  </dl>
                  {inputDirectory ? (
                    <div className="nifi-file-add-panel">
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
                              - {fileName}
                            </li>
                          ))}
                        </ul>
                      ) : null}
                    </div>
                  ) : null}
                </section>

                <section className="nifi-detail-section">
                  <h3>■ 실행로그</h3>
                  <div className="nifi-detail-last-run-date">{lastRunDate}</div>
                  <DetailLogRows
                    loading={logsLoading}
                    summaries={logSummaries}
                    keyword={logKeyword}
                    onNavigate={navigate}
                  />
                </section>
              </>
            ) : null}
          </>
        ) : null}
        {/* AI 어시스턴트: 워크플로우 속성창과 같은 대화 창(2026-10-10 요청) - 윗줄(GPT 이름 · 프롬프트 편집 · MD 저장하기),
            탭을 열면 «ETL 정보 요약해줘» 요약을 GPT 가 쓰고(«답변을 준비하고 있습니다.»), 이어서 묻는다.
            상세정보 탭으로 돌아가도 같은 그룹이면 숨겨 둔 채 대화를 남긴다(display: contents 로 감싸 배치는 그대로). */}
        {showAiPanel ? (
          <div className="nifi-ai-panel-host" style={{ display: activeTab === "ai" ? "contents" : "none" }}>
            <GptChatPanel key={aiPanelKey} targetType="ETL_ASSISTANT" target={aiTarget} />
          </div>
        ) : null}
      </aside>

      <Modal
        title="연관 워크플로우"
        open={workflowModalOpen}
        footer={null}
        onCancel={() => setWorkflowModalOpen(false)}
      >
        <div className="nifi-related-workflow-list">
          {relatedWorkflows.map((workflow) => (
            <button
              key={workflow.id}
              type="button"
              className="nifi-related-workflow-row"
              onClick={() => openWorkflow(workflow)}
            >
              <strong>{workflow.name}</strong>
              <span>{workflow.dagId}</span>
            </button>
          ))}
        </div>
      </Modal>
    </>
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
  const selectedRouteGroupId = processGroupId ?? "root";
  const [selectedProcessGroupId, setSelectedProcessGroupId] = useState<string | null>(selectedRouteGroupId);
  const [selectedCanvasItem, setSelectedCanvasItem] = useState<CanvasSelection | null>(
    { type: "processGroup", id: selectedRouteGroupId },
  );
  const [isTreePanelCollapsed, setIsTreePanelCollapsed] = useState(false);
  const [isDetailPanelCollapsed, setIsDetailPanelCollapsed] = useState(false);
  const [frameChromeReady, setFrameChromeReady] = useState(!showProcessGroupTree);
  const [detailPanelWidth, setDetailPanelWidth] = useState(NIFI_DETAIL_PANEL_DEFAULT_WIDTH);
  const [nifiProcessorDialogOpen, setNifiProcessorDialogOpen] = useState(false);
  const [jobCatalogReloadKey, setJobCatalogReloadKey] = useState(0);
  const processGroupTreeRef = useRef<NifiProcessGroupTreeNode | null>(null);
  const processGroupIdRef = useRef<string | null>(null);
  const frameRef = useRef<HTMLIFrameElement | null>(null);
  const [processorEditLockFrameState, setProcessorEditLockFrameState] = useState<ProcessorEditLockFrameState>({
    readOnly: false,
    holderLabel: null,
  });
  const frameSrc = processGroupId ? withProcessGroupId(src, processGroupId) : src;
  const frameInstanceKey = `${frameKey}:${frameSrc}`;
  processGroupTreeRef.current = processGroupTree;
  processGroupIdRef.current = selectedRouteGroupId;

  useEffect(() => {
    const normalizedGroupId = normalizeProcessGroupIdForTree(
      selectedRouteGroupId,
      processGroupTreeRef.current,
      selectedRouteGroupId === "root",
    );
    setSelectedProcessGroupId(normalizedGroupId);
    setSelectedCanvasItem({ type: "processGroup", id: normalizedGroupId });
  }, [selectedRouteGroupId]);

  useEffect(() => {
    if (selectedRouteGroupId !== "root") {
      return;
    }
    const normalizedGroupId = normalizeProcessGroupIdForTree(selectedProcessGroupId, processGroupTree, true);
    if (normalizedGroupId === selectedProcessGroupId) {
      return;
    }
    setSelectedProcessGroupId(normalizedGroupId);
    setSelectedCanvasItem((selection) => (
      selection?.type === "processGroup" ? { type: "processGroup", id: normalizedGroupId } : selection
    ));
  }, [processGroupTree, selectedProcessGroupId, selectedRouteGroupId]);

  useEffect(() => {
    setFrameChromeReady(!showProcessGroupTree);
    setNifiProcessorDialogOpen(false);
  }, [frameKey, frameSrc, showProcessGroupTree]);

  useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      if (event.source !== frameRef.current?.contentWindow) {
        return;
      }
      const data = event.data;
      if (!data || typeof data !== "object" || data.type !== "cerebro:nifi-processor-dialog") {
        return;
      }
      setNifiProcessorDialogOpen(Boolean(data.open));
    };

    window.addEventListener("message", handleMessage);
    return () => window.removeEventListener("message", handleMessage);
  }, []);

  useEffect(() => {
    if (!showProcessGroupTree || frameChromeReady) {
      return;
    }

    const timerId = window.setTimeout(() => {
      const frame = frameRef.current;
      // 아직 다 안 읽힌 문서(빈 about:blank, 읽는 중인 NiFi)면 Cerebro 판인지 알 수 없다 - 덧칠은 onLoad(handleFrameLoad)에 맡긴다.
      // 여기서 걸면 Cerebro 판 화면에도 덧칠(1초 주기 훑기)이 붙는다(2026-10-05 확인).
      if (frame && isFrameDocumentLoaded(frame) && !isCerebroNifiUi(frame)) {
        hideToolChrome(frame);
        patchKoreanTooltips(frame);
        installNifiPanelLayout(frame);
      }
      setFrameChromeReady(true);
    }, 1500);

    return () => window.clearTimeout(timerId);
  }, [frameChromeReady, frameInstanceKey, showProcessGroupTree]);

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

  const handleProcessorEditLockChange = useCallback((state: ProcessorEditLockFrameState) => {
    setProcessorEditLockFrameState(state);
    if (frameRef.current) {
      installProcessorEditReadonlyGuard(frameRef.current, state);
    }
  }, []);

  const selectCanvasItem = (selection: CanvasSelection) => {
    if (selection.type === "processGroup") {
      const normalizedGroupId = normalizeProcessGroupIdForTree(
        selection.id,
        processGroupTreeRef.current,
        selectedRouteGroupId === "root",
      );
      setSelectedCanvasItem({ type: "processGroup", id: normalizedGroupId });
      setSelectedProcessGroupId(normalizedGroupId);
      return;
    }
    setSelectedCanvasItem(selection);
  };

  const selectCanvasProcessGroupRoute = (groupId: string | null) => {
    const normalizedGroupId = normalizeProcessGroupIdForTree(
      groupId,
      processGroupTreeRef.current,
      selectedRouteGroupId === "root",
    );
    setSelectedProcessGroupId(normalizedGroupId);
    setSelectedCanvasItem({ type: "processGroup", id: normalizedGroupId });
  };

  const handleFrameLoad = (event: SyntheticEvent<HTMLIFrameElement>) => {
    const frame = event.currentTarget;
    frameRef.current = frame;
    // Cerebro 판 NiFi 화면이면 배치 · 한글을 NiFi 가 이미 처리했다 - 덧칠은 건너뛰고 연동(잠금 · 선택 · 경로)만 건다.
    const cerebroNifiUi = isCerebroNifiUi(frame);
    let chromeHidden = true;
    if (!cerebroNifiUi) {
      cleanupLegacyNifiMiniCreateToolbars(frame);
      chromeHidden = hideToolChrome(frame);
      patchKoreanTooltips(frame);
      installNifiPanelLayout(frame);
      installProcessorDialogViewportFit(frame);
    }
    installProcessorEditReadonlyGuard(frame, processorEditLockFrameState);
    clickNifiMenuItem(frame, nifiAutoOpenMenuTitles);
    if (showProcessGroupTree) {
      installCanvasRouteSync(frame, selectCanvasProcessGroupRoute);
      installCanvasSelectionSync(
        frame,
        selectCanvasItem,
        (target) => resolveProcessGroupIdByCanvasText(target, processGroupIdRef.current, processGroupTreeRef.current),
      );
    }
    if (!showProcessGroupTree || cerebroNifiUi) {
      window.requestAnimationFrame(() => setFrameChromeReady(true));
    } else {
      window.setTimeout(() => {
        if (frameRef.current === frame) {
          hideToolChrome(frame);
          patchKoreanTooltips(frame);
          installNifiPanelLayout(frame);
          installProcessorDialogViewportFit(frame);
          setFrameChromeReady(true);
        }
      }, chromeHidden ? 450 : 650);
    }
  };

  const startDetailPanelResize = (event: ReactMouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = detailPanelWidth;

    const resizeTo = (clientX: number) => {
      setDetailPanelWidth(clampDetailPanelWidth(startWidth + startX - clientX));
    };
    const handleMouseMove = (moveEvent: MouseEvent) => {
      resizeTo(moveEvent.clientX);
    };
    const handleMouseUp = (upEvent: MouseEvent) => {
      resizeTo(upEvent.clientX);
      document.body.classList.remove("is-resizing-console-panel");
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };

    document.body.classList.add("is-resizing-console-panel");
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
  };

  const consolePageStyle: CSSProperties | undefined = showProcessGroupTree
    ? {
      gridTemplateColumns: `${isTreePanelCollapsed ? "30px" : "min(250px, 16vw)"} minmax(0, 1fr) ${isDetailPanelCollapsed ? "30px" : `${detailPanelWidth}px`}`,
    }
    : undefined;

  return (
    <div
      className={`console-page${showProcessGroupTree ? " console-page--with-panels" : ""}${nifiProcessorDialogOpen ? " console-page--nifi-dialog-open" : ""}`}
      style={consolePageStyle}
    >
      {showProcessGroupTree ? (
        <div className={`console-side console-side-left${isTreePanelCollapsed ? " collapsed" : ""}`}>
          <button
            type="button"
            className="console-curtain-toggle"
            aria-label={isTreePanelCollapsed ? "트리 패널 열기" : "트리 패널 닫기"}
            title={isTreePanelCollapsed ? "트리 패널 열기" : "트리 패널 닫기"}
            onClick={() => setIsTreePanelCollapsed((collapsed) => !collapsed)}
          >
            {isTreePanelCollapsed ? <DoubleRightOutlined /> : <DoubleLeftOutlined />}
          </button>
          {!isTreePanelCollapsed ? (
            <ProcessGroupTreePanel
              activeGroupId={selectedProcessGroupId}
              onTreeChange={setProcessGroupTree}
              onCatalogSynced={handleCatalogSynced}
            />
          ) : null}
        </div>
      ) : null}
      <div className="console-frame-shell">
        {isReady ? (
          <iframe
            key={frameInstanceKey}
            ref={frameRef}
            className={`console-frame${frameChromeReady ? "" : " console-frame--preparing"}`}
            title={title}
            src={frameSrc}
            allow="clipboard-read; clipboard-write"
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
        <div
          className={`console-side console-side-right${isDetailPanelCollapsed ? " collapsed" : ""}`}
        >
          {!isDetailPanelCollapsed ? (
            <button
              type="button"
              className="console-resize-handle"
              aria-label="속성 패널 너비 조절"
              title="속성 패널 너비 조절"
              onMouseDown={startDetailPanelResize}
            />
          ) : null}
          <button
            type="button"
            className="console-curtain-toggle"
            aria-label={isDetailPanelCollapsed ? "상세 패널 열기" : "상세 패널 닫기"}
            title={isDetailPanelCollapsed ? "상세 패널 열기" : "상세 패널 닫기"}
            onClick={() => setIsDetailPanelCollapsed((collapsed) => !collapsed)}
          >
            {isDetailPanelCollapsed ? <DoubleLeftOutlined /> : <DoubleRightOutlined />}
          </button>
          {!isDetailPanelCollapsed ? (
            selectedCanvasItem?.type === "processor" ? (
              <ProcessorDetailPanel
                processorId={selectedCanvasItem.id}
                onParentGroupFound={setSelectedProcessGroupId}
                onEditLockChange={handleProcessorEditLockChange}
              />
            ) : (
              <ProcessGroupDetailPanel
                activeGroupId={selectedProcessGroupId}
                tree={processGroupTree}
                reloadKey={jobCatalogReloadKey}
              />
            )
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
