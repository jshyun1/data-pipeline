import { useEffect, useMemo, useRef, useState, type SyntheticEvent } from "react";
import { Alert, Button, Result, Spin } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import { useLocation, useNavigate } from "react-router-dom";
import {
  getEtlJob,
  getEtlJobRuns,
  listEtlJobs,
  type EtlJobDetailResponse,
  type EtlJobResponse,
  type EtlJobRunResponse,
} from "../api/etlJobs";
import {
  getNifiProcessor,
  getNifiProcessGroupTree,
  acquireNifiProcessorEditLock,
  heartbeatNifiProcessorEditLock,
  releaseNifiProcessorEditLock,
  type NifiProcessorEditLockResponse,
  type NifiProcessorDetailResponse,
  type NifiProcessGroupTreeNode,
} from "../api/platform";

const HIDE_TOOL_CHROME_STYLE_ID = "cerebro-hide-tool-chrome";
const CANVAS_SELECTION_SYNC_ATTRIBUTE = "data-cerebro-canvas-selection-sync";
const CANVAS_ROUTE_SYNC_ATTRIBUTE = "data-cerebro-canvas-route-sync";
const KOREAN_TOOLTIP_PATCH_ATTRIBUTE = "data-cerebro-korean-tooltip-patch";
const KOREAN_TOOLTIP_INTERVAL_ATTRIBUTE = "data-cerebro-korean-tooltip-interval";
const NIFI_STATUS_HIDDEN_ATTRIBUTE = "data-cerebro-status-hidden";
const NIFI_PANEL_LAYOUT_ATTRIBUTE = "data-cerebro-nifi-panel-layout";
const NIFI_PANEL_ATTRIBUTE = "data-cerebro-nifi-panel";
const NIFI_PANEL_EXPANDED_ATTRIBUTE = "data-cerebro-nifi-panel-expanded";
const NIFI_PANEL_BOUND_ATTRIBUTE = "data-cerebro-nifi-panel-bound";
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
  [title="Connected nodes / Total number of nodes in the cluster"],
  [title="연결된 노드 / 클러스터 전체 노드"],
  [title="Total queued data"],
  [title="총 대기 데이터"],
  [title="Transmitting Remote Process Groups"],
  [title="전송 중인 원격 프로세스 그룹"],
  [title="Not Transmitting Remote Process Groups"],
  [title="전송 중이 아닌 원격 프로세스 그룹"],
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
};
const NIFI_TOOLTIP_ATTRIBUTES = ["title", "aria-label", "data-tooltip", "matTooltip", "mattooltip", "tooltip"];
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
  return undefined;
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
  };

  const patchDocument = () => {
    doc
      .querySelectorAll("[title], [aria-label], [data-tooltip], [matTooltip], [mattooltip], [tooltip], title")
      .forEach(patchElement);
    patchTextNodes(doc);
    HIDDEN_NIFI_STATUS_ICON_CLASSES.forEach((iconClass) => {
      doc.querySelectorAll(`.${iconClass}`).forEach(patchStatusIconVisibility);
    });
    NIFI_STATUS_TOOLTIP_ICON_TEXT.forEach(([iconClass]) => {
      doc.querySelectorAll(`.${iconClass}`).forEach(patchStatusTooltipText);
    });
  };

  patchDocument();

  if (doc.documentElement.getAttribute(KOREAN_TOOLTIP_PATCH_ATTRIBUTE) === "true") {
    return;
  }
  doc.documentElement.setAttribute(KOREAN_TOOLTIP_PATCH_ATTRIBUTE, "true");

  const DocumentMutationObserver = doc.defaultView?.MutationObserver ?? MutationObserver;
  const observer = new DocumentMutationObserver((mutations) => {
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
          .querySelectorAll("[title], [aria-label], [data-tooltip], [matTooltip], [mattooltip], [tooltip], title")
          .forEach(patchElement);
        node.querySelectorAll("*").forEach(patchExactTextElement);
        HIDDEN_NIFI_STATUS_ICON_CLASSES.forEach((iconClass) => {
          node.querySelectorAll(`.${iconClass}`).forEach(patchStatusIconVisibility);
        });
        NIFI_STATUS_TOOLTIP_ICON_TEXT.forEach(([iconClass]) => {
          node.querySelectorAll(`.${iconClass}`).forEach(patchStatusTooltipText);
        });
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
    const intervalId = doc.defaultView?.setInterval(() => patchDocument(), 1000);
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
        const shouldExpand = !panels.some((item) => item.getAttribute(NIFI_PANEL_EXPANDED_ATTRIBUTE) === "true");
        panels.forEach((item) => item.setAttribute(NIFI_PANEL_EXPANDED_ATTRIBUTE, shouldExpand ? "true" : "false"));
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
    markPanel("navigation");
    markPanel("operation");
  };

  applyLayout();

  if (doc.documentElement.getAttribute(NIFI_PANEL_LAYOUT_ATTRIBUTE) === "true") {
    return;
  }
  doc.documentElement.setAttribute(NIFI_PANEL_LAYOUT_ATTRIBUTE, "true");

  const DocumentMutationObserver = doc.defaultView?.MutationObserver ?? MutationObserver;
  const observer = new DocumentMutationObserver(() => applyLayout());
  observer.observe(doc.documentElement, {
    childList: true,
    subtree: true,
  });
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
    /operation/i.test(elementClassName(element)) || /Operation/.test(element.textContent ?? ""),
  );

  for (const panel of panels) {
    const text = panel.textContent ?? "";
    if (!/Process\s+Group/i.test(text)) {
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
    /operation/i.test(elementClassName(element)) || /Operation/.test(element.textContent ?? ""),
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
}

type NifiTreeStatusFilter = "ALL" | "RUNNING" | "DONE" | "FAILED" | "STOPPED";

function ProcessGroupTreePanel({ activeGroupId, onTreeChange }: ProcessGroupTreePanelProps) {
  const navigate = useNavigate();
  const [tree, setTree] = useState<NifiProcessGroupTreeNode | null>(null);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [activeStatusFilter, setActiveStatusFilter] = useState<NifiTreeStatusFilter | null>(null);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    setError(null);

    getNifiProcessGroupTree()
      .then((result) => {
        if (cancelled) {
          return;
        }
        setTree(result);
        onTreeChange?.(result);
        setExpandedIds(new Set());
      })
      .catch((ex) => {
        if (!cancelled) {
          setError(ex instanceof Error ? ex.message : "트리를 불러오지 못했습니다.");
          onTreeChange?.(null);
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
  }, [onTreeChange]);

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

  const jobSummary = (() => {
    const total = tree?.processorCount ?? 0;
    const running = tree?.runningCount ?? 0;
    const failed = tree?.invalidCount ?? 0;
    const stopped = tree?.stoppedCount ?? 0;
    const completed = total - running - failed - stopped;
    return {
      total,
      running,
      completed: Math.max(completed, 0),
      failed,
      stopped,
    };
  })();

  const processGroupJobs = useMemo(() => flattenTree(tree).filter((node) => node.id !== "root" && node.processorCount > 0), [tree]);

  const statusCounts = (node: NifiProcessGroupTreeNode) => {
    const running = node.runningCount ?? 0;
    const failed = node.invalidCount ?? 0;
    const stopped = node.stoppedCount ?? 0;
    const completed = Math.max((node.processorCount ?? 0) - running - failed - stopped, 0);
    return { running, completed, failed, stopped };
  };

  const statusJobs = useMemo(() => {
    if (!activeStatusFilter) {
      return [];
    }
    return processGroupJobs.filter((node) => {
      const { running, completed, failed, stopped } = statusCounts(node);
      switch (activeStatusFilter) {
        case "RUNNING":
          return running > 0;
        case "DONE":
          return completed > 0;
        case "FAILED":
          return failed > 0;
        case "STOPPED":
          return stopped > 0;
        case "ALL":
        default:
          return true;
      }
    });
  }, [activeStatusFilter, processGroupJobs]);

  const statusButtons: Array<{ key: NifiTreeStatusFilter; label: string; count: number }> = [
    { key: "ALL", label: "전체", count: jobSummary.total },
    { key: "RUNNING", label: "실행중", count: jobSummary.running },
    { key: "DONE", label: "완료", count: jobSummary.completed },
    { key: "FAILED", label: "실패", count: jobSummary.failed },
    { key: "STOPPED", label: "중지", count: jobSummary.stopped },
  ];

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
            {node.processorCount > 0 ? <span className="nifi-tree-count">({node.processorCount})</span> : null}
          </button>
        </div>
        {hasChildren && expanded ? node.children.map((child) => renderNode(child, depth + 1)) : null}
      </div>
    );
  };

  const filteredTree = tree ? filterTree(tree, searchQuery.trim().toLowerCase()) : null;

  return (
    <aside className="nifi-tree-panel" aria-label="NiFi 프로세스 그룹 트리">
      <div className="nifi-tree-section-title">상태</div>
      <div className="nifi-job-summary" aria-label="NiFi job 상태 요약">
        {statusButtons.map((status) => (
          <button
            key={status.key}
            type="button"
            className={activeStatusFilter === status.key ? "active" : ""}
            onClick={() => setActiveStatusFilter((current) => (current === status.key ? null : status.key))}
          >
            {status.label} ({status.count})
          </button>
        ))}
      </div>
      {activeStatusFilter ? (
        <div className="nifi-status-job-list" aria-label="상태별 NiFi job 목록">
          {statusJobs.length > 0 ? (
            statusJobs.map((node) => {
              const counts = statusCounts(node);
              return (
                <button key={node.id} type="button" title={node.name} onClick={() => openGroup(node.id)}>
                  <span>{node.name}</span>
                  <small>
                    실행 {counts.running} · 완료 {counts.completed} · 실패 {counts.failed} · 중지 {counts.stopped}
                  </small>
                </button>
              );
            })
          ) : (
            <div className="nifi-tree-message">해당 Job 없음</div>
          )}
        </div>
      ) : null}
      <label className="nifi-tree-section-title" htmlFor="nifi-tree-search">
        트리 검색
      </label>
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
  if (job?.invalidCount || node?.invalidCount) {
    return "실패";
  }
  if (job?.runningCount || node?.runningCount) {
    return "실행중";
  }
  return "완료";
}

function statusClass(job: EtlJobResponse | null, node: NifiProcessGroupTreeNode | null) {
  const status = statusText(job, node);
  if (status === "실패") {
    return "error";
  }
  if (status === "실행중") {
    return "running";
  }
  return "done";
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

function compactCountLabel(count: number, unit: string) {
  return count > 0 ? `${count}${unit}` : "-";
}

function firstOrCountLabel(values: string[], suffix = "") {
  if (values.length === 0) {
    return "-";
  }
  return values.length === 1 ? values[0] : `${values.length}${suffix}`;
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
  const [processor, setProcessor] = useState<NifiProcessorDetailResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [editLock, setEditLock] = useState<NifiProcessorEditLockResponse | null>(null);
  const [lockError, setLockError] = useState<string | null>(null);
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
  const bulletins = processor?.bulletins?.map((entry) => entry.bulletin).filter(Boolean) ?? [];
  const relationships = component?.relationships ?? [];
  const autoTerminated = component?.autoTerminatedRelationships ?? config?.autoTerminatedRelationships ?? [];
  const activeThreadCount = processor?.status?.aggregateSnapshot?.activeThreadCount ?? processor?.status?.activeThreadCount;
  const isReadOnlyByLock = Boolean(editLock && !editLock.heldByMe);

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
        <h3>알림</h3>
        {bulletins.length ? (
          <dl>
            {bulletins.map((bulletin, index) => (
              <div key={`${bulletin?.timestamp ?? index}-${bulletin?.message ?? ""}`}>
                <dt>{bulletin?.level ?? "-"}</dt>
                <dd>
                  {[formatDateTime(bulletin?.timestamp), bulletin?.category, bulletin?.message]
                    .filter(Boolean)
                    .join(" / ")}
                </dd>
              </div>
            ))}
          </dl>
        ) : (
          <div className="nifi-detail-empty">-</div>
        )}
      </section>
    </aside>
  );
}

interface ProcessGroupDetailPanelProps {
  activeGroupId: string | null;
  tree: NifiProcessGroupTreeNode | null;
}

function ProcessGroupDetailPanel({ activeGroupId, tree }: ProcessGroupDetailPanelProps) {
  const navigate = useNavigate();
  const [jobs, setJobs] = useState<EtlJobResponse[]>([]);
  const [details, setDetails] = useState<EtlJobDetailResponse[]>([]);
  const [runs, setRuns] = useState<EtlJobRunResponse[]>([]);
  const [jobsLoading, setJobsLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

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
  const allSteps = details.flatMap((entry) => entry.steps);
  const allLinks = details.flatMap((entry) => entry.links);
  const targetTables = uniqueValues(allSteps.map((step) => step.targetTable));
  const operationTypes = uniqueValues(allSteps.map((step) => step.statementType ?? step.stepType));
  const sourceValues = uniqueValues(groupJobs.map((entry) => entry.comments || entry.engine));
  const targetValues = uniqueValues(groupJobs.map((entry) => entry.parameterContextName));
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
  }, []);

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
  const pathText = selected?.path.map((entry) => entry.name).join(" / ") ?? "-";
  const stepCount = job?.stepCount ?? displayNode?.processorCount ?? groupJobs.reduce((sum, entry) => sum + entry.stepCount, 0);

  return (
    <aside className="nifi-detail-panel" aria-label="선택한 프로세스 그룹 상세">
      <div className="nifi-detail-header">
        <div className="nifi-detail-title" title={displayJob?.jobName ?? displayNode?.name ?? "선택 없음"}>
          {displayJob?.jobName ?? displayNode?.name ?? "선택 없음"}
        </div>
        <div className={`nifi-detail-status ${statusClass(displayJob, displayNode)}`}>
          <span className="nifi-detail-dot" aria-hidden="true" />
          <span>{statusText(displayJob, displayNode)}</span>
          <span>{formatDateTime(latestRun?.startedAt ?? relatedJob?.lastSyncedAt)}</span>
        </div>
      </div>

      {isLoading ? <div className="nifi-detail-message">불러오는 중</div> : null}
      {!isLoading && error ? <div className="nifi-detail-message error">조회 실패</div> : null}

      <section className="nifi-detail-section">
        <h3>기본 정보</h3>
        <dl>
          <div>
            <dt>상위 경로</dt>
            <dd>{pathText}</dd>
          </div>
          <div>
            <dt>운영 / 적재</dt>
            <dd>{relatedJob?.engine ?? "NIFI"} / {operationTypes[0] ?? "적재"}</dd>
          </div>
          <div>
            <dt>스텝 수</dt>
            <dd>{stepCount}</dd>
          </div>
        </dl>
      </section>

      <section className="nifi-detail-section">
        <h3>실행</h3>
        <dl>
          <div>
            <dt>선행 의존</dt>
            <dd>{compactCountLabel(allLinks.length, "개 연결")}</dd>
          </div>
          <div>
            <dt>연결 DAG</dt>
            <dd>
              {airflowDags.length > 0 ? (
                <>
                  {firstOrCountLabel(airflowDags, "개")}{" "}
                  <button type="button" className="nifi-detail-link" onClick={() => navigate("/airflow/manage")}>
                    [열기]
                  </button>
                </>
              ) : (
                "-"
              )}
            </dd>
          </div>
        </dl>
      </section>

      <section className="nifi-detail-section">
        <h3>대상</h3>
        <dl>
          <div>
            <dt>소스</dt>
            <dd>{firstOrCountLabel(sourceValues, "종")}</dd>
          </div>
          <div>
            <dt>타깃</dt>
            <dd>{firstOrCountLabel(targetValues, "종")}</dd>
          </div>
          <div>
            <dt>테이블</dt>
            <dd>
              {targetTables.length > 0 ? (
                <>
                  {targetTables.length}종{" "}
                  <details className="nifi-detail-inline-details">
                    <summary>[보기]</summary>
                    <div>{targetTables.join(", ")}</div>
                  </details>
                </>
              ) : (
                "-"
              )}
            </dd>
          </div>
        </dl>
      </section>
    </aside>
  );
}

export function ConsoleFramePage({ title, src, healthcheckSrc, waitMessage, showProcessGroupTree = false }: ConsoleFramePageProps) {
  const location = useLocation();
  const [isReady, setIsReady] = useState(!healthcheckSrc);
  const [frameKey, setFrameKey] = useState(0);
  const [processGroupTree, setProcessGroupTree] = useState<NifiProcessGroupTreeNode | null>(null);
  const processGroupId = new URLSearchParams(location.search).get("processGroupId");
  const [selectedProcessGroupId, setSelectedProcessGroupId] = useState<string | null>(processGroupId);
  const [selectedCanvasItem, setSelectedCanvasItem] = useState<CanvasSelection | null>(
    processGroupId ? { type: "processGroup", id: processGroupId } : null,
  );
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
    hideToolChrome(frame);
    patchKoreanTooltips(frame);
    installNifiPanelLayout(frame);
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
        <ProcessGroupTreePanel activeGroupId={selectedProcessGroupId} onTreeChange={setProcessGroupTree} />
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
          <ProcessGroupDetailPanel activeGroupId={selectedProcessGroupId} tree={processGroupTree} />
        )
      ) : null}
    </div>
  );
}
