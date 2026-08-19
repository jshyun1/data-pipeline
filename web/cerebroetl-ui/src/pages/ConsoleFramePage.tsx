import { useEffect, useMemo, useState, type SyntheticEvent } from "react";
import { Button, Result, Spin } from "antd";
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
  type NifiProcessorDetailResponse,
  type NifiProcessGroupTreeNode,
} from "../api/platform";

const HIDE_TOOL_CHROME_STYLE_ID = "cerebro-hide-tool-chrome";
const CANVAS_SELECTION_SYNC_ATTRIBUTE = "data-cerebro-canvas-selection-sync";
const KOREAN_TOOLTIP_PATCH_ATTRIBUTE = "data-cerebro-korean-tooltip-patch";
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
};
const NIFI_TOOLTIP_ATTRIBUTES = ["title", "aria-label", "data-tooltip", "matTooltip", "mattooltip"];

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
  if (!doc || doc.getElementById(HIDE_TOOL_CHROME_STYLE_ID)) {
    return;
  }

  try {
    const style = doc.createElement("style");
    style.id = HIDE_TOOL_CHROME_STYLE_ID;
    style.textContent = HIDE_TOOL_CHROME_CSS;
    doc.head.appendChild(style);
  } catch {
    // 문서가 아직 교체 중이면 다음 iframe load에서 다시 시도한다.
  }
}

function patchKoreanTooltips(frame: HTMLIFrameElement) {
  const doc = iframeDocument(frame);
  if (!doc) {
    return;
  }

  const patchElement = (element: Element) => {
    NIFI_TOOLTIP_ATTRIBUTES.forEach((attributeName) => {
      const value = element.getAttribute(attributeName);
      const translated = value ? NIFI_TOOLTIP_TEXT[value.trim()] : undefined;
      if (translated) {
        element.setAttribute(attributeName, translated);
      }
    });
  };

  const patchDocument = () => {
    doc.querySelectorAll("[title], [aria-label], [data-tooltip], [matTooltip], [mattooltip]").forEach(patchElement);
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
      mutation.addedNodes.forEach((node) => {
        if (!(node instanceof Element)) {
          return;
        }
        patchElement(node);
        node.querySelectorAll("[title], [aria-label], [data-tooltip], [matTooltip], [mattooltip]").forEach(patchElement);
      });
    }
  });

  observer.observe(doc.documentElement, {
    attributeFilter: NIFI_TOOLTIP_ATTRIBUTES,
    attributes: true,
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

function installCanvasSelectionSync(frame: HTMLIFrameElement, onSelectionChange: (selection: CanvasSelection) => void) {
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

    window.setTimeout(() => {
      const selected = readSelectionFromOperationPanel(doc);
      if (selected) {
        onSelectionChange(selected);
      }
    }, 80);
  };

  doc.addEventListener("click", (event) => notifyFromDocument(event.target as Element | null), true);
  doc.addEventListener("dblclick", (event) => notifyFromDocument(event.target as Element | null), true);
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

function ProcessGroupTreePanel({ activeGroupId, onTreeChange }: ProcessGroupTreePanelProps) {
  const navigate = useNavigate();
  const [tree, setTree] = useState<NifiProcessGroupTreeNode | null>(null);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");

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

  const processorSummary = (() => {
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
      <div className="nifi-job-summary" aria-label="NiFi processor 상태 요약">
        <span>전체 ({processorSummary.total})</span>
        <span>실행중 ({processorSummary.running})</span>
        <span>완료 ({processorSummary.completed})</span>
        <span>실패 ({processorSummary.failed})</span>
        <span>중지 ({processorSummary.stopped})</span>
      </div>
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

interface ProcessorDetailPanelProps {
  processorId: string;
  onParentGroupFound?: (groupId: string) => void;
}

function ProcessorDetailPanel({ processorId, onParentGroupFound }: ProcessorDetailPanelProps) {
  const [processor, setProcessor] = useState<NifiProcessorDetailResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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

  const component = processor?.component;
  const config = component?.config;
  const bundle = component?.bundle;
  const rows = propertyRows(processor);
  const bulletins = processor?.bulletins?.map((entry) => entry.bulletin).filter(Boolean) ?? [];
  const relationships = component?.relationships ?? [];
  const autoTerminated = component?.autoTerminatedRelationships ?? config?.autoTerminatedRelationships ?? [];
  const activeThreadCount = processor?.status?.aggregateSnapshot?.activeThreadCount ?? processor?.status?.activeThreadCount;

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
  const [detail, setDetail] = useState<EtlJobDetailResponse | null>(null);
  const [runs, setRuns] = useState<EtlJobRunResponse[]>([]);
  const [jobsLoading, setJobsLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const selected = useMemo(() => findTreeNode(tree, activeGroupId), [activeGroupId, tree]);
  const selectedGroupId = activeGroupId ?? "root";
  const job = jobs.find((entry) => entry.nifiPgId === selectedGroupId) ?? null;
  const latestRun = runs[0] ?? null;
  const targetTables = uniqueValues(detail?.steps.map((step) => step.targetTable) ?? []);
  const operationTypes = uniqueValues(detail?.steps.map((step) => step.statementType ?? step.stepType) ?? []);
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
    setDetail(null);
    setRuns([]);
    setError(null);

    if (!job) {
      setDetailLoading(false);
      return () => {
        cancelled = true;
      };
    }

    setDetailLoading(true);
    Promise.all([getEtlJob(job.id), getEtlJobRuns(job.id)])
      .then(([jobDetail, jobRuns]) => {
        if (cancelled) {
          return;
        }
        setDetail(jobDetail);
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
  }, [job?.id]);

  const displayJob = detail?.job ?? job;
  const displayNode = selected?.node ?? null;
  const pathText = selected?.path.map((entry) => entry.name).join(" / ") ?? "-";

  return (
    <aside className="nifi-detail-panel" aria-label="선택한 프로세스 그룹 상세">
      <div className="nifi-detail-header">
        <div className="nifi-detail-title" title={displayJob?.jobName ?? displayNode?.name ?? "선택 없음"}>
          {displayJob?.jobName ?? displayNode?.name ?? "선택 없음"}
        </div>
        <div className={`nifi-detail-status ${statusClass(displayJob, displayNode)}`}>
          <span className="nifi-detail-dot" aria-hidden="true" />
          <span>{statusText(displayJob, displayNode)}</span>
          <span>{formatDateTime(latestRun?.startedAt ?? displayJob?.lastSyncedAt)}</span>
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
            <dd>{displayJob?.engine ?? "NIFI"} / {operationTypes[0] ?? "적재"}</dd>
          </div>
          <div>
            <dt>분류</dt>
            <dd>{displayJob?.parameterContextName ?? "-"}</dd>
          </div>
          <div>
            <dt>스텝 수</dt>
            <dd>{displayJob?.stepCount ?? displayNode?.processorCount ?? 0}</dd>
          </div>
        </dl>
      </section>

      <section className="nifi-detail-section">
        <h3>실행</h3>
        <dl>
          <div>
            <dt>스케줄</dt>
            <dd>{detail?.steps[0]?.schedulingPeriod ?? "-"}</dd>
          </div>
          <div>
            <dt>선행 의존</dt>
            <dd>{detail?.links.length ? `${detail.links.length}개 연결` : "-"}</dd>
          </div>
          <div>
            <dt>연결 DAG</dt>
            <dd>
              {displayJob?.airflowDagId ? (
                <>
                  {displayJob.airflowDagId}{" "}
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
            <dd>{displayJob?.comments || displayJob?.engine || "-"}</dd>
          </div>
          <div>
            <dt>타깃</dt>
            <dd>{displayJob?.parameterContextName ?? "-"}</dd>
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

      <section className="nifi-detail-section">
        <h3>변수</h3>
        {detail?.params.length ? (
          <dl>
            {detail.params.slice(0, 4).map((param) => (
              <div key={param.paramName}>
                <dt>{param.paramName}</dt>
                <dd>{param.sensitive ? "********" : param.paramValue || "-"}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <div className="nifi-detail-empty">-</div>
        )}
      </section>

      <button type="button" className="nifi-detail-advanced">
        [고급 설정]
      </button>
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
  const frameSrc = processGroupId ? withProcessGroupId(src, processGroupId) : src;

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

  const handleFrameLoad = (event: SyntheticEvent<HTMLIFrameElement>) => {
    const frame = event.currentTarget;
    hideToolChrome(frame);
    patchKoreanTooltips(frame);
    if (showProcessGroupTree) {
      installCanvasSelectionSync(frame, selectCanvasItem);
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
