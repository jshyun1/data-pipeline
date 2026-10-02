import { useEffect, useState, type DragEvent, type ReactNode } from "react";
import { WfIcon, type WfIconName } from "./WorkflowPageHeader";

/**
 * 워크플로우 설계 화면 왼쪽 팔레트(워크플로우 · ETL 태스크 · CDC)의 트리 한 줄.
 *
 * <p>줄이 하는 일은 구역마다 다르다 - 워크플로우 줄은 누르면 그 워크플로우로 옮겨 가고, job 줄은
 * 끌어다 캔버스에 놓고, 폴더 줄은 누르면 접힌다. 그래서 뜻은 부르는 쪽이 여기에 담아 넘기고
 * {@link PaletteTree} 는 그리기와 펼침 상태만 맡는다.
 */
export interface PaletteTreeNode {
  key: string;
  /** 줄에 적는 이름. 한 줄로 자르므로 전체는 title 로 보여준다. */
  label: ReactNode;
  title?: string;
  icon: WfIconName;
  /** 오른쪽 회색 배지(개수). 없으면 그리지 않는다. */
  badge?: number;
  badgeTitle?: string;
  /** «여기»를 가리키는 줄(지금 열려 있는 워크플로우). 관리 화면의 고른 줄과 같은 색이다. */
  active?: boolean;
  /** 아직 쓸 수 없는 줄(빈 그룹, 캔버스에 못 얹는 CDC 파이프라인). 흐리게 그린다. */
  muted?: boolean;
  /** 줄을 눌렀을 때. 있으면 하위가 있어도 ▶ 만 접기 버튼이 된다. 없으면 줄 누르기가 곧 접기/펼치기다. */
  onClick?: () => void;
  /** 있으면 줄을 끌 수 있다. 끌면 click 이 나지 않으므로 onClick 과 부딪히지 않는다. */
  onDragStart?: (event: DragEvent<HTMLDivElement>) => void;
  children?: PaletteTreeNode[];
}

/**
 * 설계 화면 팔레트 트리. 모양은 워크플로우 관리 화면 트리(WorkflowDesignPage 의 WorkflowTreeView)와
 * 같다(2026-09-11 요청) - 같은 마크업(tree-node · tree-item-row · ▶ · 폴더/파일 아이콘 · 개수 배지 ·
 * tree-sub-group 점선)에 같은 CSS 규칙(index.css 의 `:is(.wf-tree-scroll, .airflow-tree-body,
 * .wf-palette-tree) .tree-*`)을 쓴다. antd Tree 로는 그 화살표·점선·줄 간격이 맞지 않아 관리 화면에서도
 * 걷어냈다(WORK_LOG 33절).
 *
 * <p>처음 펼침은 마운트할 때 한 번만 읽는다(antd Tree 의 defaultExpandedKeys 와 같다).
 */
export function PaletteTree({
  nodes,
  defaultOpenKeys,
  ariaLabel,
  collapseSignal = 0,
  expandSignal = 0,
}: {
  nodes: PaletteTreeNode[];
  defaultOpenKeys: string[];
  ariaLabel: string;
  /** 값이 바뀌면 전부 접는다 - 패널 머리의 «전체 접기»가 쓴다(ETL 관리 트리와 같은 단추, 2026-10-01). */
  collapseSignal?: number;
  /** 값이 바뀌면 하위가 있는 칸을 전부 연다(«전체 열기»). */
  expandSignal?: number;
}) {
  const [openKeys, setOpenKeys] = useState<Set<string>>(() => new Set(defaultOpenKeys));

  useEffect(() => {
    if (collapseSignal > 0) {
      setOpenKeys(new Set());
    }
  }, [collapseSignal]);

  useEffect(() => {
    if (expandSignal > 0) {
      const keys = new Set<string>();
      const walk = (list: PaletteTreeNode[]) => list.forEach((node) => {
        if (node.children?.length) { keys.add(node.key); walk(node.children); }
      });
      walk(nodes);
      setOpenKeys(keys);
    }
    // nodes 는 자료가 바뀔 때마다 새로 만들어진다 - 단추를 누른 그 순간의 것만 쓴다.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expandSignal]);

  const toggle = (key: string) => {
    setOpenKeys((current) => {
      const next = new Set(current);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const renderNode = (node: PaletteTreeNode): ReactNode => {
    const children = node.children ?? [];
    const hasChildren = children.length > 0;
    const open = hasChildren && openKeys.has(node.key);
    const rowAction = node.onClick ?? (hasChildren ? () => toggle(node.key) : undefined);
    // ▶ 를 따로 눌리게 하는 것은 줄 누르기가 접기가 아닌 다른 일을 할 때뿐이다 -
    // 줄과 같이 눌리면 접으려다 다른 워크플로우로 옮겨 간다.
    const arrowIsButton = hasChildren && Boolean(node.onClick);
    const dragOnly = Boolean(node.onDragStart) && !rowAction;
    const inert = !rowAction && !node.onDragStart;
    const className = [
      "tree-item-row",
      node.active ? "active" : "",
      node.muted ? "is-muted" : "",
      dragOnly ? "is-drag-only" : "",
      inert ? "is-inert" : "",
    ].filter(Boolean).join(" ");
    return (
      <div key={node.key} className="tree-node" role="treeitem"
           aria-selected={node.active ?? false}
           aria-expanded={hasChildren ? open : undefined}>
        <div
          className={className}
          title={node.title}
          tabIndex={rowAction ? 0 : -1}
          draggable={Boolean(node.onDragStart)}
          onDragStart={node.onDragStart}
          onClick={rowAction}
          onKeyDown={(event) => {
            if (rowAction && (event.key === "Enter" || event.key === " ")) {
              event.preventDefault();
              rowAction();
            }
          }}
        >
          {arrowIsButton ? (
            <button
              type="button"
              className={`tree-toggle-arrow${open ? " open" : ""}`}
              aria-label={open ? "하위 접기" : "하위 펼치기"}
              onClick={(event) => {
                event.stopPropagation();
                toggle(node.key);
              }}
              onKeyDown={(event) => event.stopPropagation()}
            >
              ▶
            </button>
          ) : (
            <span className={`tree-toggle-arrow${hasChildren ? (open ? " open" : "") : " empty"}`}
                  aria-hidden="true">
              ▶
            </span>
          )}
          <WfIcon name={node.icon} size={15} className="tree-node-icon" />
          <span className="tree-node-label">{node.label}</span>
          {node.badge !== undefined && (
            <span className="tree-node-badge" title={node.badgeTitle}>{node.badge}</span>
          )}
        </div>
        {open ? (
          <div className="tree-sub-group" role="group">
            {children.map(renderNode)}
          </div>
        ) : null}
      </div>
    );
  };

  return (
    <div className="wf-palette-tree">
      <div role="tree" aria-label={ariaLabel} className="tree-root">
        {nodes.map(renderNode)}
      </div>
    </div>
  );
}
