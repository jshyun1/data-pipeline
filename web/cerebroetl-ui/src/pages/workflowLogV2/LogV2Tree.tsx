import { useEffect, useRef, useState } from "react";
import { Dropdown, Input } from "antd";
import { InfoCircleOutlined, PlayCircleOutlined } from "@ant-design/icons";
import type { AirflowDagRun } from "../../api/platform";
import type { WorkflowSummary } from "../../api/workflows";
import type { PipelineResponse } from "../../types/pipeline";
import { useAuth } from "../../auth/AuthContext";
import { WfIcon, type WfIconName } from "../../components/WorkflowPageHeader";
import { buildWorkflowHierarchy, type WorkflowTreeItem } from "../../utils/workflowTree";
import {
  CATEGORY_ORDER,
  categoryOf,
  displayName,
  dotOf,
  type DashboardDag,
  type ListScope,
} from "./logV2Data";

type TreeArrow =
  | { kind: "none" }
  | { kind: "row"; open: boolean }
  | { kind: "button"; open: boolean; onToggle: () => void };

/**
 * 로그2 왼쪽 트리. 기존 로그 화면 트리와 같은 계층·검색·우클릭 메뉴(상세·실행 설정)를 쓰고,
 * 줄 앞에 «오늘 마지막 실행» 상태 점을 더했다 - 트리만 훑어도 어디가 실패했는지 보이게.
 * CDC·ETL 이름 줄을 누르면 접기와 함께 그 업무 전체가 목록 범위가 된다.
 */
export function LogV2Tree({
  dags,
  runsByDag,
  workflows,
  pipelines,
  connectionNames,
  scope,
  selectedDagId,
  selectedPipelineId,
  search,
  onSearch,
  onScope,
  onDetail,
  onExecution,
}: {
  dags: DashboardDag[];
  runsByDag: Map<string, AirflowDagRun[]>;
  workflows: WorkflowSummary[];
  pipelines: PipelineResponse[];
  connectionNames: Map<number, string>;
  scope?: ListScope;
  selectedDagId?: string;
  selectedPipelineId?: number;
  search: string;
  onSearch: (value: string) => void;
  onScope: (scope: ListScope) => void;
  onDetail: (dag: DashboardDag) => void;
  onExecution: (dag: DashboardDag) => void;
}) {
  const [collapsedNodes, setCollapsedNodes] = useState<Set<string>>(new Set());
  const seeded = useRef(false);
  const { can, permissionsLoaded } = useAuth();
  const canRun = !permissionsLoaded || can("AIRFLOW", "WRITE");

  // 처음 모양은 기존 트리와 같다: CDC 는 «전체 파이프라인», ETL 은 최상단 워크플로우까지만 펼친다.
  useEffect(() => {
    if (seeded.current || workflows.length === 0) {
      return;
    }
    seeded.current = true;
    const initial = new Set<string>(["cdc:all", "cdc:logfile"]);
    pipelines.forEach((pipeline) => {
      if (pipeline.sourceConnectionId != null) {
        initial.add(`cdc:connection:${pipeline.sourceConnectionId}`);
        initial.add(`cdc:schema:${pipeline.sourceConnectionId}:${pipeline.sourceSchema ?? "스키마 없음"}`);
      }
    });
    const collapseChildren = (items: WorkflowTreeItem[]) => {
      items.forEach((item) => {
        if (item.children.length) initial.add(`wf:${item.workflow.id}`);
        collapseChildren(item.children);
      });
    };
    collapseChildren(buildWorkflowHierarchy(workflows.filter((w) => w.published)));
    setCollapsedNodes(initial);
  }, [workflows, pipelines]);

  // 고른 항목까지의 조상 가지만 편다.
  useEffect(() => {
    const open = new Set<string>();
    if (selectedDagId) {
      const trail: number[] = [];
      const find = (items: WorkflowTreeItem[], path: number[]): boolean => items.some((item) => {
        const next = [...path, item.workflow.id];
        if (item.workflow.dagId === selectedDagId) {
          trail.push(...next);
          return true;
        }
        return find(item.children, next);
      });
      if (find(buildWorkflowHierarchy(workflows.filter((w) => w.published)), [])) {
        open.add("category:ETL");
        trail.forEach((id) => open.add(`wf:${id}`));
      }
    }
    if (selectedPipelineId != null) {
      const pipeline = pipelines.find((row) => row.id === selectedPipelineId);
      if (pipeline) {
        open.add("category:CDC");
        open.add("cdc:all");
        if (pipeline.pipelineType === "LOG_FILE" || pipeline.sourceConnectionId == null) {
          open.add("cdc:logfile");
        } else {
          open.add(`cdc:connection:${pipeline.sourceConnectionId}`);
          open.add(`cdc:schema:${pipeline.sourceConnectionId}:${pipeline.sourceSchema ?? "스키마 없음"}`);
        }
      }
    }
    if (open.size === 0) return;
    setCollapsedNodes((current) => {
      if (![...open].some((key) => current.has(key))) return current;
      const next = new Set(current);
      open.forEach((key) => next.delete(key));
      return next;
    });
  }, [selectedDagId, selectedPipelineId, workflows, pipelines]);

  const toggleNode = (node: string) => {
    setCollapsedNodes((current) => {
      const next = new Set(current);
      if (next.has(node)) next.delete(node);
      else next.add(node);
      return next;
    });
  };

  const treeRow = ({
    active = false, pending = false, icon, label, title, arrow, dot, trailing, onClick, onContextMenu,
  }: {
    active?: boolean;
    pending?: boolean;
    icon: WfIconName;
    label: React.ReactNode;
    title?: string;
    arrow: TreeArrow;
    dot?: string;
    trailing?: React.ReactNode;
    onClick: () => void;
    onContextMenu?: () => void;
  }) => (
    <div
      className={`tree-item-row${active ? " active" : ""}${pending ? " is-pending" : ""}`}
      role="button"
      tabIndex={0}
      aria-expanded={arrow.kind === "none" ? undefined : arrow.open}
      onClick={onClick}
      onContextMenu={onContextMenu}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onClick();
        }
      }}
    >
      {arrow.kind === "button" ? (
        <button type="button" className={`tree-toggle-arrow${arrow.open ? " open" : ""}`}
                aria-label={arrow.open ? "하위 워크플로우 접기" : "하위 워크플로우 펼치기"}
                onClick={(event) => { event.stopPropagation(); arrow.onToggle(); }}
                onKeyDown={(event) => event.stopPropagation()}>
          ▶
        </button>
      ) : (
        <span className={`tree-toggle-arrow${arrow.kind === "row" ? (arrow.open ? " open" : "") : " empty"}`} aria-hidden="true">▶</span>
      )}
      {dot && <i className={`lv2-sd lv2-sd--${dot}`} title="오늘 마지막 실행" />}
      <WfIcon name={icon} size={15} className="tree-node-icon" />
      <span className="tree-node-label" title={title}>{label}</span>
      {trailing}
    </div>
  );

  const contextMenu = (dag: DashboardDag, before?: () => void) => ({
    items: [
      { key: "detail", icon: <InfoCircleOutlined />, label: "상세" },
      ...(canRun ? [{ key: "execution", icon: <PlayCircleOutlined />, label: "실행 설정" }] : []),
    ],
    onClick: ({ key }: { key: string }) => {
      before?.();
      if (key === "detail") onDetail(dag);
      else onExecution(dag);
    },
  });

  const renderDagButton = (dag: DashboardDag, arrow: TreeArrow = { kind: "none" }, icon: WfIconName = "file") => (
    <Dropdown key={dag.dag_id} trigger={["contextMenu"]} menu={contextMenu(dag, () => onScope({ kind: "dag", dagId: dag.dag_id }))}>
      {treeRow({
        active: scope?.kind === "dag" && scope.dagId === dag.dag_id,
        icon,
        label: displayName(dag),
        title: displayName(dag),
        arrow,
        dot: dotOf(runsByDag.get(dag.dag_id)),
        onClick: () => onScope({ kind: "dag", dagId: dag.dag_id }),
      })}
    </Dropdown>
  );

  const publishedWorkflows = workflows.filter((workflow) => workflow.published);
  const workflowHierarchy = buildWorkflowHierarchy(publishedWorkflows);

  const renderPipelineButton = (pipeline: PipelineResponse) => {
    const controlDag = dags.find((d) => d.dag_id === `kafka_pipeline_${pipeline.id}_control`);
    const select = () => onScope({ kind: "cdc", scope: "pipeline", pipelineId: pipeline.id });
    const row = treeRow({
      active: scope?.kind === "cdc" && scope.scope === "pipeline" && scope.pipelineId === pipeline.id,
      icon: "file",
      label: pipeline.name,
      title: pipeline.name,
      arrow: { kind: "none" },
      dot: dotOf(runsByDag.get(`kafka_pipeline_${pipeline.id}_control`)),
      onClick: select,
    });
    if (!controlDag) {
      return <div key={pipeline.id} className="tree-node">{row}</div>;
    }
    return (
      <div key={pipeline.id} className="tree-node">
        <Dropdown trigger={["contextMenu"]} menu={contextMenu(controlDag, select)}>{row}</Dropdown>
      </div>
    );
  };

  const renderCdcTree = () => {
    const byConnection = new Map<number, Map<string, PipelineResponse[]>>();
    const logFiles: PipelineResponse[] = [];
    const needle = search.trim().toLowerCase();
    for (const pipeline of pipelines) {
      const matches = !needle
        || pipeline.name.toLowerCase().includes(needle)
        || (pipeline.sourceTable ?? "").toLowerCase().includes(needle)
        || (pipeline.targetTable ?? "").toLowerCase().includes(needle);
      if (!matches) continue;
      if (pipeline.pipelineType === "LOG_FILE" || pipeline.sourceConnectionId == null) {
        logFiles.push(pipeline);
        continue;
      }
      const schemas = byConnection.get(pipeline.sourceConnectionId) ?? new Map<string, PipelineResponse[]>();
      const schema = pipeline.sourceSchema ?? "스키마 없음";
      schemas.set(schema, [...(schemas.get(schema) ?? []), pipeline]);
      byConnection.set(pipeline.sourceConnectionId, schemas);
    }
    const folder = (key: string, label: string, count: number, selected: boolean, onClick: () => void, children: React.ReactNode) => {
      const collapsed = collapsedNodes.has(key);
      return (
        <div key={key} className="tree-node">
          {treeRow({
            active: selected,
            icon: "folder",
            label,
            title: label,
            arrow: { kind: "row", open: !collapsed },
            trailing: <span className="tree-node-badge">{count}</span>,
            onClick: () => { toggleNode(key); onClick(); },
          })}
          {!collapsed && <div className="tree-sub-group" role="group">{children}</div>}
        </div>
      );
    };
    return folder(
      "cdc:all", "전체 파이프라인", pipelines.length,
      scope?.kind === "cdc" && scope.scope === "all",
      () => onScope({ kind: "cdc", scope: "all" }),
      <>
        {[...byConnection.entries()].map(([connectionId, schemas]) => {
          const count = [...schemas.values()].reduce((sum, rows) => sum + rows.length, 0);
          return folder(
            `cdc:connection:${connectionId}`, connectionNames.get(connectionId) ?? `연결 ${connectionId}`, count,
            scope?.kind === "cdc" && scope.scope === "connection" && scope.connectionId === connectionId,
            () => onScope({ kind: "cdc", scope: "connection", connectionId }),
            <>
              {[...schemas.entries()].map(([schema, rows]) => folder(
                `cdc:schema:${connectionId}:${schema}`, schema, rows.length,
                scope?.kind === "cdc" && scope.scope === "schema" && scope.connectionId === connectionId && scope.schema === schema,
                () => onScope({ kind: "cdc", scope: "schema", connectionId, schema }),
                <>{rows.map((pipeline) => renderPipelineButton(pipeline))}</>,
              ))}
            </>,
          );
        })}
        {logFiles.length > 0 && folder(
          "cdc:logfile", "로그파일", logFiles.length,
          scope?.kind === "cdc" && scope.scope === "logfile",
          () => onScope({ kind: "cdc", scope: "logfile" }),
          <>{logFiles.map((pipeline) => renderPipelineButton(pipeline))}</>,
        )}
      </>,
    );
  };

  const placedDagIds = new Set(workflows.map((workflow) => workflow.dagId));

  const renderWorkflowTree = (item: WorkflowTreeItem, etlDags: DashboardDag[]): React.ReactNode => {
    const needle = search.trim().toLowerCase();
    const nodeKey = `wf:${item.workflow.id}`;
    const collapsed = collapsedNodes.has(nodeKey);
    const renderedChildren = collapsed ? [] : item.children.map((child) => renderWorkflowTree(child, etlDags)).filter(Boolean);
    const selfMatches = !needle || item.workflow.name.toLowerCase().includes(needle);
    if (needle && !selfMatches && renderedChildren.length === 0) {
      return null;
    }
    const dag = etlDags.find((d) => d.dag_id === item.workflow.dagId);
    const hasChildren = item.children.length > 0;
    return (
      <div key={item.workflow.id} className="tree-node">
        {dag
          ? renderDagButton(
              dag,
              hasChildren ? { kind: "button", open: !collapsed, onToggle: () => toggleNode(nodeKey) } : { kind: "none" },
              hasChildren ? "folder" : "file",
            )
          : treeRow({
              pending: true,
              icon: hasChildren ? "folder" : "file",
              label: <>{item.workflow.name}<span className="tree-node-note"> (DAG 준비 중)</span></>,
              title: `${item.workflow.name} (DAG 준비 중)`,
              arrow: hasChildren ? { kind: "row", open: !collapsed } : { kind: "none" },
              onClick: () => { if (hasChildren) toggleNode(nodeKey); },
            })}
        {renderedChildren.length > 0 && <div className="tree-sub-group" role="group">{renderedChildren}</div>}
      </div>
    );
  };

  return (
    <aside className="lv2-card lv2-nav">
      <div className="airflow-tree-search">
        <Input allowClear prefix={<WfIcon name="search" size={13} />} placeholder="검색" aria-label="트리 검색"
               value={search} onChange={(event) => onSearch(event.target.value)} />
      </div>
      <div className="airflow-tree-body" role="tree" aria-label="업무별 트리">
        {CATEGORY_ORDER.map((category) => {
          const categoryDags = dags.filter((dag) => categoryOf(dag) === category);
          const categoryNode = `category:${category}`;
          const categoryCollapsed = collapsedNodes.has(categoryNode);
          return (
            <section key={category} className={`tree-node airflow-tree-category airflow-tree-category--${category.toLowerCase()}`}>
              {treeRow({
                icon: "folder",
                label: category,
                active: category === "ETL" && scope?.kind === "etl-all",
                arrow: { kind: "row", open: !categoryCollapsed },
                onClick: () => {
                  toggleNode(categoryNode);
                  onScope(category === "CDC" ? { kind: "cdc", scope: "all" } : { kind: "etl-all" });
                },
              })}
              {!categoryCollapsed && (
                <div className="tree-sub-group" role="group">
                  {category === "CDC" && renderCdcTree()}
                  {category === "ETL" && workflowHierarchy.map((item) => renderWorkflowTree(item, categoryDags))}
                  {category === "ETL" && categoryDags
                    .filter((dag) => !placedDagIds.has(dag.dag_id))
                    .map((dag) => <div key={dag.dag_id} className="tree-node">{renderDagButton(dag)}</div>)}
                </div>
              )}
            </section>
          );
        })}
      </div>
      <div className="lv2-nav-legend">
        <span><i className="lv2-sd lv2-sd--success" />성공</span>
        <span><i className="lv2-sd lv2-sd--failed" />실패</span>
        <span><i className="lv2-sd lv2-sd--running" />실행 중</span>
        <span><i className="lv2-sd lv2-sd--none" />오늘 실행 없음</span>
      </div>
    </aside>
  );
}
