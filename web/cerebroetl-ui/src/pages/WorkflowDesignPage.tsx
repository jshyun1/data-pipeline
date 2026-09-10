import { useState, type ReactNode } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ConfigProvider, Empty, Form, Input, message, Modal, Popconfirm, Table } from "antd";
import type { ColumnsType } from "antd/es/table";
import dayjs from "dayjs";
import { useAuth } from "../auth/AuthContext";
import { syncEtlJobs } from "../api/etlJobs";
import {
  createWorkflow,
  deleteWorkflow,
  listWorkflows,
  unpublishWorkflow,
  type WorkflowSummary,
} from "../api/workflows";
import {
  buildWorkflowHierarchy,
  flattenWorkflowTree,
  type WorkflowTreeItem,
} from "../utils/workflowTree";
import { WfIcon, WorkflowPageHeader } from "../components/WorkflowPageHeader";
import { cerebroBrandTheme } from "../theme/cerebro";

// 워크플로우 목록. 여기서 만들고, 캔버스(설계)로 들어가 job을 배치한다.
//
// "게시" 여부가 곧 "Airflow에 DAG가 있는가"다. 저장만 해서는 DAG가 생기지 않으므로
// 목록에서 그 상태를 한눈에 보여준다.
//
// 왼쪽 트리는 «워크플로우 계층»이다. 최상단은 누구의 하위도 아닌 워크플로우이고, 그 아래는
// 캔버스에 노드로 얹어 둔 하위 워크플로우다. 고르면 그 워크플로우와 하위 전부를 표에 보여준다.
// 워크플로우는 업무 그룹(NiFi 그룹)에 속하지 않는다. 분류는 이 계층이 전부다.
//
// 모양은 디자인 초안 workflow-scheduling.html(왼쪽 트리 카드 + 오른쪽 목록 카드)을 따른다.

/**
 * 워크플로우 계층 트리. 초안의 트리 마크업(tree-node · tree-item-row · tree-sub-group)을 그대로 쓴다 -
 * antd Tree 로는 초안의 ▶ 화살표·점선 들여쓰기·줄 간격이 맞지 않았다.
 *
 * <p>동작은 antd Tree 를 쓰던 때와 같다: 줄을 누르면 고르고(고른 줄을 다시 누르면 해제),
 * 화살표는 펼치기/접기만 한다. 처음에는 최상단만 펼쳐 둔다 - 부모가 key 를 바꿔 다시 마운트하면
 * (검색어가 바뀌면) 이 기본 상태로 돌아간다.
 */
function WorkflowTreeView({
  items,
  selectedId,
  onSelect,
}: {
  items: WorkflowTreeItem[];
  selectedId?: number;
  onSelect: (id: number | undefined) => void;
}) {
  const [openIds, setOpenIds] = useState<Set<number>>(() => new Set(items.map((item) => item.workflow.id)));

  const toggle = (id: number) => {
    setOpenIds((current) => {
      const next = new Set(current);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const renderNode = (item: WorkflowTreeItem): ReactNode => {
    const id = item.workflow.id;
    const hasChildren = item.children.length > 0;
    const open = openIds.has(id);
    const active = selectedId === id;
    const select = () => onSelect(active ? undefined : id);
    return (
      <div key={id} className="tree-node" role="treeitem" aria-selected={active}
           aria-expanded={hasChildren ? open : undefined}>
        <div
          className={active ? "tree-item-row active" : "tree-item-row"}
          tabIndex={0}
          onClick={select}
          onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === " ") {
              event.preventDefault();
              select();
            }
          }}
        >
          <button
            type="button"
            className={`tree-toggle-arrow${hasChildren ? (open ? " open" : "") : " empty"}`}
            tabIndex={hasChildren ? 0 : -1}
            aria-label={hasChildren ? (open ? "하위 워크플로우 접기" : "하위 워크플로우 펼치기") : undefined}
            onClick={(event) => {
              // 화살표는 접기만 한다 - 줄 선택까지 같이 되면 접으려다 표가 바뀐다.
              event.stopPropagation();
              if (hasChildren) {
                toggle(id);
              }
            }}
            onKeyDown={(event) => event.stopPropagation()}
          >
            ▶
          </button>
          <WfIcon name={hasChildren ? "folder" : "file"} size={15} className="tree-node-icon" />
          <span className="tree-node-label">{item.workflow.name}</span>
          <span className="tree-node-badge" title="캔버스에 놓인 job 수">{item.workflow.jobCount}</span>
        </div>
        {hasChildren && open ? (
          <div className="tree-sub-group" role="group">
            {item.children.map(renderNode)}
          </div>
        ) : null}
      </div>
    );
  };

  return (
    <div role="tree" aria-label="워크플로우 계층" className="tree-root">
      {items.map(renderNode)}
    </div>
  );
}

export function WorkflowDesignPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { can, permissionsLoaded } = useAuth();
  // 생성·삭제는 설계(NIFI 쓰기), 게시 취소는 DAG 를 없애는 운영 동작(AIRFLOW 쓰기).
  const canWrite = !permissionsLoaded || can("NIFI", "WRITE");
  const canPublish = !permissionsLoaded || can("AIRFLOW", "WRITE");
  const [createOpen, setCreateOpen] = useState(false);
  const [selectedWorkflowId, setSelectedWorkflowId] = useState<number>();
  const [treeSearch, setTreeSearch] = useState("");
  const [form] = Form.useForm();

  const { data, isLoading } = useQuery({ queryKey: ["workflows"], queryFn: listWorkflows });

  const refresh = () => queryClient.invalidateQueries({ queryKey: ["workflows"] });

  /**
   * ETL(NiFi)에서 만든 job 을 바로 끌어와 쓰고 싶을 때. 미러링은 5분 주기라 방금 만든 job 이
   * 캔버스 팔레트에 아직 없을 수 있는데, 이걸 누르면 기다리지 않고 반영된다.
   */
  const syncMutation = useMutation({
    mutationFn: syncEtlJobs,
    onSuccess: (result) => {
      const changed = result.created + result.deleted;
      message.success(changed > 0
          ? `동기화 완료 - job ${result.jobsSeen}개 (신규 ${result.created}, 삭제 ${result.deleted})`
          : `동기화 완료 - job ${result.jobsSeen}개 (변경 없음)`);
    },
    onError: (error: Error) => message.warning(`동기화 실패(목록만 새로고침합니다): ${error.message}`),
    onSettled: () => {
      void refresh();
    },
  });

  const createMutation = useMutation({
    mutationFn: createWorkflow,
    onSuccess: (created) => {
      message.success("워크플로우를 만들었습니다. 캔버스에서 job을 배치해주세요.");
      setCreateOpen(false);
      form.resetFields();
      void refresh();
      navigate(`/workflows/design/${created.id}`);
    },
    onError: (error: Error) => message.error(error.message),
  });

  const unpublishMutation = useMutation({
    mutationFn: unpublishWorkflow,
    onSuccess: () => {
      message.success("게시를 내렸습니다. 잠시 후 Airflow에서 DAG가 사라집니다.");
      void refresh();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const deleteMutation = useMutation({
    mutationFn: deleteWorkflow,
    onSuccess: () => {
      message.success("삭제했습니다.");
      void refresh();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const workflows = data ?? [];

  /**
   * 워크플로우 트리. 최상단은 «누구의 하위도 아닌» 워크플로우이고, 그 아래는 캔버스에
   * 노드로 얹어 둔 하위 워크플로우다. 이름 오른쪽 숫자는 그 워크플로우 캔버스에 놓인
   * job 수다(하위 워크플로우 노드는 세지 않는다 - 그건 자기 줄에서 다시 세어진다).
   *
   * 예전에는 NiFi 업무 그룹(DW/DZ/Template)으로 묶어 보여줬는데, 여기서 알고 싶은 것은
   * «total 을 돌리면 무엇이 같이 도는가»이고 그건 그룹이 아니라 캔버스에 그린 상하 관계다.
   */
  const workflowTree = buildWorkflowHierarchy(workflows);

  /** 검색어가 걸리는 워크플로우와 그 조상만 남긴다(자손이 걸리면 부모도 길로 남아야 한다). */
  const filterTree = (items: WorkflowTreeItem[], needle: string): WorkflowTreeItem[] => {
    if (!needle) {
      return items;
    }
    return items.flatMap((item) => {
      const children = filterTree(item.children, needle);
      const self = item.workflow.name.toLowerCase().includes(needle);
      return self || children.length ? [{ ...item, children }] : [];
    });
  };
  const visibleTree = filterTree(workflowTree, treeSearch.trim().toLowerCase());

  /**
   * 트리에서 워크플로우를 고르면 그 워크플로우와 <b>하위 전부</b>를 표에 보여준다.
   * 상위 그룹을 고르면 하위까지 보여주던 예전 동작을 계층 기준으로 옮긴 것이다 -
   * total 을 고르면 total·dz_com_daily·dz_pop_daily 가 함께 보인다.
   */
  const findSubtree = (items: WorkflowTreeItem[], id: number): WorkflowTreeItem | undefined => {
    for (const item of items) {
      if (item.workflow.id === id) {
        return item;
      }
      const hit = findSubtree(item.children, id);
      if (hit) {
        return hit;
      }
    }
    return undefined;
  };
  const selectedSubtree = selectedWorkflowId === undefined
    ? undefined
    : findSubtree(workflowTree, selectedWorkflowId);
  const visible = selectedSubtree ? flattenWorkflowTree([selectedSubtree]) : workflows;

  const columns: ColumnsType<WorkflowSummary> = [
    {
      title: "워크플로우",
      render: (_, row) => (
        <div className="wf-info-cell">
          <button type="button" className="wf-main-name" onClick={() => navigate(`/workflows/design/${row.id}`)}>
            {row.name}
          </button>
          <span className="wf-sub-desc">{row.dagId}</span>
        </div>
      ),
    },
    {
      title: "job 수",
      dataIndex: "jobCount",
      width: 90,
      align: "center",
      render: (value: number) => <span className="wf-job-count">{value}</span>,
    },
    {
      title: "스케줄",
      width: 180,
      render: (_, row) => {
        // 선행이 있으면 시간이 아니라 "앞 워크플로우가 끝나면" 실행된다.
        if (row.upstreamCount) {
          return (
            <span className="wf-schedule-badge wf-schedule-badge--upstream">
              <WfIcon name="flow" size={13} />
              선행 {row.upstreamCount}개 완료 후
            </span>
          );
        }
        return row.scheduleCron
          ? (
            <span className="wf-schedule-badge wf-schedule-badge--cron">
              <WfIcon name="clock" size={13} />
              {row.scheduleCron} <span className="wf-schedule-tz">({row.timezone})</span>
            </span>
          )
          : (
            <span className="wf-schedule-badge">
              <WfIcon name="clock" size={13} />
              수동 실행
            </span>
          );
      },
    },
    {
      title: "게시 상태",
      width: 190,
      render: (_, row) => row.published
        ? (
          <div className="wf-status-cell">
            <span className="wf-status-pill">● 게시됨</span>
            <span className="wf-status-meta">
              {row.publishedAt ? dayjs(row.publishedAt).format("MM-DD HH:mm") : ""}
              {row.publishedBy ? ` · ${row.publishedBy}` : ""}
            </span>
          </div>
        )
        : <span className="wf-status-pill unpublished">미게시 (DAG 없음)</span>,
    },
    {
      title: "관리",
      width: 210,
      align: "center",
      render: (_, row) => (
        <div className="wf-action-group">
          <button type="button" className="btn-tbl-action btn-act-design"
                  onClick={() => navigate(`/workflows/design/${row.id}`)}>설계</button>
          {row.published && (
            <Popconfirm title="게시를 내릴까요?" description="Airflow에서 DAG가 사라집니다."
                        onConfirm={() => unpublishMutation.mutate(row.id)}>
              <button type="button" className="btn-tbl-action btn-act-unpublish" disabled={!canPublish}
                      title={canPublish ? undefined : "Airflow 쓰기 권한이 없습니다"}>게시 취소</button>
            </Popconfirm>
          )}
          <Popconfirm title="이 워크플로우를 삭제할까요?"
                      description={row.published ? "게시 중이면 먼저 게시를 내려야 합니다." : undefined}
                      onConfirm={() => deleteMutation.mutate(row.id)}>
            <button type="button" className="btn-tbl-action btn-act-delete" disabled={!canWrite}>삭제</button>
          </Popconfirm>
        </div>
      ),
    },
  ];

  return (
    <ConfigProvider theme={cerebroBrandTheme}>
    <div className="wf-design-page">
      <WorkflowPageHeader
        title="워크플로우 스케줄링"
        icon={<WfIcon name="calendar" size={22} strokeWidth={2.2} />}
        actions={
          <>
            {/* antd 버튼의 loading 과 같게: 동기화 중에는 누름을 무시하고 아이콘만 돈다. */}
            <button type="button" className={syncMutation.isPending ? "btn-wf-sync spinning" : "btn-wf-sync"}
                    aria-busy={syncMutation.isPending}
                    title="ETL 에서 만든 job 을 기다리지 않고 바로 가져옵니다"
                    onClick={() => {
                      if (!syncMutation.isPending) {
                        syncMutation.mutate();
                      }
                    }}>
              <WfIcon name="sync" size={15} strokeWidth={2.2} />
              동기화
            </button>
            <button type="button" className="btn-wf-create" disabled={!canWrite}
                    onClick={() => setCreateOpen(true)}>
              <WfIcon name="plus" size={15} strokeWidth={2.4} />
              새 워크플로우
            </button>
          </>
        }
      />

      <div className="wf-design-body">
        {/* 트리 카드 제목 줄은 뺐다(사용자 요청) - 화면 제목이 이미 무엇의 트리인지 말한다. */}
        <aside className="wf-tree-card">
          <div className="wf-tree-search">
            <WfIcon name="search" size={13} />
            <input
              id="wf-tree-search"
              className="wf-tree-search-input"
              aria-label="트리 검색"
              value={treeSearch}
              onChange={(event) => setTreeSearch(event.target.value)}
              placeholder="워크플로우명 검색"
            />
          </div>
          <div className="wf-tree-scroll">
            {isLoading ? <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="불러오는 중" /> : (
              <WorkflowTreeView
                // 검색어가 바뀌면 트리 모양이 달라지므로 다시 마운트해 펼침 기본값(최상단만)을 새로 먹인다.
                key={`wf-tree-${treeSearch}-${visibleTree.length}`}
                items={visibleTree}
                selectedId={selectedWorkflowId}
                onSelect={setSelectedWorkflowId}
              />
            )}
          </div>
        </aside>

        <section className="wf-list-card">
          <div className="wf-toolbar">
            <div className="wf-toolbar-left">
              {/* 지금 표가 무엇을 보여주는지 한 줄로 - 트리에서 고르면 그 워크플로우와 하위 전부다. */}
              <span className="wf-selected-chip">
                <span className="chip-dot" />
                {selectedSubtree ? `${selectedSubtree.workflow.name} · 하위 포함` : "전체 워크플로우"}
              </span>
              {/* 트리 제목 줄에 있던 «전체» 버튼. 고른 줄을 다시 눌러도 해제된다. */}
              {selectedWorkflowId !== undefined && (
                <button type="button" className="btn-tree-toggle-all"
                        onClick={() => setSelectedWorkflowId(undefined)}>전체 보기</button>
              )}
            </div>
          </div>
          <Table<WorkflowSummary>
            rowKey="id"
            size="middle"
            className="wf-design-table"
            loading={isLoading}
            dataSource={visible}
            columns={columns}
            pagination={{ pageSize: 20, showTotal: (total) => `전체 ${total}건` }}
            locale={{
              emptyText: (
                <Empty description="아직 워크플로우가 없습니다. ETL에서 만든 job을 캔버스에 배치해 워크플로우를 구성하세요." />
              ),
            }}
          />
        </section>
      </div>

      <Modal
        title="새 워크플로우"
        open={createOpen}
        onCancel={() => setCreateOpen(false)}
        onOk={() => form.submit()}
        confirmLoading={createMutation.isPending}
        okText="만들기"
        cancelText="취소"
        rootClassName="cerebro-modal"
      >
        <Form form={form} layout="vertical" onFinish={(values) => createMutation.mutate(values)}>
          {/* 워크플로우 키(=dag_id의 축)는 서버가 이름에서 만든다. 사용자가 짓게 하면
              규칙을 어긴 값이 게시 때 터진다. */}
          <Form.Item name="name" label="이름" rules={[{ required: true, message: "이름을 입력해주세요." }]}>
            <Input placeholder="DW 일배치" />
          </Form.Item>
          <Form.Item name="scheduleCron" label="스케줄"
                     extra="비워두면 수동 실행 전용입니다. 예: 0 2 * * * (매일 새벽 2시)">
            <Input placeholder="0 2 * * *" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
    </ConfigProvider>
  );
}
