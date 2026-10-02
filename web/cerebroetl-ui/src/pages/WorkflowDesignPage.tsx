import { useEffect, useState, type ReactNode } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ConfigProvider, Empty, Input, message, Popconfirm, Select, Space, Table } from "antd";
import type { ColumnsType } from "antd/es/table";
import dayjs from "dayjs";
import { useAuth } from "../auth/AuthContext";
import { syncEtlJobs } from "../api/etlJobs";
import {
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
import { TreeHead } from "../components/TreeHead";
import { cronToKoreanText, parseCronToPreset } from "../utils/schedulePreset";
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

/** 표 위 «게시 상태» 필터. 표의 게시 상태 열과 같은 말을 쓴다. */
const PUBLISH_OPTIONS = [
  { value: "PUBLISHED", label: "게시됨" },
  { value: "UNPUBLISHED", label: "미게시" },
];

/**
 * 목록 표의 열별 정렬(2026-09-11 요청). antd 는 오름차순 비교 하나만 받고 내림차순은 뒤집어 쓴다.
 * 값이 같으면 이름으로 한 번 더 갈라서, 누를 때마다 같은 값끼리 순서가 흔들리지 않게 한다.
 */
const compareName = (a: WorkflowSummary, b: WorkflowSummary) => a.name.localeCompare(b.name, "ko");

/**
 * 스케줄 열의 정렬 기준: 정기 실행, 선행 완료 후, 수동 실행 순이다.
 *
 * <p>정기 실행끼리는 매시간·매일·매주(요일 순) 묶음 안에서 이른 시각이 앞이다. 크론 문자열 그대로 비교하면
 * «10 14 * * *»(14:10)가 «30 10 * * *»(10:30)보다 앞에 와서 시각 순이 아니다. 프리셋으로 못 읽는
 * 크론은 정기 실행 맨 뒤에 둔다.
 */
function scheduleSortKey(row: WorkflowSummary): number[] {
  if (row.upstreamCount) return [1, row.upstreamCount];
  if (!row.scheduleCron) return [2];
  const parsed = parseCronToPreset(row.scheduleCron);
  if (!parsed.matched) return [0, 3];
  const group = parsed.preset === "hourly" ? 0 : parsed.preset === "daily" ? 1 : 2;
  return [0, group, parsed.preset === "weekly" ? parsed.weekday : 0, parsed.hour * 60 + parsed.minute];
}

function compareSchedule(a: WorkflowSummary, b: WorkflowSummary): number {
  const ka = scheduleSortKey(a);
  const kb = scheduleSortKey(b);
  for (let index = 0; index < Math.max(ka.length, kb.length); index += 1) {
    const diff = (ka[index] ?? 0) - (kb[index] ?? 0);
    if (diff !== 0) return diff;
  }
  return (a.scheduleCron ?? "").localeCompare(b.scheduleCron ?? "") || compareName(a, b);
}

/** 게시 상태 열: 미게시가 앞, 게시됨은 게시 시각이 이른 것부터. 내림차순이면 최근 게시가 맨 위다. */
function comparePublish(a: WorkflowSummary, b: WorkflowSummary): number {
  const time = (row: WorkflowSummary) => (row.published && row.publishedAt ? dayjs(row.publishedAt).valueOf() : 0);
  return Number(a.published) - Number(b.published) || time(a) - time(b) || compareName(a, b);
}

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
  collapseSignal = 0,
  expandSignal = 0,
}: {
  items: WorkflowTreeItem[];
  selectedId?: number;
  onSelect: (id: number | undefined) => void;
  /** 값이 바뀌면 전부 접는다(트리 머리의 «전체 접기»). */
  collapseSignal?: number;
  /** 값이 바뀌면 하위가 있는 칸을 전부 연다(«전체 열기»). */
  expandSignal?: number;
}) {
  const [openIds, setOpenIds] = useState<Set<number>>(() => new Set(items.map((item) => item.workflow.id)));

  useEffect(() => {
    if (collapseSignal > 0) {
      setOpenIds(new Set());
    }
  }, [collapseSignal]);

  useEffect(() => {
    if (expandSignal > 0) {
      const ids = new Set<number>();
      const walk = (list: WorkflowTreeItem[]) => list.forEach((item) => {
        if (item.children.length > 0) { ids.add(item.workflow.id); walk(item.children); }
      });
      walk(items);
      setOpenIds(ids);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expandSignal]);

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
  const [selectedWorkflowId, setSelectedWorkflowId] = useState<number>();
  const [treeSearch, setTreeSearch] = useState("");
  // 표 위 필터. 왼쪽 트리 검색과는 역할이 다르다 - 트리는 «어느 갈래를 볼지», 이쪽은 «그 안에서 무엇을 추릴지».
  const [nameFilter, setNameFilter] = useState("");
  const [publishFilter, setPublishFilter] = useState<string[]>([]);

  const { data, isLoading, refetch, isFetching } = useQuery({ queryKey: ["workflows"], queryFn: listWorkflows });
  // 트리 머리 «전체 접기»: 값이 바뀌면 트리가 전부 접힌다(ETL 관리 트리와 같은 단추).
  const [collapseSignal, setCollapseSignal] = useState(0);
  const [expandSignal, setExpandSignal] = useState(0);

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
  const scoped = selectedSubtree ? flattenWorkflowTree([selectedSubtree]) : workflows;
  const nameNeedle = nameFilter.trim().toLowerCase();
  const visible = scoped.filter((row) => {
    // 이름으로 못 찾을 때 DAG ID 로도 찾히게 한다 - 로그·Airflow 쪽에서 DAG ID 만 들고 오는 일이 잦다.
    if (nameNeedle
        && !row.name.toLowerCase().includes(nameNeedle)
        && !row.dagId.toLowerCase().includes(nameNeedle)) {
      return false;
    }
    if (publishFilter.length && !publishFilter.includes(row.published ? "PUBLISHED" : "UNPUBLISHED")) {
      return false;
    }
    return true;
  });

  const columns: ColumnsType<WorkflowSummary> = [
    {
      title: "워크플로우",
      sorter: compareName,
      render: (_, row) => (
        <div className="wf-info-cell">
          <button type="button" className="wf-main-name" onClick={() => navigate(`/workflows/design/${row.id}`)}>
            {row.name}
          </button>
          {/* DAG ID(etl_wf_…) 아래 줄은 이름과 겹쳐 보여 뺐다(2026-09-11 요청). 로그 화면 속성창에서 볼 수 있다. */}
        </div>
      ),
    },
    {
      title: "job 수",
      dataIndex: "jobCount",
      sorter: (a, b) => a.jobCount - b.jobCount || compareName(a, b),
      width: 90,
      align: "center",
      render: (value: number) => <span className="wf-job-count">{value}</span>,
    },
    {
      title: "스케줄",
      width: 180,
      sorter: compareSchedule,
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
              {/* 크론 문자열 대신 «매일 오전 10시 30분» 꼴(2026-09-11 요청). */}
              {cronToKoreanText(row.scheduleCron)}
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
      sorter: comparePublish,
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
                  onClick={() => navigate(`/workflows/design/${row.id}`)}>수정</button>
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
      {/* 조작 줄은 아래 목록 카드 안 필터 띠로 옮겼다(2026-09-18 요청) - CDC 관리 화면과 같은 구성이다.
          actions 를 주지 않으면 이 머리줄은 아무것도 그리지 않는다. */}
      <WorkflowPageHeader
        title="워크플로우 관리"
        icon={<WfIcon name="calendar" size={22} strokeWidth={2.2} />}
      />

      <div className="wf-design-body">
        {/* 트리 카드 제목 줄은 뺐다(사용자 요청) - 화면 제목이 이미 무엇의 트리인지 말한다. */}
        <aside className="wf-tree-card">
          <TreeHead onRefresh={() => void refetch()} refreshing={isFetching}
                    refreshTitle="워크플로우 목록을 다시 불러옵니다"
                    onCollapseAll={() => setCollapseSignal((n) => n + 1)}
                    onExpandAll={() => setExpandSignal((n) => n + 1)} />
          <input
            id="wf-tree-search"
            className="nifi-tree-search-input"
            aria-label="트리 검색"
            value={treeSearch}
            onChange={(event) => setTreeSearch(event.target.value)}
            placeholder="워크플로우명"
          />
          <div className="wf-tree-scroll">
            {isLoading ? <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="불러오는 중" /> : (
              <WorkflowTreeView
                // 검색어가 바뀌면 트리 모양이 달라지므로 다시 마운트해 펼침 기본값(최상단만)을 새로 먹인다.
                key={`wf-tree-${treeSearch}-${visibleTree.length}`}
                items={visibleTree}
                collapseSignal={collapseSignal}
                expandSignal={expandSignal}
                selectedId={selectedWorkflowId}
                onSelect={setSelectedWorkflowId}
              />
            )}
          </div>
        </aside>

        <section className="wf-list-card">
          {/* 표 위 «○○ · 하위 포함 / 전체 보기» 줄은 뺐다(2026-09-11 요청) - 무엇을 골랐는지는 왼쪽 트리가
              이미 보여준다. 선택 해제는 트리에서 고른 줄을 다시 누르면 된다. */}
          {/* 필터 띠: 왼쪽에 조회 조건, 오른쪽 끝에 주 동작. CDC 관리 화면(std-filter-card)과 같은 마크업이다. */}
          <div className="std-filter-card wf-list-filter">
            <Space wrap>
              <Input
                placeholder="이름 검색"
                allowClear
                style={{ width: 200 }}
                value={nameFilter}
                onChange={(event) => setNameFilter(event.target.value)}
              />
              <Select
                mode="multiple"
                allowClear
                placeholder="게시 상태"
                style={{ minWidth: 160 }}
                options={PUBLISH_OPTIONS}
                value={publishFilter}
                onChange={setPublishFilter}
              />
            </Space>
            <Space wrap className="std-push-end">
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
              {/* 생성은 이름 없이 빈 캔버스에서 시작한다(«생성» 메뉴와 같은 길). */}
              <button type="button" className="btn-wf-create" disabled={!canWrite}
                      onClick={() => navigate("/workflows/create")}>
                <WfIcon name="plus" size={15} strokeWidth={2.4} />
                새 워크플로우
              </button>
            </Space>
          </div>
          <Table<WorkflowSummary>
            rowKey="id"
            size="middle"
            className="wf-design-table"
            loading={isLoading}
            dataSource={visible}
            columns={columns}
            // 페이지당 건수를 고를 수 있게 한다(2026-09-11 요청). 처음 값만 20 으로 주고(defaultPageSize)
            // 이후 선택은 표가 기억한다. 앱에 antd 한국어 설정이 없어서 이번에 생긴 문구만 한국어로 준다.
            pagination={{
              defaultPageSize: 20,
              showSizeChanger: true,
              pageSizeOptions: [10, 20, 50, 100],
              showTotal: (total) => `총 ${total}건`,
              locale: { items_per_page: "/ 페이지" },
            }}
            locale={{
              triggerAsc: "오름차순 정렬",
              triggerDesc: "내림차순 정렬",
              cancelSort: "정렬 해제",
              emptyText: (
                <Empty description="아직 워크플로우가 없습니다. ETL에서 만든 job을 캔버스에 배치해 워크플로우를 구성하세요." />
              ),
            }}
          />
        </section>
      </div>

    </div>
    </ConfigProvider>
  );
}
