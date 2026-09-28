import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, DatePicker, Input, Modal, Select, Space, Table, Tag, Tooltip } from "antd";
import {
  CheckCircleFilled,
  CloseCircleFilled,
  DownOutlined,
  RightOutlined,
  UpOutlined,
} from "@ant-design/icons";
import dayjs, { type Dayjs } from "dayjs";
import { Link, useSearchParams } from "react-router-dom";
import { activeDayPresetByDate } from "../components/dayPreset";
import { GptErrorAnalysis } from "../components/gpt/GptErrorAnalysis";
import { useGptEnabled } from "../components/gpt/useGptEnabled";
import {
  getNifiProcessGroupTree,
  listNifiExecutionLogs,
  listNifiProcessorRuns,
  type NifiExecutionLogEntry,
  type NifiProcessGroupTreeNode,
  type NifiProcessorRun,
} from "../api/platform";

const { RangePicker } = DatePicker;

const EVENT_STATUS_LABEL: Record<string, string> = {
  RUNNING: "진행 중",
  SUCCESS: "성공",
  FAILED: "실패",
};

const EVENT_STATUS_COLOR: Record<string, string> = {
  RUNNING: "processing",
  SUCCESS: "success",
  FAILED: "error",
};

const ETL_LOG_GPT_HEADINGS = ["원인 추정", "조치 방법", "확인할 것"] as const;
const ETL_LOG_GPT_HEADING_LABELS = {
  "원인 추정": "원인",
  "조치 방법": "조치방법",
  "확인할 것": "확인할 것",
};

type UnifiedEtlLogKind = "run" | "event";
type StatusFilter = "ALL" | string;

interface UnifiedEtlLogRow {
  key: string;
  kind: UnifiedEtlLogKind;
  status: string;
  jobName: string;
  startedAt: string;
  endedAt?: string | null;
  durationSeconds?: number | null;
  insertedCount?: number | null;
  jobId?: number | null;
  groupId?: string | null;
  groupName?: string | null;
  path: string;
  processorName?: string | null;
  message?: string | null;
}

function formatDateTime(value?: string | null) {
  if (!value) {
    return "-";
  }
  return new Intl.DateTimeFormat("ko-KR", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function formatCount(value?: number | null) {
  return value == null ? "-" : value.toLocaleString();
}

function eventGroupName(row: NifiExecutionLogEntry) {
  return row.groupName ?? row.rootGroupName ?? "-";
}

function runGroupName(row: NifiProcessorRun) {
  return row.groupName ?? "-";
}

function compareText(a?: string | null, b?: string | null) {
  return (a ?? "").localeCompare(b ?? "", "ko");
}

function compareDate(a?: string | null, b?: string | null) {
  return (a ? new Date(a).getTime() : 0) - (b ? new Date(b).getTime() : 0);
}

function compareNumber(a?: number | null, b?: number | null) {
  return (a ?? -1) - (b ?? -1);
}

/** 733초를 "12분 13초"처럼. 한 관측 주기 안에 끝난 구간은 0초로 들어온다. */
function formatDuration(seconds?: number | null) {
  if (seconds == null) {
    return "-";
  }
  if (seconds < 60) {
    return `${seconds}초`;
  }
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  if (minutes < 60) {
    return rest === 0 ? `${minutes}분` : `${minutes}분 ${rest}초`;
  }
  const hours = Math.floor(minutes / 60);
  return `${hours}시간 ${minutes % 60}분`;
}

// NiFi에는 Airflow의 dag_run 같은 "실행 이력" 개념이 없다 - Provenance 조회는 이 환경에서
// 인덱스/이벤트파일 불일치로 구조적으로 안 되는 것으로 확인됐고(재시작/저장소 재구축 후에도
// 재현), 프로세서 단위 Status History도 항상 비어 있다(그룹 단위는 되지만 그룹 안 여러
// 테이블이 섞여서 프로세서별 구분이 안 됨). 그래서 두 갈래 데이터를 하나의 목록으로 합친다:
//  - 처리 이력: 백엔드가 15초마다 활성 스레드와 적재 카운터를 관측해 "실행 구간"을 직접
//    만든다(nifi_processor_run). 시작/종료는 관측값이라 최대 15초 오차가 있다.
//  - 오류·상태 이력: NiFi bulletin(경고/에러)을 30초마다 긁어서 남긴다. 카운터는 "늘었을
//    때"만 보이므로 실패를 표현할 방법이 원래 없었고, 그래서 DB 인증 실패나 OOM처럼 실제로
//    파이프라인이 죽은 날에도 이 화면엔 아무것도 안 남았다.
export function EtlLogsPage() {
  const [searchParams] = useSearchParams();
  const initialFrom = searchParams.get("from");
  const initialTo = searchParams.get("to");
  const defaultDate = dayjs().subtract(1, "day");
  const initialRange: [Dayjs, Dayjs] = [
    initialFrom ? dayjs(initialFrom) : defaultDate,
    initialTo ? dayjs(initialTo) : defaultDate,
  ];
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>(initialRange);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(initialRange);
  const [keyword, setKeyword] = useState(searchParams.get("q") ?? "");
  const [groupFilter, setGroupFilter] = useState<string[]>([]);
  const [dashboardOpen, setDashboardOpen] = useState(true);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("ALL");
  const [detailRow, setDetailRow] = useState<UnifiedEtlLogRow | null>(null);

  const from = appliedRange[0].format("YYYY-MM-DD");
  const to = appliedRange[1].format("YYYY-MM-DD");

  useEffect(() => {
    const nextFrom = searchParams.get("from");
    const nextTo = searchParams.get("to");
    const nextKeyword = searchParams.get("q") ?? "";
    if (nextFrom && nextTo) {
      const nextRange: [Dayjs, Dayjs] = [dayjs(nextFrom), dayjs(nextTo)];
      setDateRange(nextRange);
      setAppliedRange(nextRange);
    }
    setKeyword(nextKeyword);
  }, [searchParams]);

  const runsQuery = useQuery({
    queryKey: ["nifi-processor-runs", from, to],
    queryFn: () => listNifiProcessorRuns(from, to),
    placeholderData: (previous) => previous,
  });

  const eventsQuery = useQuery({
    queryKey: ["nifi-execution-logs", from, to],
    queryFn: () => listNifiExecutionLogs(from, to),
    placeholderData: (previous) => previous,
  });

  const treeQuery = useQuery({ queryKey: ["nifi-pg-tree"], queryFn: getNifiProcessGroupTree });

  const pathByGroupId = useMemo(() => {
    const paths = new Map<string, string>();
    const walk = (node: NifiProcessGroupTreeNode, parents: string[]) => {
      const nextPath = [...parents, node.name];
      paths.set(node.id, nextPath.join(" / "));
      node.children.forEach((child) => walk(child, nextPath));
    };
    if (treeQuery.data) {
      walk(treeQuery.data, []);
    }
    return paths;
  }, [treeQuery.data]);

  const allRows = useMemo<UnifiedEtlLogRow[]>(() => {
    const runRows: UnifiedEtlLogRow[] = (runsQuery.data ?? []).map((row) => {
      const jobName = runGroupName(row);
      return {
        key: `run-${row.id}`,
        kind: "run",
        status: row.status === "RUNNING" ? "RUNNING" : "SUCCESS",
        jobName,
        startedAt: row.startedAt,
        endedAt: row.endedAt,
        durationSeconds: row.durationSeconds,
        insertedCount: row.insertedCount,
        jobId: null,
        groupId: row.groupId,
        groupName: row.groupName,
        path: (row.groupId && pathByGroupId.get(row.groupId)) || jobName,
        processorName: row.processorName,
      };
    });

    const eventRows: UnifiedEtlLogRow[] = (eventsQuery.data ?? [])
      .filter((row) => row.status !== "SUCCESS")
      .map((row) => {
        const jobName = eventGroupName(row);
        return {
          key: `event-${row.id}`,
          kind: "event",
          status: row.status,
          jobName,
          startedAt: row.occurredAt,
          endedAt: null,
          durationSeconds: null,
          insertedCount: null,
          jobId: row.jobId,
          groupId: row.groupId,
          groupName: row.groupName,
          path: (row.groupId && pathByGroupId.get(row.groupId)) || jobName,
          processorName: row.processorName,
          message: row.message,
        };
      });

    return [...runRows, ...eventRows].sort((a, b) => compareDate(b.startedAt, a.startedAt));
  }, [runsQuery.data, eventsQuery.data, pathByGroupId]);

  // 그룹 필터 후보는 통합 데이터에서 뽑는다 - 실패만 있고 적재는 없는 그룹도 골라볼 수 있어야 한다.
  const groupOptions = useMemo(() => {
    const names = new Set<string>();
    for (const row of allRows) {
      if (row.jobName !== "-") names.add(row.jobName);
    }
    return [...names].sort().map((name) => ({ value: name, label: name }));
  }, [allRows]);

  const baseRows = useMemo(() => {
    const needle = keyword.trim().toLowerCase();
    return allRows.filter((row) => {
      if (groupFilter.length > 0 && !groupFilter.includes(row.jobName)) {
        return false;
      }
      if (!needle) {
        return true;
      }
      return [row.processorName, row.jobName, row.status, row.path, row.message].some((text) =>
        (text ?? "").toLowerCase().includes(needle),
      );
    });
  }, [allRows, keyword, groupFilter]);

  const rows = useMemo(() => {
    if (statusFilter === "ALL") {
      return baseRows;
    }
    return baseRows.filter((row) => row.status === statusFilter);
  }, [baseRows, statusFilter]);

  const summary = useMemo(() => {
    const counts = new Map<string, number>();
    for (const row of baseRows) {
      counts.set(row.status, (counts.get(row.status) ?? 0) + 1);
    }
    return {
      total: baseRows.length,
      running: counts.get("RUNNING") ?? 0,
      success: counts.get("SUCCESS") ?? 0,
      failed: counts.get("FAILED") ?? 0,
      extra: [...counts.entries()]
        .filter(([status]) => !["RUNNING", "SUCCESS", "FAILED"].includes(status))
        .sort(([a], [b]) => compareText(EVENT_STATUS_LABEL[a] ?? a, EVENT_STATUS_LABEL[b] ?? b)),
    };
  }, [baseRows]);

  const applyDatePreset = (offsetDays: 0 | 1) => {
    const target = dayjs().subtract(offsetDays, "day");
    const range: [Dayjs, Dayjs] = [target, target];
    setDateRange(range);
    setAppliedRange(range);
  };
  // 이 화면의 RangePicker 는 날짜 단위라 시각까지 비교하는 판정으로는 일치하지 않는다.
  const selectedPreset = activeDayPresetByDate(appliedRange[0], appliedRange[1]);

  const detailText = (row: UnifiedEtlLogRow) => [
    `상태: ${EVENT_STATUS_LABEL[row.status] ?? row.status}`,
    `Job: ${row.jobName}`,
    `Processor: ${row.processorName ?? "-"}`,
    `경로: ${row.path || "-"}`,
    `발생 시각: ${formatDateTime(row.startedAt)}`,
    row.message ? `오류 내용:\n${row.message}` : "오류 내용: -",
  ].join("\n");

  function LogDetailModal({ row, onClose }: { row: UnifiedEtlLogRow | null; onClose: () => void }) {
    const gptEnabled = useGptEnabled();
    const target: Record<string, string> = {};
    if (row?.jobId != null) target.jobId = String(row.jobId);
    if (row?.groupId) target.groupId = row.groupId;
    const canAnalyze = row != null && gptEnabled && row.status === "FAILED" && (row.jobId != null || Boolean(row.groupId));
    return (
      <Modal
        open={Boolean(row)}
        title={row ? `${row.jobName} 실패 로그` : undefined}
        footer={null}
        width={960}
        rootClassName="cerebro-modal etl-log-detail-modal-root"
        onCancel={onClose}
      >
        {row ? (
          <div className="etl-log-detail-modal">
            <section className="etl-log-detail-section">
              <h3>실패 원인</h3>
              {canAnalyze ? (
                <GptErrorAnalysis
                  targetType="ETL_JOB"
                  target={target}
                  errorText={detailText(row)}
                  onClose={onClose}
                  headings={[...ETL_LOG_GPT_HEADINGS]}
                  headingLabels={ETL_LOG_GPT_HEADING_LABELS}
                />
              ) : (
                <div className="etl-log-detail-gpt-empty">GPT 분석을 사용할 수 없습니다.</div>
              )}
            </section>
            <section className="etl-log-detail-raw">
              <h3>에러 로그</h3>
              <pre>{row.message ?? "상세 정보 없음"}</pre>
            </section>
          </div>
        ) : null}
      </Modal>
    );
  }

  const filters = (
    <Space className="etl-log-filter-toolbar" wrap>
      <Space.Compact>
        <Button type={selectedPreset === 0 ? "primary" : "default"} onClick={() => applyDatePreset(0)}>당일</Button>
        <Button type={selectedPreset === 1 ? "primary" : "default"} onClick={() => applyDatePreset(1)}>전일</Button>
      </Space.Compact>
      <RangePicker
        value={dateRange}
        onChange={(value) => {
          if (value && value[0] && value[1]) {
            setDateRange([value[0], value[1]]);
          }
        }}
        allowClear={false}
      />
      <Input
        placeholder="job/경로/메시지 검색"
        allowClear
        style={{ width: 240 }}
        value={keyword}
        onChange={(event) => setKeyword(event.target.value)}
      />
      <Select
        mode="multiple"
        allowClear
        placeholder="그룹(파이프라인)"
        style={{ minWidth: 200 }}
        options={groupOptions}
        value={groupFilter}
        onChange={setGroupFilter}
      />
      <Button type="primary" onClick={() => setAppliedRange(dateRange)}>
        조회
      </Button>
    </Space>
  );

  const metric = (
    status: StatusFilter,
    icon: ReactNode | null,
    label: string,
    value: number,
    modifier = "",
    key?: string,
  ) => (
    <button
      key={key}
      type="button"
      className={`etl-log-monitoring-metric${modifier}${statusFilter === status ? " active" : ""}`}
      onClick={() => setStatusFilter(statusFilter === status && status !== "ALL" ? "ALL" : status)}
    >
      {icon}
      <span>{label}<strong>{value.toLocaleString()}</strong></span>
    </button>
  );

  return (
    <div className="etl-log-page">
      <div className={`etl-log-dashboard-curtain${dashboardOpen ? " open" : " closed"}`}>
        <button
          type="button"
          className="etl-log-dashboard-toggle"
          onClick={() => setDashboardOpen((open) => !open)}
          aria-expanded={dashboardOpen}
        >
          <span>대시보드</span>
          {dashboardOpen ? <UpOutlined /> : <DownOutlined />}
        </button>
        <div className="etl-log-dashboard-panel">
          <section className="etl-log-monitoring-card">
            {metric("ALL", null, "전체 작업", summary.total)}
            {metric("RUNNING", <RightOutlined />, "실행 중", summary.running)}
            {metric("SUCCESS", <CheckCircleFilled />, "성공", summary.success, " etl-log-monitoring-metric--success")}
            {metric("FAILED", <CloseCircleFilled />, "실패", summary.failed, " etl-log-monitoring-metric--danger")}
            {summary.extra.map(([status, count]) =>
              metric(status, null, EVENT_STATUS_LABEL[status] ?? status, count, " etl-log-monitoring-metric--extra", status),
            )}
          </section>
        </div>
      </div>
      {filters}
      <Table<UnifiedEtlLogRow>
        className="etl-log-table"
        rowKey="key"
        size="small"
        loading={(runsQuery.isLoading && !runsQuery.data) || (eventsQuery.isLoading && !eventsQuery.data)}
        dataSource={rows}
        pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
        scroll={{ x: 1050 }}
        columns={[
          {
            title: "상태",
            dataIndex: "status",
            width: 100,
            fixed: "left",
            sorter: (a, b) => compareText(EVENT_STATUS_LABEL[a.status] ?? a.status, EVENT_STATUS_LABEL[b.status] ?? b.status),
            render: (value: string, row) => (
              <Tooltip title={row.message || undefined}>
                <Tag
                  className={row.status === "FAILED" && row.message ? "etl-log-status-detail-trigger" : undefined}
                  color={EVENT_STATUS_COLOR[value] ?? "default"}
                  onClick={(event) => {
                    if (row.status === "FAILED" && row.message) {
                      event.stopPropagation();
                      setDetailRow(row);
                    }
                  }}
                >
                  {EVENT_STATUS_LABEL[value] ?? value}
                </Tag>
              </Tooltip>
            ),
          },
          {
            title: "job명",
            dataIndex: "jobName",
            width: 180,
            sorter: (a, b) => compareText(a.jobName, b.jobName),
          },
          {
            title: "시작시간",
            dataIndex: "startedAt",
            width: 180,
            sorter: (a, b) => compareDate(a.startedAt, b.startedAt),
            defaultSortOrder: "descend",
            render: formatDateTime,
          },
          {
            title: "종료시간",
            dataIndex: "endedAt",
            width: 180,
            sorter: (a, b) => compareDate(a.endedAt, b.endedAt),
            render: formatDateTime,
          },
          {
            title: "소요시간",
            dataIndex: "durationSeconds",
            width: 120,
            align: "right",
            sorter: (a, b) => compareNumber(a.durationSeconds, b.durationSeconds),
            render: formatDuration,
          },
          {
            title: "적재건수",
            dataIndex: "insertedCount",
            width: 120,
            align: "right",
            sorter: (a, b) => compareNumber(a.insertedCount, b.insertedCount),
            render: (_value: number | null | undefined, row) =>
              row.status === "FAILED" ? "-" : formatCount(row.insertedCount),
          },
          {
            title: "경로",
            dataIndex: "path",
            sorter: (a, b) => compareText(a.path, b.path),
            ellipsis: true,
            render: (value: string, row) =>
              row.groupId ? (
                <Link
                  to={`/etl/manage?processGroupId=${encodeURIComponent(row.groupId)}`}
                  onClick={(event) => event.stopPropagation()}
                >
                  {value}
                </Link>
              ) : (
                value || "-"
              ),
          },
        ]}
        expandable={{
          rowExpandable: (row) => row.status !== "SUCCESS" && Boolean(row.message),
          expandedRowKeys: [],
          expandIcon: ({ record, expandable }) =>
            expandable ? (
              <button
                type="button"
                className="etl-log-detail-open"
                aria-label="오류 상세 보기"
                title="오류 상세 보기"
                onClick={(event) => {
                  event.stopPropagation();
                  setDetailRow(record);
                }}
              >
                +
              </button>
            ) : null,
        }}
        onRow={(row) => ({
          className: row.status === "FAILED" && row.message ? "etl-log-clickable-row" : undefined,
          onClick: () => {
            if (row.status === "FAILED" && row.message) {
              setDetailRow(row);
            }
          },
        })}
      />
      <LogDetailModal row={detailRow} onClose={() => setDetailRow(null)} />
    </div>
  );
}
