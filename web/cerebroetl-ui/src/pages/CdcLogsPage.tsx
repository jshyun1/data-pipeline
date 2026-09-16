import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, DatePicker, Input, Select, Space, Table, Tag, Tooltip } from "antd";
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
import {
  listCdcEventLogs,
  listCdcProcessingLogs,
  type CdcEventLogEntry,
  type CdcProcessingLogEntry,
} from "../api/cdcLogs";
import { listPipelines } from "../api/pipelines";
import { listPipelineGroups } from "../api/pipelineGroups";

const { RangePicker } = DatePicker;

const STATUS_LABEL: Record<string, string> = {
  RUNNING: "진행 중",
  SUCCESS: "성공",
  DELAYED: "지연",
  STOPPED: "중지",
  FAILED: "실패",
};

const STATUS_COLOR: Record<string, string> = {
  RUNNING: "processing",
  SUCCESS: "success",
  DELAYED: "warning",
  STOPPED: "default",
  FAILED: "error",
};

const COMMAND_LABEL: Record<string, string> = {
  VALIDATE: "검증",
  DEPLOY: "배포",
  PREPARE: "실행 준비",
  START: "시작",
  PAUSE: "일시정지",
  STOP: "중지",
  RESTART: "재시작",
  DELETE: "삭제",
  RUNTIME_MONITOR: "런타임 감시",
  DISMISS_DRIFT: "불일치 확인",
};

type StatusFilter = "ALL" | string;

interface UnifiedCdcLogRow {
  key: string;
  kind: "processing" | "event";
  status: string;
  pipelineId: number;
  pipelineName: string;
  /** «처리»(1분 처리 구간) 또는 이벤트 이름(배포·시작·중지 …). */
  kindLabel: string;
  startedAt: string;
  endedAt?: string | null;
  durationSeconds?: number | null;
  processedCount?: number | null;
  path: string;
  message?: string | null;
  processing?: CdcProcessingLogEntry;
  event?: CdcEventLogEntry;
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

function compareText(a?: string | null, b?: string | null) {
  return (a ?? "").localeCompare(b ?? "", "ko");
}

function compareDate(a?: string | null, b?: string | null) {
  return (a ? new Date(a).getTime() : 0) - (b ? new Date(b).getTime() : 0);
}

function compareNumber(a?: number | null, b?: number | null) {
  return (a ?? -1) - (b ?? -1);
}

/** 733초를 "12분 13초"처럼. */
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

/** 펼친 줄. 표 열에서 뺀 처리 구간의 세부 값(소스·타깃·Topic·커넥터 상태·Lag)과 메시지를 보여준다. */
function DetailRow({ row }: { row: UnifiedCdcLogRow }) {
  const lines: Array<[string, ReactNode]> = [];
  if (row.processing) {
    const p = row.processing;
    lines.push(["소스 → 타깃", `${p.source} → ${p.target}`]);
    lines.push(["Topic", p.topicName || "-"]);
    // 로그 파이프라인은 소스가 Filebeat(외부 에이전트)라 Source 커넥터가 없다 -> "-".
    lines.push(["커넥터 상태", `Source ${p.sourceState ?? "-"} · Sink ${p.sinkState ?? "-"}`]);
    lines.push(["Lag", formatCount(p.consumerLag)]);
    lines.push(["일 누적 처리", formatCount(p.dailyProcessedCount)]);
  }
  if (row.event) {
    lines.push(["요청자", row.event.requestedBy ?? "시스템"]);
  }
  return (
    <div style={{ fontSize: 12 }}>
      {lines.map(([label, value]) => (
        <div key={label} style={{ display: "flex", gap: 12, lineHeight: 1.8 }}>
          <span style={{ width: 90, color: "#888", flexShrink: 0 }}>{label}</span>
          <span>{value}</span>
        </div>
      ))}
      {row.message ? (
        <pre style={{ whiteSpace: "pre-wrap", margin: lines.length ? "6px 0 0" : 0, fontSize: 12 }}>{row.message}</pre>
      ) : null}
    </div>
  );
}

/**
 * CDC 로그. ETL 로그 화면(EtlLogsPage)과 같은 구성이다(2026-09-13 요청) - 접히는 상태별 건수 대시보드,
 * 당일·전일·기간·검색·파이프라인 필터, 그리고 한 장의 표.
 *
 * <p>예전에는 «처리 이력 / 오류·상태 이력 / 실패 데이터(DLQ)» 탭으로 나뉘어 있었다. 두 이력은 한 표로 합치고
 * (시각 역순), DLQ 는 화면에서 뺐다 - 싱크 실패 정책을 «버리고 계속»에서 «멈춤»으로 바꾸기로 해서 DLQ 는 쓰이지
 * 않게 된다(docs/2026-09-09-cdc-error-policy-dlq.md). 백엔드의 DLQ·재처리 API 는 정책 전환 때 함께 정리한다.
 * Lag 추이 차트와 자동 갱신도 ETL 로그 화면에 없어 뺐고, 처리 구간의 세부 값은 줄을 펼쳐 본다.
 *
 * <ul>
 *   <li>처리 이력: 백엔드가 1분 구간마다 처리량·Lag·상태 변화가 있을 때만 남긴다(변화 없는 heartbeat 제외).
 *       처리 건수는 Sink committed offset 증가량 기준 추정치다.</li>
 *   <li>오류·상태 이력: 배포·시작·중지 같은 명령과 그 결과. 결과가 아직 없으면 «진행 중»이다.</li>
 * </ul>
 */
export function CdcLogsPage() {
  const [searchParams] = useSearchParams();
  const initialFrom = searchParams.get("from");
  const initialTo = searchParams.get("to");
  const today = dayjs();
  const initialRange: [Dayjs, Dayjs] = [
    initialFrom ? dayjs(initialFrom) : today,
    initialTo ? dayjs(initialTo) : today,
  ];
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>(initialRange);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(initialRange);
  const [keyword, setKeyword] = useState(searchParams.get("q") ?? "");
  const [pipelineFilter, setPipelineFilter] = useState<number[]>([]);
  const [dashboardOpen, setDashboardOpen] = useState(true);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("ALL");

  const from = appliedRange[0].format("YYYY-MM-DD");
  const to = appliedRange[1].format("YYYY-MM-DD");

  useEffect(() => {
    const nextFrom = searchParams.get("from");
    const nextTo = searchParams.get("to");
    if (nextFrom && nextTo) {
      const nextRange: [Dayjs, Dayjs] = [dayjs(nextFrom), dayjs(nextTo)];
      setDateRange(nextRange);
      setAppliedRange(nextRange);
    }
    setKeyword(searchParams.get("q") ?? "");
  }, [searchParams]);

  const processingQuery = useQuery({
    queryKey: ["cdc-processing-logs", from, to],
    queryFn: () => listCdcProcessingLogs(from, to),
    placeholderData: (previous) => previous,
  });
  const eventsQuery = useQuery({
    queryKey: ["cdc-event-logs", from, to],
    queryFn: () => listCdcEventLogs(from, to),
    placeholderData: (previous) => previous,
  });
  const pipelinesQuery = useQuery({ queryKey: ["pipelines"], queryFn: listPipelines });
  const groupsQuery = useQuery({ queryKey: ["pipeline-groups"], queryFn: listPipelineGroups });

  /** 파이프라인 → «전체 파이프라인 > 그룹 > 하위 그룹» 경로. CDC 관리 트리와 같은 계층이다. */
  const pathByPipelineId = useMemo(() => {
    const groups = new Map((groupsQuery.data ?? []).map((group) => [group.id, group]));
    const groupPath = (groupId: number | null | undefined) => {
      const names: string[] = [];
      const seen = new Set<number>();
      let current = groupId != null ? groups.get(groupId) : undefined;
      while (current && !seen.has(current.id)) {
        seen.add(current.id);
        names.unshift(current.name);
        current = current.parentId != null ? groups.get(current.parentId) : undefined;
      }
      return ["전체 파이프라인", ...names].join(" > ");
    };
    return new Map((pipelinesQuery.data ?? []).map((pipeline) => [pipeline.id, groupPath(pipeline.groupId)]));
  }, [groupsQuery.data, pipelinesQuery.data]);

  const allRows = useMemo<UnifiedCdcLogRow[]>(() => {
    const processingRows: UnifiedCdcLogRow[] = (processingQuery.data ?? []).map((row) => ({
      key: `processing-${row.pipelineId}-${row.occurredAt}`,
      kind: "processing",
      status: row.status,
      pipelineId: row.pipelineId,
      pipelineName: row.pipelineName,
      kindLabel: "처리",
      startedAt: row.occurredAt,
      endedAt: null,
      durationSeconds: null,
      processedCount: row.processedCount,
      path: pathByPipelineId.get(row.pipelineId) ?? "전체 파이프라인",
      message: row.message,
      processing: row,
    }));
    const eventRows: UnifiedCdcLogRow[] = (eventsQuery.data ?? []).map((row) => ({
      key: `event-${row.id}`,
      kind: "event",
      status: row.result ?? "RUNNING",
      pipelineId: row.pipelineId,
      pipelineName: row.pipelineName,
      kindLabel: COMMAND_LABEL[row.command] ?? row.command,
      startedAt: row.occurredAt,
      endedAt: row.completedAt,
      durationSeconds: row.completedAt
        ? Math.max(0, dayjs(row.completedAt).diff(dayjs(row.occurredAt), "second"))
        : null,
      processedCount: null,
      path: pathByPipelineId.get(row.pipelineId) ?? "전체 파이프라인",
      message: row.message,
      event: row,
    }));
    return [...processingRows, ...eventRows].sort((a, b) => compareDate(b.startedAt, a.startedAt));
  }, [processingQuery.data, eventsQuery.data, pathByPipelineId]);

  // 파이프라인 필터 후보: 등록된 파이프라인 + 로그에만 남은(삭제된) 파이프라인.
  const pipelineOptions = useMemo(() => {
    const names = new Map<number, string>(allRows.map((row) => [row.pipelineId, row.pipelineName]));
    for (const pipeline of pipelinesQuery.data ?? []) names.set(pipeline.id, pipeline.name);
    return [...names.entries()]
      .map(([value, label]) => ({ value, label }))
      .sort((a, b) => compareText(a.label, b.label));
  }, [allRows, pipelinesQuery.data]);

  const baseRows = useMemo(() => {
    const needle = keyword.trim().toLowerCase();
    return allRows.filter((row) => {
      if (pipelineFilter.length > 0 && !pipelineFilter.includes(row.pipelineId)) {
        return false;
      }
      if (!needle) {
        return true;
      }
      return [
        row.pipelineName, row.kindLabel, row.path, row.message,
        row.processing?.source, row.processing?.target, row.processing?.topicName,
      ].some((text) => (text ?? "").toLowerCase().includes(needle));
    });
  }, [allRows, keyword, pipelineFilter]);

  const rows = useMemo(
    () => (statusFilter === "ALL" ? baseRows : baseRows.filter((row) => row.status === statusFilter)),
    [baseRows, statusFilter],
  );

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
        .sort(([a], [b]) => compareText(STATUS_LABEL[a] ?? a, STATUS_LABEL[b] ?? b)),
    };
  }, [baseRows]);

  const applyDatePreset = (offsetDays: 0 | 1) => {
    const target = dayjs().subtract(offsetDays, "day");
    const range: [Dayjs, Dayjs] = [target, target];
    setDateRange(range);
    setAppliedRange(range);
  };
  const selectedPreset = activeDayPresetByDate(appliedRange[0], appliedRange[1]);

  const filters = (
    <Space style={{ marginBottom: 16 }} wrap>
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
        placeholder="파이프라인/Topic/메시지 검색"
        allowClear
        style={{ width: 240 }}
        value={keyword}
        onChange={(event) => setKeyword(event.target.value)}
      />
      <Select
        mode="multiple"
        allowClear
        showSearch
        optionFilterProp="label"
        placeholder="파이프라인"
        style={{ minWidth: 200 }}
        options={pipelineOptions}
        value={pipelineFilter}
        onChange={setPipelineFilter}
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
    <div>
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
            {metric("RUNNING", <RightOutlined />, "진행 중", summary.running)}
            {metric("SUCCESS", <CheckCircleFilled />, "성공", summary.success, " etl-log-monitoring-metric--success")}
            {metric("FAILED", <CloseCircleFilled />, "실패", summary.failed, " etl-log-monitoring-metric--danger")}
            {summary.extra.map(([status, count]) =>
              metric(status, null, STATUS_LABEL[status] ?? status, count, " etl-log-monitoring-metric--extra", status),
            )}
          </section>
        </div>
      </div>
      {filters}
      <Table<UnifiedCdcLogRow>
        rowKey="key"
        size="small"
        loading={(processingQuery.isLoading && !processingQuery.data) || (eventsQuery.isLoading && !eventsQuery.data)}
        dataSource={rows}
        pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
        scroll={{ x: 1150 }}
        columns={[
          {
            title: "상태",
            dataIndex: "status",
            width: 100,
            fixed: "left",
            sorter: (a, b) => compareText(STATUS_LABEL[a.status] ?? a.status, STATUS_LABEL[b.status] ?? b.status),
            render: (value: string, row) => (
              <Tooltip title={row.message || undefined}>
                <Tag color={STATUS_COLOR[value] ?? "default"}>{STATUS_LABEL[value] ?? value}</Tag>
              </Tooltip>
            ),
          },
          {
            title: "파이프라인명",
            dataIndex: "pipelineName",
            width: 180,
            sorter: (a, b) => compareText(a.pipelineName, b.pipelineName),
          },
          {
            // ETL 로그에는 없는 열. 한 표에 처리 구간과 명령 이벤트가 섞여서 무엇의 기록인지 알려야 한다.
            title: "구분",
            dataIndex: "kindLabel",
            width: 110,
            sorter: (a, b) => compareText(a.kindLabel, b.kindLabel),
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
            width: 110,
            align: "right",
            sorter: (a, b) => compareNumber(a.durationSeconds, b.durationSeconds),
            render: formatDuration,
          },
          {
            title: "처리건수",
            dataIndex: "processedCount",
            width: 110,
            align: "right",
            sorter: (a, b) => compareNumber(a.processedCount, b.processedCount),
            render: (_value: number | null | undefined, row) =>
              row.status === "FAILED" ? "-" : formatCount(row.processedCount),
          },
          {
            title: "경로",
            dataIndex: "path",
            sorter: (a, b) => compareText(a.path, b.path),
            ellipsis: true,
            render: (value: string) => <Link to="/cdc/pipelines">{value}</Link>,
          },
        ]}
        expandable={{
          expandedRowRender: (row) => <DetailRow row={row} />,
          rowExpandable: (row) => row.kind === "processing" || Boolean(row.message) || Boolean(row.event?.requestedBy),
        }}
      />
    </div>
  );
}
