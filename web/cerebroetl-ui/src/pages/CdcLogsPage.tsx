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
import { StdPage } from "../components/StdPage";
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
  SINK_CONFIG_REFRESH: "싱크 설정 재적용",
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
  processedCount?: number | null;
  /** 처리 줄만: 그날 0시부터의 누적 처리 건수 · Sink 가 아직 처리하지 않은 메시지 수(추정). 명령 줄은 없다. */
  dailyProcessedCount?: number | null;
  consumerLag?: number | null;
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

/** GPT 에 넘길 «화면에 보이는 오류»: 줄의 구분·시각·상태와 메시지. 커넥터 오류 추적·파이프라인 구성은 서버가 붙인다. */
function screenErrorText(row: UnifiedCdcLogRow) {
  const lines = [
    `구분: ${row.kindLabel}`,
    `시각: ${formatDateTime(row.startedAt)}${row.endedAt ? ` ~ ${formatDateTime(row.endedAt)}` : ""}`,
    `상태: ${STATUS_LABEL[row.status] ?? row.status}`,
  ];
  if (row.processing) {
    const p = row.processing;
    lines.push(`커넥터 상태: Source ${p.sourceState ?? "-"} · Sink ${p.sinkState ?? "-"}`, `Lag: ${p.consumerLag}`);
  }
  if (row.message) {
    lines.push(`메시지:\n${row.message}`);
  }
  return lines.join("\n");
}

/**
 * 상세로 볼 것이 있는 줄인가. 없으면 [+] 를 그리지 않는다(눌러도 빈 창만 뜨므로).
 * 요청자만 있던 줄은 빠졌다 - 요청자를 창에서 뺐으므로 더 보여 줄 것이 없다(2026-09-27).
 */
function hasDetail(row: UnifiedCdcLogRow) {
  return row.kind === "processing" || row.status === "FAILED" || Boolean(row.message);
}

/** 펼친 줄에 보여 주던 항목들. 이제는 상세 창이 실패가 아닌 줄의 «로그» 상자에 쓴다. */
function detailLines(row: UnifiedCdcLogRow): Array<[string, ReactNode]> {
  const lines: Array<[string, ReactNode]> = [];
  lines.push(["구분", `${row.kindLabel} · ${row.status}`]);
  lines.push(["시각", `${formatDateTime(row.startedAt)}${row.endedAt ? ` ~ ${formatDateTime(row.endedAt)}` : ""}`]);
  if (row.processing) {
    const p = row.processing;
    lines.push(["소스 → 타깃", `${p.source} → ${p.target}`]);
    lines.push(["Topic", p.topicName || "-"]);
    // 로그 파이프라인은 소스가 Filebeat(외부 에이전트)라 Source 커넥터가 없다 -> "-".
    lines.push(["커넥터 상태", `Source ${p.sourceState ?? "-"} · Sink ${p.sinkState ?? "-"}`]);
    lines.push(["Lag", formatCount(p.consumerLag)]);
    lines.push(["일 누적 처리", formatCount(p.dailyProcessedCount)]);
  }
  return lines;
}

/**
 * 로그 한 줄의 상세 창(2026-09-27 요청).
 *
 * <p>예전에는 줄을 펼쳐 표 아래에 붙였다. 메시지가 길면 표가 밀려 앞뒤 줄을 같이 못 봤고, GPT 해석까지 열면
 * 화면 대부분을 그 한 줄이 차지했다. 워크플로우 로그의 실패 로그 창과 같은 자리(팝업)로 옮긴다.
 *
 * <p>GPT 해석은 <b>창을 열 때 바로</b> 시작한다 - 실패 줄을 여는 이유가 «왜 실패했나»라서 한 번 더 누르게 할 까닭이 없다.
 * GPT 를 설정하지 않았거나(설정 화면에서 제공자를 고르지 않았거나) 실패 줄이 아니면 로그 정보만 보여 준다.
 */
function CdcLogDetailModal({ row, pipelineAlive, onClose }: {
  row: UnifiedCdcLogRow | null;
  /** 그 파이프라인이 아직 살아 있는가. 지워진 파이프라인은 GPT 가 볼 대상이 없다(수집기가 «찾을 수 없음»). */
  pipelineAlive: boolean;
  onClose: () => void;
}) {
  // 워크플로우 로그 · ETL 로그 창과 같은 모양(2026-10-02 요청): 맨 위 강조 띠, 제목은 파이프라인 이름,
  // 열자마자 GPT 해석, 그 아래 로그(워크플로우 창의 «실패 원인» 상자 모양). 구분 · 시각 줄은 뺐다.
  const gptEnabled = useGptEnabled();
  // 지운 파이프라인의 실패도 해석한다(2026-10-06) - 서버가 지울 때 남긴 이름 · 소스 · 타깃과 명령 이력, 오류 내용으로 해석한다.
  const analyzable = Boolean(row) && row?.status === "FAILED" && Boolean(row?.pipelineId);
  return (
    <Modal
      open={Boolean(row)}
      onCancel={onClose}
      footer={null}
      width={960}
      destroyOnHidden
      rootClassName="cerebro-modal accent-modal"
      title={row ? row.pipelineName || "로그" : undefined}
    >
      {row && (
        <section className="failure-log-task">
          {/* GPT 해석은 창을 열면 바로 시작하고 창이 열려 있는 동안 늘 보인다 - 안의 «닫기»를 뺐다(2026-10-13 요청). */}
          {gptEnabled && analyzable && (
            <GptErrorAnalysis
              key={row.key}
              targetType="CDC_PIPELINE"
              target={{ pipelineId: String(row.pipelineId) }}
              errorText={screenErrorText(row)}
            />
          )}
          {gptEnabled && row.status === "FAILED" && !pipelineAlive && (
            <div className="failure-log-note">
              이 파이프라인은 삭제됐습니다. 커넥터 · 실행 상태 없이 지울 때 남긴 정보와 오류 내용으로 해석합니다.
            </div>
          )}
          {row.status === "FAILED" ? (
            <div className="failure-log-block">
              <div className="failure-log-label">에러 로그</div>
              {row.message
                ? <pre className="failure-log-exception">{row.message}</pre>
                : <div className="failure-log-empty">메시지가 없습니다.</div>}
            </div>
          ) : (
            // 실패가 아닌 줄(처리 이력 · 명령 결과)은 GPT 해석이 없고, 구분 · 토픽 · Lag 같은 값이 곧 내용이다.
            <div className="failure-log-block">
              <div className="failure-log-label">로그</div>
              <pre className="failure-log-lines">
                {detailLines(row).map(([label, value]) => <div key={label}>{label}: {value}</div>)}
                {row.message && <div>{row.message}</div>}
              </pre>
            </div>
          )}
        </section>
      )}
    </Modal>
  );
}

/**
 * CDC 로그. ETL 로그 화면(EtlLogsPage)과 같은 구성이다(2026-09-13 요청) - 접히는 상태별 건수 대시보드,
 * 당일·전일·기간·검색·파이프라인 필터, 그리고 한 장의 표.
 * 표의 열은 CDC 에 맞게 다시 골랐다(2026-10-10 요청): 상태 · 파이프라인 · 구분 · 시각 · 처리건수 · 일 누적 처리건수 ·
 * 지연건수 · 경로. ETL 에서 온 종료 · 소요시간은 뺐고, 상세 창에만 있던 일 누적 · 지연(Lag)을 열로 꺼냈다.
 *
 * <p>예전에는 «처리 이력 / 오류·상태 이력 / 실패 데이터(DLQ)» 탭으로 나뉘어 있었다. 두 이력은 한 표로 합치고
 * (시각 역순), DLQ 는 화면에서 뺐다 - 싱크 실패 정책을 «버리고 계속»에서 «멈춤»으로 바꾸기로 해서 DLQ 는 쓰이지
 * 않게 된다(docs/2026-09-09-cdc-error-policy-dlq.md). 백엔드의 DLQ·재처리 API 도 2026-09-17 정책 전환 때 함께 뺐다.
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
  /** [+] 로 연 상세 창의 대상 줄. null 이면 닫힌 상태다. */
  const [detailRow, setDetailRow] = useState<UnifiedCdcLogRow | null>(null);
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
      processedCount: row.processedCount,
      dailyProcessedCount: row.dailyProcessedCount,
      consumerLag: row.consumerLag,
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
      processedCount: null,
      dailyProcessedCount: null,
      consumerLag: null,
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
      stopped: counts.get("STOPPED") ?? 0,
      delayed: counts.get("DELAYED") ?? 0,
      // 위 다섯 가지 밖의 상태가 오면 뒤에 붙인다.
      extra: [...counts.entries()]
        .filter(([status]) => !["RUNNING", "SUCCESS", "FAILED", "STOPPED", "DELAYED"].includes(status))
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
    // 디자인 작업물 cdc-log.html 필터 띠(날짜 프리셋 · 기간 · 검색 · 파이프라인 · 조회).
    // 카드를 따로 두지 않고 표와 한 카드에 담는다(2026-09-18 목록 화면 공통 규격).
    <div className="std-filter-card">
      <Space.Compact className="std-date-preset">
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
    </div>
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
    <StdPage menu="cdc" title="CDC 로그" icon="database" className="cdc-log-page" fill>
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
            {/* 중지 · 지연은 0 이어도 늘 보인다(2026-10-11 요청) - 예전에는 그 상태 행이 있을 때만 붙어,
                기간을 늘려 조회하면 칸이 생겼다 없어졌다 했다. */}
            {metric("STOPPED", null, STATUS_LABEL.STOPPED, summary.stopped, " etl-log-monitoring-metric--extra")}
            {metric("DELAYED", null, STATUS_LABEL.DELAYED, summary.delayed, " etl-log-monitoring-metric--extra")}
            {summary.extra.map(([status, count]) =>
              metric(status, null, STATUS_LABEL[status] ?? status, count, " etl-log-monitoring-metric--extra", status),
            )}
          </section>
        </div>
      </div>
      <div className="std-card std-table-card">
      {filters}
      <Table<UnifiedCdcLogRow>
        rowKey="key"
        size="middle"
        className="std-full-table"
        loading={(processingQuery.isLoading && !processingQuery.data) || (eventsQuery.isLoading && !eventsQuery.data)}
        dataSource={rows}
        pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (total) => `총 ${total}건` }}
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
                <Tag className="log-status-tag" color={STATUS_COLOR[value] ?? "default"}>{STATUS_LABEL[value] ?? value}</Tag>
              </Tooltip>
            ),
          },
          {
            title: "파이프라인",
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
            // 종료 · 소요는 뺐다(2026-10-10 요청) - 처리 줄은 1분 구간이라 뜻이 없고, 명령의 시작 ~ 종료는 상세 창 «시각»에 있다.
            title: "시각",
            dataIndex: "startedAt",
            width: 180,
            sorter: (a, b) => compareDate(a.startedAt, b.startedAt),
            defaultSortOrder: "descend",
            render: formatDateTime,
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
            // 상세 창에만 있던 값을 열로 꺼냈다(2026-10-10 요청). 명령 줄은 값이 없어 «-».
            title: "일 누적 처리건수",
            dataIndex: "dailyProcessedCount",
            width: 140,
            align: "right",
            sorter: (a, b) => compareNumber(a.dailyProcessedCount, b.dailyProcessedCount),
            render: (value: number | null | undefined) => formatCount(value),
          },
          {
            title: (
              <Tooltip title="Sink 커넥터가 아직 처리하지 않은 메시지 수(추정)입니다. 시간 지연이 아니라 밀린 건수입니다.">
                <span>지연건수</span>
              </Tooltip>
            ),
            dataIndex: "consumerLag",
            width: 110,
            align: "right",
            // 정렬 안내 말풍선(«Click to sort…»)이 위 설명과 겹쳐 뜨지 않게 이 열만 끈다.
            showSorterTooltip: false,
            sorter: (a, b) => compareNumber(a.consumerLag, b.consumerLag),
            render: (value: number | null | undefined) => formatCount(value),
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
          // 줄은 펼치지 않는다 - [+] 를 누르면 상세 «창»이 열린다(2026-09-27 요청).
          // expandedRowKeys 를 늘 비워 두어 antd 가 줄을 펴지 않게 하고, 아이콘만 우리가 그린다.
          expandedRowKeys: [],
          expandedRowRender: () => null,
          expandIcon: ({ record }) =>
            hasDetail(record) ? (
              <button type="button" className="cdc-log-open" aria-label="상세 보기"
                      onClick={(event) => { event.stopPropagation(); setDetailRow(record); }}>
                +
              </button>
            ) : <span className="cdc-log-open is-empty" />,
        }}
      />
      </div>
      <CdcLogDetailModal
        row={detailRow}
        pipelineAlive={Boolean(detailRow) && (pipelinesQuery.data ?? []).some((p) => p.id === detailRow?.pipelineId)}
        onClose={() => setDetailRow(null)}
      />
    </StdPage>
  );
}
