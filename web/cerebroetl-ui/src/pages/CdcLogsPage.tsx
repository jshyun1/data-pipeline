import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, DatePicker, Input, Select, Space, Table, Tabs, Tag, Tooltip, Typography } from "antd";
import { InfoCircleFilled } from "@ant-design/icons";
import dayjs, { type Dayjs } from "dayjs";
import {
  listCdcEventLogs,
  listCdcProcessingLogs,
  type CdcEventLogEntry,
  type CdcProcessingLogEntry,
} from "../api/cdcLogs";

const { RangePicker } = DatePicker;

const STATUS_LABEL: Record<string, string> = {
  SUCCESS: "정상",
  DELAYED: "지연",
  STOPPED: "중지",
  FAILED: "실패",
};

const STATUS_COLOR: Record<string, string> = {
  SUCCESS: "success",
  DELAYED: "warning",
  STOPPED: "default",
  FAILED: "error",
  RUNNING: "success",
  PAUSED: "warning",
  UNASSIGNED: "default",
  UNKNOWN: "default",
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
  return (value ?? 0).toLocaleString("ko-KR");
}

function StateTag({ value }: { value: string | null }) {
  // 로그 파이프라인의 Source처럼 "원래 없는" 항목은 태그 대신 "-"로 둔다.
  // UNKNOWN 태그로 보이면 "상태를 못 읽었다"는 뜻으로 오해된다.
  if (!value) {
    return <span style={{ color: "#999" }}>-</span>;
  }
  return <Tag color={STATUS_COLOR[value] ?? (value === "FAILED" ? "error" : "default")}>{value}</Tag>;
}

function ConnectorStateTitle({ label, description }: { label: string; description: string }) {
  return (
    <Tooltip title={description}>
      <span
        tabIndex={0}
        aria-label={`${label} 컬럼 설명`}
        style={{ display: "inline-flex", alignItems: "center", gap: 4, cursor: "help" }}
      >
        {label}
        <InfoCircleFilled style={{ color: "#1677ff", fontSize: 13 }} />
      </span>
    </Tooltip>
  );
}

export function CdcLogsPage() {
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(6, "day"), dayjs()]);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(dateRange);
  const [keyword, setKeyword] = useState("");
  const [activeTab, setActiveTab] = useState("processing");
  const [processingStatuses, setProcessingStatuses] = useState<string[]>([]);
  const [eventResults, setEventResults] = useState<string[]>([]);

  const from = appliedRange[0].format("YYYY-MM-DD");
  const to = appliedRange[1].format("YYYY-MM-DD");
  const processingQuery = useQuery({
    queryKey: ["cdc-processing-logs", from, to],
    queryFn: () => listCdcProcessingLogs(from, to),
    placeholderData: (previousData) => previousData,
  });
  const eventsQuery = useQuery({
    queryKey: ["cdc-event-logs", from, to],
    queryFn: () => listCdcEventLogs(from, to),
    placeholderData: (previousData) => previousData,
  });

  const normalizedKeyword = keyword.trim().toLowerCase();
  const processingRows = useMemo(
    () =>
      (processingQuery.data ?? []).filter((row) => {
        const matchesKeyword =
          !normalizedKeyword ||
          row.pipelineName.toLowerCase().includes(normalizedKeyword) ||
          row.source.toLowerCase().includes(normalizedKeyword) ||
          row.target.toLowerCase().includes(normalizedKeyword) ||
          (row.topicName ?? "").toLowerCase().includes(normalizedKeyword);
        return matchesKeyword && (processingStatuses.length === 0 || processingStatuses.includes(row.status));
      }),
    [normalizedKeyword, processingQuery.data, processingStatuses],
  );
  const eventRows = useMemo(
    () =>
      (eventsQuery.data ?? []).filter((row) => {
        const matchesKeyword =
          !normalizedKeyword ||
          row.pipelineName.toLowerCase().includes(normalizedKeyword) ||
          (row.message ?? "").toLowerCase().includes(normalizedKeyword);
        return matchesKeyword && (eventResults.length === 0 || eventResults.includes(row.result ?? ""));
      }),
    [eventResults, eventsQuery.data, normalizedKeyword],
  );

  const filters = (
    <Space style={{ marginBottom: 16 }} wrap>
      <RangePicker
        value={dateRange}
        onChange={(value) => {
          if (value?.[0] && value[1]) {
            setDateRange([value[0], value[1]]);
          }
        }}
        allowClear={false}
      />
      <Input
        placeholder="파이프라인/Topic 검색"
        allowClear
        style={{ width: 220 }}
        value={keyword}
        onChange={(event) => setKeyword(event.target.value)}
      />
      <Select
        mode="multiple"
        allowClear
        placeholder="상태"
        style={{ minWidth: 160 }}
        value={activeTab === "processing" ? processingStatuses : eventResults}
        onChange={activeTab === "processing" ? setProcessingStatuses : setEventResults}
        options={
          activeTab === "processing"
            ? [
                { value: "SUCCESS", label: "정상" },
                { value: "DELAYED", label: "지연" },
                { value: "STOPPED", label: "중지" },
                { value: "FAILED", label: "실패" },
              ]
            : [
                { value: "SUCCESS", label: "성공" },
                { value: "FAILED", label: "실패" },
              ]
        }
      />
      <Button type="primary" onClick={() => setAppliedRange(dateRange)}>
        조회
      </Button>
      <Tooltip
        placement="right"
        title={
          <>
            <div>처리 건수는 Kafka Sink의 committed offset 증가량을 기준으로 한 추정치입니다.</div>
            <div style={{ marginTop: 6 }}>
              일 누적 처리는 파이프라인별로 한국 시간 자정부터 해당 시각까지의 처리 건수를 합산합니다.
            </div>
            <div style={{ marginTop: 6 }}>
              원본 데이터의 컬럼값은 표시하지 않으며, 중지 상태에서는 Source가 계속 수집하므로 Lag 증가가
              정상적인 적재 대기일 수 있습니다.
            </div>
          </>
        }
      >
        <InfoCircleFilled
          aria-label="CDC 처리 건수 안내"
          tabIndex={0}
          style={{ color: "#1677ff", cursor: "help", fontSize: 18 }}
        />
      </Tooltip>
    </Space>
  );

  return (
    <div>
      <Card title="CDC 처리 로그">
        {filters}
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          items={[
            {
              key: "processing",
              label: "처리 이력",
              children: (
                <>
                  <Typography.Text type="secondary">
                    변화 없는 정상 heartbeat는 제외하고 처리량, Lag 또는 상태 변화가 있는 1분 구간을 표시합니다.
                  </Typography.Text>
                  <Table<CdcProcessingLogEntry>
                    rowKey={(row) => `${row.pipelineId}-${row.occurredAt}`}
                    style={{ marginTop: 12 }}
                    size="small"
                    loading={processingQuery.isLoading && !processingQuery.data}
                    dataSource={processingRows}
                    pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
                    scroll={{ x: 1540 }}
                    columns={[
                      { title: "발생 시각", dataIndex: "occurredAt", width: 180, render: formatDateTime },
                      { title: "파이프라인", dataIndex: "pipelineName", width: 160, fixed: "left" },
                      {
                        title: "소스 → 타깃",
                        width: 330,
                        render: (_, row) => (
                          <span>
                            {row.source}
                            <br />
                            <Typography.Text type="secondary">→ {row.target}</Typography.Text>
                          </span>
                        ),
                      },
                      { title: "Topic", dataIndex: "topicName", width: 210 },
                      {
                        title: "상태",
                        dataIndex: "status",
                        width: 90,
                        render: (value: string) => (
                          <Tag color={STATUS_COLOR[value] ?? "default"}>{STATUS_LABEL[value] ?? value}</Tag>
                        ),
                      },
                      {
                        title: (
                          <ConnectorStateTitle
                            label="Source"
                            description="원본 시스템의 변경 데이터를 읽어 중간 전송 채널로 보내는 Source 커넥터의 현재 상태입니다. 로그 파이프라인은 소스가 Filebeat(외부 에이전트)라 Source 커넥터가 없어 -로 표시됩니다."
                          />
                        ),
                        dataIndex: "sourceState",
                        width: 110,
                        render: (value: string | null) => <StateTag value={value} />,
                      },
                      {
                        title: (
                          <ConnectorStateTitle
                            label="Sink"
                            description="중간 전송 채널의 데이터를 읽어 대상 시스템에 반영하는 Sink 커넥터의 현재 상태입니다."
                          />
                        ),
                        dataIndex: "sinkState",
                        width: 110,
                        render: (value: string) => <StateTag value={value} />,
                      },
                      { title: "처리 건수", dataIndex: "processedCount", width: 110, align: "right", render: formatCount },
                      {
                        title: "일 누적 처리",
                        dataIndex: "dailyProcessedCount",
                        width: 120,
                        align: "right",
                        render: formatCount,
                      },
                      {
                        title: "Lag",
                        dataIndex: "consumerLag",
                        width: 100,
                        align: "right",
                        render: (value: number) =>
                          value > 0 ? <Tag color={value >= 1000 ? "warning" : "processing"}>{formatCount(value)}</Tag> : "0",
                      },
                      { title: "메시지", dataIndex: "message", width: 260, render: (value?: string | null) => value ?? "-" },
                    ]}
                  />
                </>
              ),
            },
            {
              key: "events",
              label: "오류·상태 이력",
              children: (
                <Table<CdcEventLogEntry>
                  rowKey="id"
                  size="small"
                  loading={eventsQuery.isLoading && !eventsQuery.data}
                  dataSource={eventRows}
                  pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
                  columns={[
                    { title: "발생 시각", dataIndex: "occurredAt", width: 190, render: formatDateTime },
                    { title: "파이프라인", dataIndex: "pipelineName" },
                    {
                      title: "이벤트",
                      dataIndex: "command",
                      width: 140,
                      render: (value: string) => COMMAND_LABEL[value] ?? value,
                    },
                    {
                      title: "결과",
                      dataIndex: "result",
                      width: 100,
                      render: (value?: string | null) =>
                        value ? <Tag color={STATUS_COLOR[value] ?? "default"}>{STATUS_LABEL[value] ?? value}</Tag> : "-",
                    },
                    { title: "요청자", dataIndex: "requestedBy", width: 120, render: (value?: string | null) => value ?? "시스템" },
                    { title: "메시지", dataIndex: "message", render: (value?: string | null) => value ?? "-" },
                  ]}
                />
              ),
            },
          ]}
        />
      </Card>
    </div>
  );
}
