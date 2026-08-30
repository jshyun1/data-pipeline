import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, DatePicker, Input, Select, Space, Table, Tabs, Tag, Tooltip, Typography } from "antd";
import { QuestionCircleOutlined } from "@ant-design/icons";
import dayjs, { type Dayjs } from "dayjs";
import { activeDayPresetByDate } from "../components/dayPreset";
import {
  listNifiExecutionLogs,
  listNifiProcessorRuns,
  type NifiExecutionLogEntry,
  type NifiProcessorRun,
} from "../api/platform";

const { RangePicker } = DatePicker;

const RUN_STATUS_LABEL: Record<string, string> = {
  RUNNING: "진행 중",
  SUCCESS: "완료",
};

const RUN_STATUS_COLOR: Record<string, string> = {
  RUNNING: "processing",
  SUCCESS: "success",
};

const EVENT_STATUS_LABEL: Record<string, string> = {
  SUCCESS: "정상",
  FAILED: "실패",
};

const EVENT_STATUS_COLOR: Record<string, string> = {
  SUCCESS: "success",
  FAILED: "error",
};

const LEVEL_LABEL: Record<string, string> = {
  ERROR: "오류",
  WARNING: "경고",
};

const LEVEL_COLOR: Record<string, string> = {
  ERROR: "error",
  WARNING: "warning",
};

interface NifiProcessorRunGroup {
  groupKey: string;
  groupName: string;
  latestRun: NifiProcessorRun;
  runs: NifiProcessorRun[];
  runCount: number;
  totalInserted: number;
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
// 테이블이 섞여서 프로세서별 구분이 안 됨). 그래서 두 갈래로 만든다:
//  - 처리 이력: 백엔드가 15초마다 활성 스레드와 적재 카운터를 관측해 "실행 구간"을 직접
//    만든다(nifi_processor_run). 시작/종료는 관측값이라 최대 15초 오차가 있다.
//  - 오류·상태 이력: NiFi bulletin(경고/에러)을 30초마다 긁어서 남긴다. 카운터는 "늘었을
//    때"만 보이므로 실패를 표현할 방법이 원래 없었고, 그래서 DB 인증 실패나 OOM처럼 실제로
//    파이프라인이 죽은 날에도 이 화면엔 아무것도 안 남았다.
export function EtlLogsPage() {
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs(), dayjs()]);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(dateRange);
  const [keyword, setKeyword] = useState("");
  const [groupFilter, setGroupFilter] = useState<string[]>([]);
  const [activeTab, setActiveTab] = useState("runs");

  const from = appliedRange[0].format("YYYY-MM-DD");
  const to = appliedRange[1].format("YYYY-MM-DD");

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

  // 그룹 필터 후보는 두 탭의 데이터를 합쳐서 뽑는다 - 실패만 있고 적재는 없는
  // 그룹도 골라볼 수 있어야 한다.
  const groupOptions = useMemo(() => {
    const names = new Set<string>();
    for (const row of runsQuery.data ?? []) {
      if (row.groupName) names.add(row.groupName);
    }
    for (const row of eventsQuery.data ?? []) {
      const name = eventGroupName(row);
      if (name !== "-") names.add(name);
    }
    return [...names].sort().map((name) => ({ value: name, label: name }));
  }, [runsQuery.data, eventsQuery.data]);

  const runRows = useMemo(() => {
    const needle = keyword.trim().toLowerCase();
    return (runsQuery.data ?? []).filter((row) => {
      if (groupFilter.length > 0 && !groupFilter.includes(row.groupName ?? "")) {
        return false;
      }
      if (!needle) {
        return true;
      }
      return [row.processorName, row.groupName, row.targetTable, row.processorType].some((text) =>
        (text ?? "").toLowerCase().includes(needle),
      );
    });
  }, [runsQuery.data, keyword, groupFilter]);

  const runGroupRows = useMemo<NifiProcessorRunGroup[]>(() => {
    const groups = new Map<string, NifiProcessorRun[]>();
    for (const row of runRows) {
      const groupName = runGroupName(row);
      const groupKey = row.groupId ?? groupName;
      groups.set(groupKey, [...(groups.get(groupKey) ?? []), row]);
    }

    return [...groups.entries()]
      .map(([groupKey, rows]) => {
        const runs = [...rows].sort(
          (a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime(),
        );
        return {
          groupKey,
          groupName: runGroupName(runs[0]),
          latestRun: runs[0],
          runs,
          runCount: runs.length,
          totalInserted: runs.reduce((sum, row) => sum + row.insertedCount, 0),
        };
      })
      .sort((a, b) => new Date(b.latestRun.startedAt).getTime() - new Date(a.latestRun.startedAt).getTime());
  }, [runRows]);

  // 오류·상태 이력은 bulletin 기반 행만 보여준다. 카운터 기반 SUCCESS 행은 처리 이력
  // 탭이 구간으로 대신 보여주므로, 여기 섞이면 같은 적재가 두 탭에 중복으로 나온다.
  const eventRows = useMemo(() => {
    const needle = keyword.trim().toLowerCase();
    return (eventsQuery.data ?? [])
      .filter((row) => row.status !== "SUCCESS")
      .filter((row) => {
        const displayGroupName = eventGroupName(row);
        if (groupFilter.length > 0 && !groupFilter.includes(displayGroupName)) {
          return false;
        }
        if (!needle) {
          return true;
        }
        return [row.processorName, displayGroupName, row.jobName, row.status, row.level, row.message].some((text) =>
          (text ?? "").toLowerCase().includes(needle),
        );
      });
  }, [eventsQuery.data, keyword, groupFilter]);

  const errorCount = eventRows.filter((row) => row.status === "FAILED" && row.level !== "WARNING").length;

  const applyDatePreset = (offsetDays: 0 | 1) => {
    const target = dayjs().subtract(offsetDays, "day");
    const range: [Dayjs, Dayjs] = [target, target];
    setDateRange(range);
    setAppliedRange(range);
  };
  // 이 화면의 RangePicker 는 날짜 단위라 시각까지 비교하는 판정으로는 일치하지 않는다.
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
        placeholder="프로세서/그룹/테이블 검색"
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
      {errorCount > 0 && <Tag color="error">오류 {errorCount}건</Tag>}
      <Tooltip
        title={
          "NiFi에는 실행 이력 개념이 없어(Provenance 조회 불가, 프로세서 단위 Status History 빈 응답) " +
          "백엔드가 15초마다 프로세서의 활성 스레드와 적재 카운터를 관측해 실행 구간을 만듭니다. " +
          "따라서 시작/종료 시각은 최대 15초 오차가 있는 추정값이고, 15초 안에 끝난 적재는 " +
          "소요 0초로 기록되어 처리량을 계산할 수 없습니다."
        }
      >
        <QuestionCircleOutlined
          role="button"
          tabIndex={0}
          style={{ color: "#1677ff", cursor: "help", fontSize: 18 }}
        />
      </Tooltip>
    </Space>
  );

  return (
    <div>
      <Card title="NiFi ETL 로그">
        {filters}
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          items={[
            {
              key: "runs",
              label: "처리 이력",
              children: (
                <>
                  <Typography.Text type="secondary">
                    적재 프로세서가 연속으로 일한 구간을 하나의 실행으로 묶어 표시합니다. 처리량은 적재 건수를
                    소요시간으로 나눈 값입니다.
                  </Typography.Text>
                  <Table<NifiProcessorRunGroup>
                    rowKey="groupKey"
                    style={{ marginTop: 12 }}
                    size="small"
                    loading={runsQuery.isLoading && !runsQuery.data}
                    dataSource={runGroupRows}
                    pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
                    scroll={{ x: 1330 }}
                    columns={[
                      {
                        title: "최근 시작 시각",
                        dataIndex: ["latestRun", "startedAt"],
                        width: 180,
                        fixed: "left",
                        render: formatDateTime,
                      },
                      {
                        title: "최근 종료 시각",
                        dataIndex: ["latestRun", "endedAt"],
                        width: 180,
                        render: formatDateTime,
                      },
                      {
                        title: "그룹(파이프라인)",
                        dataIndex: "groupName",
                        width: 180,
                      },
                      {
                        title: "최근 프로세서",
                        dataIndex: ["latestRun", "processorName"],
                        width: 190,
                      },
                      {
                        title: "최근 유형",
                        dataIndex: ["latestRun", "processorType"],
                        width: 165,
                        render: (value?: string | null) => value ?? "-",
                      },
                      {
                        title: "최근 적재 대상",
                        dataIndex: ["latestRun", "targetTable"],
                        width: 190,
                        // 스크립트 안에 테이블명이 박혀 있는 적재(ExecuteGroovyScript)는
                        // 프로세서 설정에서 읽을 수 없어 비어 있다.
                        render: (value?: string | null) => value ?? "-",
                      },
                      {
                        title: "최근 적재 건수",
                        dataIndex: ["latestRun", "insertedCount"],
                        width: 120,
                        align: "right",
                        render: formatCount,
                      },
                      {
                        title: "총 적재 건수",
                        dataIndex: "totalInserted",
                        width: 120,
                        align: "right",
                        render: formatCount,
                      },
                      {
                        title: "이력 수",
                        dataIndex: "runCount",
                        width: 90,
                        align: "right",
                        render: (value: number) => `${value.toLocaleString()}건`,
                      },
                      {
                        title: "최근 상태",
                        dataIndex: ["latestRun", "status"],
                        width: 100,
                        render: (value: string) => (
                          <Tag color={RUN_STATUS_COLOR[value] ?? "default"}>{RUN_STATUS_LABEL[value] ?? value}</Tag>
                        ),
                      },
                    ]}
                    expandable={{
                      expandedRowRender: (group) => (
                        <Table<NifiProcessorRun>
                          rowKey="id"
                          size="small"
                          dataSource={group.runs}
                          pagination={false}
                          scroll={{ x: 1470 }}
                          columns={[
                            { title: "시작 시각", dataIndex: "startedAt", width: 180, render: formatDateTime },
                            { title: "종료 시각", dataIndex: "endedAt", width: 180, render: formatDateTime },
                            {
                              title: "소요",
                              dataIndex: "durationSeconds",
                              width: 110,
                              align: "right",
                              render: formatDuration,
                            },
                            { title: "프로세서", dataIndex: "processorName", width: 190 },
                            {
                              title: "유형",
                              dataIndex: "processorType",
                              width: 165,
                              render: (value?: string | null) => value ?? "-",
                            },
                            {
                              title: "적재 대상",
                              dataIndex: "targetTable",
                              width: 190,
                              render: (value?: string | null) => value ?? "-",
                            },
                            {
                              title: "적재 건수",
                              dataIndex: "insertedCount",
                              width: 120,
                              align: "right",
                              render: formatCount,
                            },
                            {
                              title: "처리량",
                              dataIndex: "rowsPerSecond",
                              width: 125,
                              align: "right",
                              render: (value?: number | null) =>
                                value == null ? (
                                  <Tooltip title="한 관측 주기(15초) 안에 끝나 소요시간을 잴 수 없습니다.">
                                    <span style={{ color: "#999" }}>-</span>
                                  </Tooltip>
                                ) : (
                                  `${value.toLocaleString()} 행/초`
                                ),
                            },
                            {
                              title: "상태",
                              dataIndex: "status",
                              width: 100,
                              render: (value: string) => (
                                <Tag color={RUN_STATUS_COLOR[value] ?? "default"}>
                                  {RUN_STATUS_LABEL[value] ?? value}
                                </Tag>
                              ),
                            },
                          ]}
                        />
                      ),
                      rowExpandable: (group) => group.runs.length > 0,
                    }}
                  />
                </>
              ),
            },
            {
              key: "events",
              label: "오류·상태 이력",
              children: (
                <>
                  <Typography.Text type="secondary">
                    NiFi가 올린 경고/오류입니다. NiFi는 이 알림을 5분만 메모리에 두므로 30초마다 수집해 영속화한
                    것입니다.
                  </Typography.Text>
                  <Table<NifiExecutionLogEntry>
                    rowKey="id"
                    style={{ marginTop: 12 }}
                    size="small"
                    loading={eventsQuery.isLoading && !eventsQuery.data}
                    dataSource={eventRows}
                    pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
                    scroll={{ x: 1010 }}
                    columns={[
                      { title: "발생 시각", dataIndex: "occurredAt", width: 180, fixed: "left", render: formatDateTime },
                      { title: "프로세서", dataIndex: "processorName", width: 200 },
                      {
                        title: "그룹(파이프라인)",
                        dataIndex: "groupName",
                        width: 150,
                        render: (_value: string | null | undefined, row) => eventGroupName(row),
                      },
                      {
                        title: "상태",
                        dataIndex: "status",
                        width: 95,
                        render: (value: string) => (
                          <Tag color={EVENT_STATUS_COLOR[value] ?? "default"}>{EVENT_STATUS_LABEL[value] ?? value}</Tag>
                        ),
                      },
                      {
                        title: "수준",
                        dataIndex: "level",
                        width: 90,
                        render: (value?: string | null) =>
                          value ? <Tag color={LEVEL_COLOR[value] ?? "default"}>{LEVEL_LABEL[value] ?? value}</Tag> : "-",
                      },
                      {
                        title: "메시지",
                        dataIndex: "message",
                        ellipsis: true,
                        render: (value?: string | null) => value ?? "-",
                      },
                    ]}
                    // 실패 원인은 스택트레이스까지 길어서 한 줄에 다 못 보여준다.
                    expandable={{
                      expandedRowRender: (row) => (
                        <pre style={{ whiteSpace: "pre-wrap", margin: 0, fontSize: 12 }}>
                          {row.message ?? "상세 메시지 없음"}
                        </pre>
                      ),
                      rowExpandable: (row) => Boolean(row.message),
                    }}
                  />
                </>
              ),
            },
          ]}
        />
      </Card>
    </div>
  );
}
