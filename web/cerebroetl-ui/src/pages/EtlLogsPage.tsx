import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, DatePicker, Input, Select, Space, Table, Tag } from "antd";
import dayjs, { type Dayjs } from "dayjs";
import {
  getNifiRootStatus,
  listAirflowDagRuns,
  listAirflowDags,
  listAirflowTaskInstances,
} from "../api/platform";
import { collectNifiJobs } from "../utils/nifiJobs";

const { RangePicker } = DatePicker;

type EtlLogState = "all" | "success" | "failed" | "running" | "queued";

const STATE_LABEL: Record<string, string> = {
  success: "성공",
  failed: "실패",
  running: "실행중",
  queued: "대기",
};

const STATE_COLOR: Record<string, string> = {
  success: "success",
  failed: "error",
  running: "processing",
  queued: "default",
};

const ACTION_LABEL: Record<string, string> = {
  start: "시작",
  stop: "중지",
};

interface EtlLogRow {
  id: string;
  dagId: string;
  pipelineName: string;
  action: string;
  state: string;
  startedAt?: string;
  endedAt?: string;
  failedTask: string;
}

// nifi_pipelines_dynamic.py가 프로세스 그룹별로 만드는 제어 DAG는 항상
// "nifi_pipeline_{그룹 id 앞 8자리}_control" 형태다 - 여기서 이름을 되짚어
// NiFi 프로세스 그룹의 실제 이름과 매칭한다.
function extractNifiShortId(dagId: string): string {
  return dagId.replace(/^nifi_pipeline_/, "").replace(/_control$/, "");
}

function formatDateTime(value?: string) {
  if (!value) {
    return "-";
  }
  return new Intl.DateTimeFormat("ko-KR", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function formatDuration(start?: string, end?: string) {
  if (!start) {
    return "-";
  }
  const startTime = new Date(start).getTime();
  const endTime = end ? new Date(end).getTime() : Date.now();
  const minutes = Math.max(1, Math.round((endTime - startTime) / 60000));
  return `${minutes}분`;
}

// ETL 메뉴는 NiFi 전용이다(생성/관리 모두 NiFi 프로세스 그룹을 다룬다) - 로그도
// nifi_pipeline_*_control DAG의 실행 이력만 대상으로 한다. Kafka 쪽
// kafka_pipeline_*_control DAG나 1분마다 도는 nifi_pipelines_metrics_collector(집계용,
// 실행마다 노이즈만 큼)는 의도적으로 제외한다.
async function getEtlLogs(range: [Dayjs, Dayjs], state: EtlLogState): Promise<EtlLogRow[]> {
  const [dags, nifiStatus] = await Promise.all([listAirflowDags(), getNifiRootStatus().catch(() => undefined)]);
  const nifiControlDags = dags.filter(
    (dag) => dag.dag_id.startsWith("nifi_pipeline_") && dag.dag_id.endsWith("_control"),
  );
  const jobs = collectNifiJobs(nifiStatus?.processGroupStatus?.aggregateSnapshot?.processGroupStatusSnapshots);

  const runGroupResults = await Promise.allSettled(
    nifiControlDags.map(async (dag) => ({
      dagId: dag.dag_id,
      runs: await listAirflowDagRuns(dag.dag_id, {
        limit: 200,
        startDateGte: range[0].startOf("day").toISOString(),
        startDateLte: range[1].endOf("day").toISOString(),
        state: state === "all" ? undefined : state,
      }),
    })),
  );
  const runGroups = runGroupResults.flatMap((result) => (result.status === "fulfilled" ? [result.value] : []));

  const failedTaskResults = await Promise.allSettled(
    runGroups.flatMap((group) =>
      group.runs
        .filter((run) => run.state === "failed")
        .map(async (run) => {
          const tasks = await listAirflowTaskInstances(group.dagId, run.dag_run_id).catch(() => []);
          const failedTask = tasks.find((task) => task.state === "failed");
          return { runKey: `${group.dagId}:${run.dag_run_id}`, failedTask: failedTask?.task_id };
        }),
    ),
  );
  const failedTaskMap = new Map(
    failedTaskResults
      .filter((result) => result.status === "fulfilled")
      .map((result) => [result.value.runKey, result.value.failedTask]),
  );

  const rows: EtlLogRow[] = runGroups.flatMap((group) => {
    const shortId = extractNifiShortId(group.dagId);
    const job = jobs.find((candidate) => candidate.id.startsWith(shortId));
    return group.runs.map((run) => ({
      id: `${group.dagId}:${run.dag_run_id}`,
      dagId: group.dagId,
      pipelineName: job?.name ?? shortId,
      action: typeof run.conf?.action === "string" ? (run.conf.action as string) : "-",
      state: run.state ?? "-",
      startedAt: run.start_date ?? run.execution_date,
      endedAt: run.end_date,
      failedTask: failedTaskMap.get(`${group.dagId}:${run.dag_run_id}`) ?? "-",
    }));
  });

  return rows.sort((a, b) => new Date(b.startedAt ?? 0).getTime() - new Date(a.startedAt ?? 0).getTime());
}

export function EtlLogsPage() {
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(6, "day"), dayjs()]);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(dateRange);
  const [stateFilter, setStateFilter] = useState<EtlLogState>("all");
  const [nameFilter, setNameFilter] = useState("");

  const logsQuery = useQuery({
    queryKey: ["etl-logs", appliedRange[0].toISOString(), appliedRange[1].toISOString(), stateFilter],
    queryFn: () => getEtlLogs(appliedRange, stateFilter),
    placeholderData: (previousData) => previousData,
  });

  const rows = logsQuery.data ?? [];
  const filteredRows = useMemo(() => {
    const keyword = nameFilter.trim().toLowerCase();
    if (!keyword) {
      return rows;
    }
    return rows.filter((row) => row.pipelineName.toLowerCase().includes(keyword));
  }, [rows, nameFilter]);

  const showInitialLoading = logsQuery.isLoading && !logsQuery.data;

  return (
    <div>
      <Card title="NiFi ETL 로그">
        <Space style={{ marginBottom: 16 }} wrap>
          <RangePicker
            value={dateRange}
            onChange={(value) => {
              if (value && value[0] && value[1]) {
                setDateRange([value[0], value[1]]);
              }
            }}
            allowClear={false}
          />
          <Select<EtlLogState>
            value={stateFilter}
            style={{ width: 120 }}
            onChange={setStateFilter}
            options={[
              { value: "all", label: "전체 상태" },
              { value: "success", label: "성공" },
              { value: "failed", label: "실패" },
              { value: "running", label: "실행중" },
              { value: "queued", label: "대기" },
            ]}
          />
          <Input
            placeholder="이름 검색"
            allowClear
            style={{ width: 200 }}
            value={nameFilter}
            onChange={(event) => setNameFilter(event.target.value)}
          />
          <Button type="primary" onClick={() => setAppliedRange(dateRange)}>
            조회
          </Button>
        </Space>
        <Table<EtlLogRow>
          rowKey="id"
          size="small"
          loading={showInitialLoading}
          dataSource={filteredRows}
          pagination={{ pageSize: 20 }}
          columns={[
            { title: "이름", dataIndex: "pipelineName" },
            {
              title: "액션",
              dataIndex: "action",
              width: 100,
              render: (value: string) => ACTION_LABEL[value] ?? value,
            },
            {
              title: "상태",
              dataIndex: "state",
              width: 100,
              render: (value: string) => <Tag color={STATE_COLOR[value] ?? "default"}>{STATE_LABEL[value] ?? value}</Tag>,
            },
            { title: "실행 시각", dataIndex: "startedAt", width: 170, render: formatDateTime },
            {
              title: "소요 시간",
              width: 100,
              render: (_, record) => formatDuration(record.startedAt, record.endedAt),
            },
            { title: "실패 Task", dataIndex: "failedTask", width: 160 },
          ]}
        />
      </Card>
    </div>
  );
}
