import { Space, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { RealtimePipelineMetricResponse } from "../api/dashboard";
import type { PipelineResponse, PipelineRuntimeStatusResponse } from "../types/pipeline";
import { byDate, byNumber, byText } from "./listSort";

// CDC 파이프라인을 화면에 그리는 규칙. CDC 관리 화면과 워크플로우 실행 현황이 같은 목록을
// 보여줘야 해서 공용으로 뺐다 - 각자 그리면 "화면마다 상태 표기가 다르다"가 된다.

export const CDC_STATUS_COLOR: Record<string, string> = {
  CREATED: "default",
  DEPLOYING: "processing",
  READY: "blue",
  DEPLOYED: "success",
  PAUSED: "warning",
  STOPPED: "default",
  FAILED: "error",
};

export const CDC_STATUS_LABEL: Record<string, string> = {
  CREATED: "생성됨",
  DEPLOYING: "준비 중",
  READY: "실행 대기",
  DEPLOYED: "실행 중",
  PAUSED: "일시정지",
  STOPPED: "Sink 중지",
  FAILED: "실패",
};

export const RUNTIME_STATUS_LABEL: Record<string, string> = {
  NOT_DEPLOYED: "미배포",
  READY: "실행 대기",
  RUNNING: "실행 중",
  PAUSED: "일시정지",
  STOPPED: "중지",
  FAILED: "실패",
  MISSING: "구성 누락",
  UNKNOWN: "확인 불가",
  DEGRADED: "부분 이상",
};

export const RUNTIME_STATUS_COLOR: Record<string, string> = {
  NOT_DEPLOYED: "default",
  READY: "blue",
  RUNNING: "success",
  PAUSED: "warning",
  STOPPED: "default",
  FAILED: "error",
  MISSING: "error",
  UNKNOWN: "default",
  DEGRADED: "warning",
};

export function formatDuration(seconds: number) {
  if (seconds < 60) return `${seconds}초`;
  if (seconds < 3600) return `${Math.ceil(seconds / 60)}분`;
  if (seconds < 86_400) return `${Math.ceil(seconds / 3600)}시간`;
  return `${Math.ceil(seconds / 86_400)}일`;
}

export function formatRelativeTime(value: string | null) {
  if (!value) return "—";
  const timestamp = new Date(value).getTime();
  if (!Number.isFinite(timestamp)) return "—";
  const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
  if (seconds < 60) return "방금 전";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}분 전`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3600)}시간 전`;
  return `${Math.floor(seconds / 86_400)}일 전`;
}

/** 소스 표기. 로그파일은 테이블이 없으므로 수집기 이름을 쓴다. */
export function sourceLabel(pipeline: PipelineResponse) {
  return pipeline.pipelineType === "LOG_FILE"
    ? "Filebeat"
    : `${pipeline.sourceDbType} · ${pipeline.sourceSchema}.${pipeline.sourceTable}`;
}

export function targetLabel(pipeline: PipelineResponse) {
  return `${pipeline.targetDbType} · ${pipeline.targetSchema}.${pipeline.targetTable}`;
}

/**
 * 실제 커넥터 상태. 저장된 status가 아니라 Kafka Connect에서 읽은 runtime을 우선한다.
 * 둘이 어긋나면 "저장 상태 불일치"를 같이 띄운다 - 화면만 RUNNING이던 사고가 있었다.
 */
export function renderRuntimeStatus(
  pipeline: PipelineResponse,
  runtime: PipelineRuntimeStatusResponse | undefined,
) {
  if (!runtime) {
    return (
      <Tag color={CDC_STATUS_COLOR[pipeline.status] ?? "default"}>
        {CDC_STATUS_LABEL[pipeline.status] ?? pipeline.status}
      </Tag>
    );
  }
  return (
    <div>
      <Tag color={RUNTIME_STATUS_COLOR[runtime.runtimeStatus] ?? "default"}>
        {RUNTIME_STATUS_LABEL[runtime.runtimeStatus] ?? runtime.runtimeStatus}
      </Tag>
      {runtime.statusMismatch && <Tag color="warning">저장 상태 불일치</Tag>}
      <div>
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          Source {runtime.sourceConnectorState ?? "—"} · Sink {runtime.sinkConnectorState ?? "—"}
        </Typography.Text>
      </div>
    </div>
  );
}

export function renderLag(metric: RealtimePipelineMetricResponse | undefined) {
  if (!metric || metric.collectionStatus === "NO_DATA" || metric.consumerLag == null) return "—";
  const hasLag = metric.consumerLag > 0;
  return (
    <div>
      <Typography.Text type={hasLag ? "warning" : undefined} strong={hasLag}>
        {metric.consumerLag.toLocaleString()}
      </Typography.Text>
      {metric.estimatedRecoverySeconds != null && (
        <div>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            약 {formatDuration(metric.estimatedRecoverySeconds)}
          </Typography.Text>
        </div>
      )}
    </div>
  );
}

/**
 * 화면에 보이는 유형 이름(2026-10-13 요청). 저장 값은 그대로 두고 표기만 짧게 한다
 * - «TABLE_CDC» 는 CDC 화면 안에서 CDC 가 자명하고, 로그 적재는 «LOG» 로 짝을 맞춘다.
 */
export const CDC_TYPE_LABEL: Record<string, string> = {
  TABLE_CDC: "TABLE",
  LOG_FILE: "LOG",
};

/**
 * CDC 파이프라인 목록 열. 이름·유형·소스→타깃·Topic·상태·지연·마지막 처리.
 * CDC 관리 화면이 여기에 «소속 워크플로우» · «관리» 열을 덧붙여 쓴다.
 *
 * <p>상태는 커넥터 상태(데이터가 실제로 흐르는지) 하나다. 예전에는 워크플로우 로그 화면의 CDC 가지가
 * 같은 열을 쓰면서 상태 자리에 «워크플로우 실행 상태»를 넣었는데, 그 가지는 2026-10-10 에 뺐다.
 */
export function cdcPipelineColumns(
  runtimeByPipeline: Map<number, PipelineRuntimeStatusResponse>,
  metricByPipeline: Map<number, RealtimePipelineMetricResponse>,
): ColumnsType<PipelineResponse> {
  return [
    { title: "이름", dataIndex: "name", width: 130, sorter: (a, b) => byText(a.name, b.name) },
    {
      title: "유형",
      dataIndex: "pipelineType",
      width: 84,
      sorter: (a, b) => byText(a.pipelineType, b.pipelineType),
      render: (value: string) => (
        <Tag color={value === "LOG_FILE" ? "purple" : "blue"}>{CDC_TYPE_LABEL[value] ?? value}</Tag>
      ),
    },
    {
      // 여기만 폭을 안 준다 - 남는 자리를 이 열이 흡수해야 표가 가로로 넘치지 않는다.
      title: "소스 → 타깃",
      sorter: (a, b) => byText(sourceLabel(a), sourceLabel(b)) || byText(targetLabel(a), targetLabel(b)),
      render: (_, row) => (
        <div>
          <div>{sourceLabel(row)}</div>
          <Typography.Text type="secondary">→ {targetLabel(row)}</Typography.Text>
        </div>
      ),
    },
    { title: "Topic", dataIndex: "topicName", width: 120, ellipsis: true, sorter: (a, b) => byText(a.topicName, b.topicName) },
    {
      title: "상태",
      width: 150,
      sorter: (a: PipelineResponse, b: PipelineResponse) => byText(
        runtimeByPipeline.get(a.id)?.runtimeStatus ?? a.status,
        runtimeByPipeline.get(b.id)?.runtimeStatus ?? b.status,
      ),
      render: (_: unknown, row: PipelineResponse) =>
        renderRuntimeStatus(row, runtimeByPipeline.get(row.id)),
    },
    {
      title: "지연",
      width: 68,
      sorter: (a, b) => byNumber(
        metricByPipeline.get(a.id)?.consumerLag, metricByPipeline.get(b.id)?.consumerLag),
      render: (_, row) => renderLag(metricByPipeline.get(row.id)),
    },
    {
      title: "마지막 처리",
      width: 92,
      sorter: (a, b) => byDate(
        metricByPipeline.get(a.id)?.lastProgressAt, metricByPipeline.get(b.id)?.lastProgressAt),
      render: (_, row) => {
        const lastProgressAt = metricByPipeline.get(row.id)?.lastProgressAt ?? null;
        return <span title={lastProgressAt ?? undefined}>{formatRelativeTime(lastProgressAt)}</span>;
      },
    },
  ];
}

/** 파이프라인 트리 라벨(연결 → 스키마 → 파이프라인)에 붙이는 개수 태그. */
export function countTag(label: string, count: number) {
  return <Space size={4}><span>{label}</span><Tag>{count}</Tag></Space>;
}
