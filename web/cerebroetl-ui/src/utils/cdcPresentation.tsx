import type { ReactNode } from "react";
import { Space, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { RealtimePipelineMetricResponse } from "../api/dashboard";
import type { PipelineResponse, PipelineRuntimeStatusResponse } from "../types/pipeline";

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
 * CDC 파이프라인 목록 열. 이름·유형·소스→타깃·Topic·상태·지연·마지막 처리.
 * 관리 화면은 여기에 "관리" 열을 덧붙이고, 실시간 모니터링은 이대로 쓴다.
 *
 * <p>상태 열은 화면에 따라 <b>하나만</b> 둔다.
 * <ul>
 *   <li>CDC 관리 화면: 커넥터 상태(데이터가 실제로 흐르는지)</li>
 *   <li>실시간 모니터링(workflowStatus 를 넘김): 제어 DAG 가 지금 도는지</li>
 * </ul>
 *
 * <p>둘 다 열 이름은 «상태»다 - 화면마다 하나뿐이라 수식어가 필요 없다.
 *
 * <p>예전엔 모니터링에서 둘을 나란히 보여줬는데, 상태 열이 둘이면 어느 쪽을 봐야 하는지
 * 헷갈리고 표만 넓어졌다. 커넥터 상태는 속성창의 상세보기에서 확인한다(2026-09-03 결정).
 */
export function cdcPipelineColumns(
  runtimeByPipeline: Map<number, PipelineRuntimeStatusResponse>,
  metricByPipeline: Map<number, RealtimePipelineMetricResponse>,
  workflowStatus?: { render: (pipeline: PipelineResponse) => ReactNode },
): ColumnsType<PipelineResponse> {
  return [
    { title: "이름", dataIndex: "name", width: 130 },
    {
      title: "유형",
      dataIndex: "pipelineType",
      width: 84,
      render: (value: string) => <Tag color={value === "LOG_FILE" ? "purple" : "blue"}>{value}</Tag>,
    },
    {
      // 여기만 폭을 안 준다 - 남는 자리를 이 열이 흡수해야 표가 가로로 넘치지 않는다.
      title: "소스 → 타깃",
      render: (_, row) => (
        <div>
          <div>{sourceLabel(row)}</div>
          <Typography.Text type="secondary">→ {targetLabel(row)}</Typography.Text>
        </div>
      ),
    },
    { title: "Topic", dataIndex: "topicName", width: 120, ellipsis: true },
    // 상태 열은 하나. 모니터링은 워크플로우 상태를, 관리 화면은 커넥터 상태를 쓴다.
    workflowStatus
      ? {
          title: "상태",
          width: 110,
          render: (_: unknown, row: PipelineResponse) => workflowStatus.render(row),
        }
      : {
          title: "상태",
          width: 150,
          render: (_: unknown, row: PipelineResponse) =>
            renderRuntimeStatus(row, runtimeByPipeline.get(row.id)),
        },
    {
      title: "지연",
      width: 68,
      render: (_, row) => renderLag(metricByPipeline.get(row.id)),
    },
    {
      title: "마지막 처리",
      width: 92,
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
