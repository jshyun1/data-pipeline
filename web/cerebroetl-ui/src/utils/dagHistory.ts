import type { NifiJob } from "./nifiJobs";
import type { PipelineResponse } from "../types/pipeline";

export type DagCategory = "ETL" | "CDC" | "기타";

// nifi_pipeline_*_control은 NiFi 프로세스 그룹 하나당 하나씩 생성되는 시작/중지 DAG(ETL),
// kafka_pipeline_*_control은 Kafka 파이프라인 하나당 하나씩 생성되는 동일한 역할의 DAG(CDC).
// 그 외(test_hello, mail_to_oracle_pipeline, nifi_pipelines_metrics_collector 등)는 기타로 묶는다.
export function categorizeDag(dagId: string): DagCategory {
  // etl_wf_*는 워크플로우 캔버스가 게시해 만든 DAG다(워크플로우 1개 = DAG 1개).
  // 이걸 인식하지 못하면 "기타"로 밀려 실행 현황 ETL 탭에 나타나지 않는다.
  if (dagId.startsWith("etl_wf_")) {
    return "ETL";
  }
  if (dagId.startsWith("nifi_pipeline_") && dagId.endsWith("_control")) {
    return "ETL";
  }
  if (dagId.startsWith("kafka_pipeline_") && dagId.endsWith("_control")) {
    return "CDC";
  }
  return "기타";
}

export function extractNifiShortId(dagId: string): string {
  return dagId.replace(/^nifi_pipeline_/, "").replace(/_control$/, "");
}

export function extractKafkaPipelineId(dagId: string): string {
  return dagId.replace(/^kafka_pipeline_/, "").replace(/_control$/, "");
}

export function resolveDagDisplayName(
  dagId: string,
  category: DagCategory,
  nifiJobs: NifiJob[],
  kafkaPipelines: PipelineResponse[],
): string {
  if (category === "ETL") {
    const shortId = extractNifiShortId(dagId);
    return nifiJobs.find((job) => job.id.startsWith(shortId))?.name ?? dagId;
  }
  if (category === "CDC") {
    const idStr = extractKafkaPipelineId(dagId);
    return kafkaPipelines.find((pipeline) => String(pipeline.id) === idStr)?.name ?? dagId;
  }
  return dagId;
}

// 1분마다 도는 집계 전용 DAG - 알려진 Airflow executor 버그로 스케줄 실행이 항상
// failed로 찍히는 노이즈라, 실행 이력(성공/실패/지연) 집계에서는 제외한다.
export const NIFI_METRICS_COLLECTOR_DAG_ID = "nifi_pipelines_metrics_collector";
