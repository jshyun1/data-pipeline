import { useQuery } from "@tanstack/react-query";
import { Card, Table, Tag } from "antd";
import { listPipelines } from "../api/pipelines";
import type { PipelineResponse } from "../types/pipeline";

const STATUS_COLOR: Record<string, string> = {
  CREATED: "default",
  DEPLOYING: "processing",
  DEPLOYED: "success",
  PAUSED: "warning",
  STOPPED: "default",
  FAILED: "error",
};

export function EtlLogsPage() {
  const { data, isLoading } = useQuery({
    queryKey: ["etl-log-pipelines"],
    queryFn: listPipelines,
    refetchInterval: 60000,
    placeholderData: (previousData) => previousData,
  });
  const logPipelines = (data ?? []).filter((pipeline) => pipeline.pipelineType === "LOG_FILE");
  const showInitialLoading = isLoading && !data;

  return (
    <div>
      <div className="page-toolbar">
        <div>
          <div className="page-kicker">LOG INGESTION</div>
          <h2 className="page-title">로그</h2>
        </div>
      </div>

      <Card title="로그 파이프라인">
        <Table<PipelineResponse>
          rowKey="id"
          loading={showInitialLoading}
          dataSource={logPipelines}
          pagination={false}
          columns={[
            { title: "이름", dataIndex: "name" },
            { title: "Topic", dataIndex: "topicName" },
            {
              title: "타겟",
              render: (_, record) => `${record.targetDbType} · ${record.targetSchema}.${record.targetTable}`,
            },
            {
              title: "상태",
              dataIndex: "status",
              render: (value: string) => <Tag color={STATUS_COLOR[value] ?? "default"}>{value}</Tag>,
            },
            { title: "수정 시각", dataIndex: "updatedAt" },
          ]}
        />
      </Card>
    </div>
  );
}

