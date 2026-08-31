import { useMemo, useState } from "react";
import { useAuth } from "../auth/AuthContext";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Alert,
  Button,
  Card,
  Collapse,
  Descriptions,
  Drawer,
  Input,
  message,
  Popconfirm,
  Segmented,
  Select,
  Space,
  Spin,
  Table,
  Tabs,
  Tag,
  Tree,
  Typography,
} from "antd";
import { PlusOutlined } from "@ant-design/icons";
import type { TreeProps } from "antd";
import { useNavigate } from "react-router-dom";
import { listConnections } from "../api/connections";
import {
  getPipelineDashboardSummary,
  getRealtimePipelineMetrics,
} from "../api/dashboard";
import {
  deletePipeline,
  checkPipelineConsistency,
  dismissConnectorDrift,
  getPipelineHistory,
  listPipelineRuntimeStatuses,
  listPipelineConsistencyChecks,
  listPipelines,
} from "../api/pipelines";
import type {
  PipelineCommandHistoryResponse,
  PipelineResponse,
} from "../types/pipeline";
import {
  CDC_STATUS_LABEL,
  RUNTIME_STATUS_COLOR,
  RUNTIME_STATUS_LABEL,
  cdcPipelineColumns,
  renderRuntimeStatus,
} from "../utils/cdcPresentation";

const STATUS_OPTIONS = ["CREATED", "DEPLOYING", "READY", "DEPLOYED", "PAUSED", "STOPPED", "FAILED"].map((value) => ({
  value,
  label: CDC_STATUS_LABEL[value],
}));

const TYPE_OPTIONS = [
  { value: "TABLE_CDC", label: "TABLE_CDC" },
  { value: "LOG_FILE", label: "LOG_FILE" },
];

const STATUS_SEVERITY: Record<string, number> = {
  FAILED: 0,
  DEPLOYING: 1,
  STOPPED: 2,
  PAUSED: 3,
  CREATED: 4,
  READY: 5,
  DEPLOYED: 6,
};

export function PipelinesPage() {
  const navigate = useNavigate();
  // CDC 쓰기 권한이 없으면 생성·삭제를 막는다. 권한이 아직 안 실렸을 때는 사이드바와 같은
  // 규칙으로 fail-open 한다(잠깐 비활성으로 깜빡이지 않게). 실제 차단은 백엔드가 한다.
  const { can, permissionsLoaded } = useAuth();
  const canWrite = !permissionsLoaded || can("KAFKA", "WRITE");
  const queryClient = useQueryClient();
  const [detailPipelineId, setDetailPipelineId] = useState<number | null>(null);
  const [nameFilter, setNameFilter] = useState("");
  const [topicFilter, setTopicFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState<string[]>([]);
  const [typeFilter, setTypeFilter] = useState<string[]>([]);
  const [treeSearch, setTreeSearch] = useState("");
  const [selectedTreeKey, setSelectedTreeKey] = useState<string>("all");
  const [quickFilter, setQuickFilter] = useState("ALL");

  const { data: pipelines, isLoading } = useQuery({
    queryKey: ["pipelines"],
    queryFn: listPipelines,
  });
  const { data: runtimeStatuses } = useQuery({
    queryKey: ["pipeline-runtime-statuses"],
    queryFn: listPipelineRuntimeStatuses,
    refetchInterval: 10_000,
  });
  const runtimeByPipeline = useMemo(
    () => new Map((runtimeStatuses ?? []).map((runtime) => [runtime.pipelineId, runtime])),
    [runtimeStatuses],
  );
  const { data: connections } = useQuery({
    queryKey: ["connections"],
    queryFn: listConnections,
  });

  // metadata-db는 RUNNING이라고 알고 있는데 실제 Kafka Connect엔 없는 커넥터가 있는지
  // 주기적으로 확인한다 - 예전에 이걸 놓쳐서 데이터가 조용히 안 들어온 적이 있었다.
  const { data: dashboardSummary } = useQuery({
    queryKey: ["pipeline-dashboard-summary"],
    queryFn: getPipelineDashboardSummary,
    refetchInterval: 30_000,
  });
  const { data: realtimeMetrics } = useQuery({
    queryKey: ["realtime-pipeline-metrics"],
    queryFn: getRealtimePipelineMetrics,
    refetchInterval: 20_000,
  });
  const connectorDrift = dashboardSummary?.connectorDrift ?? [];
  const metricByPipeline = useMemo(
    () => new Map((realtimeMetrics ?? []).map((metric) => [metric.pipelineId, metric])),
    [realtimeMetrics],
  );

  const { data: history, isLoading: historyLoading } = useQuery({
    queryKey: ["pipeline-history", detailPipelineId],
    queryFn: () => getPipelineHistory(detailPipelineId!),
    enabled: detailPipelineId !== null,
  });
  const detailPipeline = pipelines?.find((p) => p.id === detailPipelineId) ?? null;
  const { data: consistencyChecks, isLoading: consistencyLoading } = useQuery({
    queryKey: ["pipeline-consistency-checks", detailPipelineId],
    queryFn: () => listPipelineConsistencyChecks(detailPipelineId!),
    enabled: detailPipelineId !== null,
  });

  const matchesQuickFilter = (pipeline: PipelineResponse) => {
    const runtimeStatus = runtimeByPipeline.get(pipeline.id)?.runtimeStatus;
    const lag = metricByPipeline.get(pipeline.id)?.consumerLag ?? 0;
    if (quickFilter === "READY") return runtimeStatus === "READY" || (!runtimeStatus && pipeline.status === "READY");
    if (quickFilter === "RUNNING") return runtimeStatus === "RUNNING";
    if (quickFilter === "LAGGING") return lag > 0;
    if (quickFilter === "ERROR") return ["FAILED", "MISSING", "DEGRADED"].includes(runtimeStatus ?? pipeline.status);
    if (quickFilter === "STOPPED") return ["STOPPED", "PAUSED"].includes(runtimeStatus ?? pipeline.status);
    return true;
  };

  const filteredPipelines = useMemo(() => {
    const name = nameFilter.trim().toLowerCase();
    const topic = topicFilter.trim().toLowerCase();
    return (pipelines ?? []).filter((p) => {
      if (!matchesQuickFilter(p)) return false;
      if (selectedTreeKey.startsWith("connection:")) {
        const connectionId = Number(selectedTreeKey.split(":")[1]);
        if (p.sourceConnectionId !== connectionId) return false;
      } else if (selectedTreeKey.startsWith("schema:")) {
        const [, connectionId, schema] = selectedTreeKey.split(":");
        if (p.sourceConnectionId !== Number(connectionId) || p.sourceSchema !== schema) return false;
      } else if (selectedTreeKey.startsWith("pipeline:")) {
        if (p.id !== Number(selectedTreeKey.split(":")[1])) return false;
      } else if (selectedTreeKey === "log-files" && p.pipelineType !== "LOG_FILE") {
        return false;
      }
      if (name && !p.name.toLowerCase().includes(name)) {
        return false;
      }
      if (topic && !(p.topicName ?? "").toLowerCase().includes(topic)) {
        return false;
      }
      if (statusFilter.length > 0 && !statusFilter.includes(p.status)) {
        return false;
      }
      if (typeFilter.length > 0 && !typeFilter.includes(p.pipelineType)) {
        return false;
      }
      return true;
    }).sort((a, b) => {
      const severity = (STATUS_SEVERITY[a.status] ?? 99) - (STATUS_SEVERITY[b.status] ?? 99);
      if (severity !== 0) return severity;
      const aLag = metricByPipeline.get(a.id)?.consumerLag ?? -1;
      const bLag = metricByPipeline.get(b.id)?.consumerLag ?? -1;
      return bLag - aLag;
    });
  }, [pipelines, nameFilter, topicFilter, statusFilter, typeFilter, metricByPipeline, runtimeByPipeline, selectedTreeKey, quickFilter]);

  const pipelineTreeData = useMemo(() => {
    const search = treeSearch.trim().toLowerCase();
    const connectionNameById = new Map((connections ?? []).map((connection) => [connection.id, connection.name]));
    const sourceGroups = new Map<number, Map<string, PipelineResponse[]>>();
    const logPipelines: PipelineResponse[] = [];

    for (const pipeline of pipelines ?? []) {
      if (!matchesQuickFilter(pipeline)) continue;
      if (pipeline.pipelineType === "LOG_FILE" || pipeline.sourceConnectionId == null) {
        if (!search || pipeline.name.toLowerCase().includes(search) || pipeline.targetTable.toLowerCase().includes(search)) {
          logPipelines.push(pipeline);
        }
        continue;
      }
      const matches = !search
        || pipeline.name.toLowerCase().includes(search)
        || (pipeline.sourceTable ?? "").toLowerCase().includes(search)
        || (pipeline.sourceSchema ?? "").toLowerCase().includes(search)
        || (connectionNameById.get(pipeline.sourceConnectionId) ?? "").toLowerCase().includes(search);
      if (!matches) continue;
      const schemas = sourceGroups.get(pipeline.sourceConnectionId) ?? new Map<string, PipelineResponse[]>();
      const schema = pipeline.sourceSchema ?? "스키마 없음";
      schemas.set(schema, [...(schemas.get(schema) ?? []), pipeline]);
      sourceGroups.set(pipeline.sourceConnectionId, schemas);
    }

    const children: NonNullable<TreeProps["treeData"]> = [...sourceGroups.entries()].map(([connectionId, schemas]) => {
      const count = [...schemas.values()].reduce((sum, rows) => sum + rows.length, 0);
      return {
        key: `connection:${connectionId}`,
        title: <Space size={4}><span>{connectionNameById.get(connectionId) ?? `연결 ${connectionId}`}</span><Tag>{count}</Tag></Space>,
        children: [...schemas.entries()].map(([schema, rows]) => ({
          key: `schema:${connectionId}:${schema}`,
          title: <Space size={4}><span>{schema}</span><Tag>{rows.length}</Tag></Space>,
          children: rows.map((pipeline) => ({
            key: `pipeline:${pipeline.id}`,
            title: <Space size={4}><span>{pipeline.name}</span><Tag color={RUNTIME_STATUS_COLOR[runtimeByPipeline.get(pipeline.id)?.runtimeStatus ?? ""] ?? "default"}>{pipeline.sourceTable}</Tag></Space>,
            isLeaf: true,
          })),
        })),
      };
    });
    if (logPipelines.length > 0) {
      children.push({
        key: "log-files",
        title: <Space size={4}><span>로그파일</span><Tag>{logPipelines.length}</Tag></Space>,
        children: logPipelines.map((pipeline) => ({
          key: `pipeline:${pipeline.id}`,
          title: pipeline.name,
          isLeaf: true,
        })),
      });
    }
    const visibleCount = (pipelines ?? []).filter(matchesQuickFilter).length;
    return [{ key: "all", title: <Space size={4}><span>전체 파이프라인</span><Tag>{visibleCount}</Tag></Space>, children }];
  }, [pipelines, connections, treeSearch, runtimeByPipeline, metricByPipeline, quickFilter]);

  const invalidatePipelines = () => queryClient.invalidateQueries({ queryKey: ["pipelines"] });

  const deleteMutation = useMutation({
    mutationFn: deletePipeline,
    onSuccess: () => {
      message.success("파이프라인을 삭제했습니다 (CDC 커넥터도 함께 정리됨).");
      invalidatePipelines();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const consistencyMutation = useMutation({
    mutationFn: checkPipelineConsistency,
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ["pipeline-consistency-checks", result.pipelineId] });
      if (result.result === "MATCH") message.success("소스와 타깃의 통계 추정 행 수가 일치합니다.");
      else if (result.result === "MISMATCH") message.warning("소스와 타깃의 통계 추정 행 수가 다릅니다.");
      else message.info(result.message ?? "검증 결과를 확인하세요.");
    },
    onError: (error: Error) => message.error(error.message),
  });

  const dismissDriftMutation = useMutation({
    mutationFn: dismissConnectorDrift,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["pipeline-dashboard-summary"] }),
    onError: (error: Error) => message.error(error.message),
  });

  const driftByPipeline = connectorDrift.reduce<Record<number, { pipelineName: string; connectorNames: string[] }>>(
    (acc, entry) => {
      const existing = acc[entry.pipelineId] ?? { pipelineName: entry.pipelineName, connectorNames: [] };
      existing.connectorNames.push(entry.connectorName);
      acc[entry.pipelineId] = existing;
      return acc;
    },
    {},
  );

  return (
    <div>
      <Card title="CDC 파이프라인">
        {Object.entries(driftByPipeline).map(([pipelineId, info]) => (
          <Alert
            key={pipelineId}
            type="warning"
            showIcon
            closable
            onClose={() => dismissDriftMutation.mutate(Number(pipelineId))}
            style={{ marginBottom: 12 }}
            message="파이프라인 커넥터가 CDC에서 사라졌습니다"
            description={
              <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                <span>
                  <b>{info.pipelineName}</b> — {info.connectorNames.join(", ")} 없음
                  {/* 여기서 바로 재배포하지 않는다 - 배포/시작/중지는 Airflow 제어 DAG가
                      단독으로 지시한다(화면은 생성/삭제만 담당). 화면과 DAG 양쪽에서
                      배포가 가능하면 "누가 언제 실행시켰는지"가 이력에서 흐려진다. */}
                  <br />
                  <Typography.Text type="secondary">
                    복구하려면 Airflow에서 이 파이프라인의 제어 DAG를 action=deploy로 실행하세요.
                  </Typography.Text>
                </span>
              </div>
            }
          />
        ))}
        <div style={{ display: "grid", gridTemplateColumns: "290px minmax(0, 1fr)", gap: 16, alignItems: "start" }}>
          <Card size="small" title="파이프라인 탐색" styles={{ body: { padding: 12, overflowX: "auto" } }}>
            <Segmented
              block
              size="small"
              value={quickFilter}
              onChange={(value) => { setQuickFilter(String(value)); setSelectedTreeKey("all"); }}
              options={[
                { value: "ALL", label: "전체" },
                { value: "READY", label: "대기" },
                { value: "RUNNING", label: "실행" },
              ]}
              style={{ marginBottom: 8 }}
            />
            <Segmented
              block
              size="small"
              value={["LAGGING", "ERROR", "STOPPED"].includes(quickFilter) ? quickFilter : undefined}
              onChange={(value) => { setQuickFilter(String(value)); setSelectedTreeKey("all"); }}
              options={[
                { value: "LAGGING", label: "지연" },
                { value: "ERROR", label: "오류" },
                { value: "STOPPED", label: "중지" },
              ]}
              style={{ marginBottom: 12 }}
            />
            <Input.Search
              allowClear
              placeholder="이름·스키마·테이블 검색"
              value={treeSearch}
              onChange={(event) => setTreeSearch(event.target.value)}
              style={{ marginBottom: 12 }}
            />
            <Tree
              blockNode
              defaultExpandAll
              autoExpandParent={Boolean(treeSearch)}
              treeData={pipelineTreeData}
              selectedKeys={[selectedTreeKey]}
              onSelect={(keys) => {
                // 트리에서 파이프라인을 선택하면 목록(내역)만 그 파이프라인으로 좁힌다.
                // 상세창(Drawer)은 '상세' 버튼으로만 열도록 하여 선택만으로 열리지 않게 한다.
                const key = String(keys[0] ?? "all");
                setSelectedTreeKey(key);
              }}
            />
          </Card>
          <div style={{ minWidth: 0 }}>
        <div style={{ display: "flex", flexWrap: "wrap", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16, gap: 12 }}>
          <Space wrap>
            <Input
              placeholder="이름 검색"
              allowClear
              style={{ width: 200 }}
              value={nameFilter}
              onChange={(e) => setNameFilter(e.target.value)}
            />
            <Input
              placeholder="Topic 검색"
              allowClear
              style={{ width: 200 }}
              value={topicFilter}
              onChange={(e) => setTopicFilter(e.target.value)}
            />
            <Select
              mode="multiple"
              allowClear
              placeholder="상태"
              style={{ minWidth: 160 }}
              options={STATUS_OPTIONS}
              value={statusFilter}
              onChange={setStatusFilter}
            />
            <Select
              mode="multiple"
              allowClear
              placeholder="유형"
              style={{ minWidth: 140 }}
              options={TYPE_OPTIONS}
              value={typeFilter}
              onChange={setTypeFilter}
            />
          </Space>
          <Space wrap>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => navigate("/cdc/create")}
              disabled={!canWrite || !connections || connections.length < 1}
              title={
                !canWrite
                  ? "CDC 쓰기 권한이 없습니다"
                  : !connections || connections.length < 1
                    ? "연결정보가 최소 1개는 있어야 합니다"
                    : undefined
              }
            >
              파이프라인 신규 생성
            </Button>
          </Space>
        </div>

        <Table<PipelineResponse>
        rowKey="id"
        loading={isLoading}
        dataSource={filteredPipelines}
        scroll={{ x: 1120 }}
        pagination={{ pageSize: 20, hideOnSinglePage: true, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
        columns={[
          ...cdcPipelineColumns(runtimeByPipeline, metricByPipeline),
          {
            title: "관리",
            width: 130,
            render: (_, record) => (
              <Space wrap>
                <Button size="small" onClick={() => setDetailPipelineId(record.id)}>
                  상세
                </Button>
                <Popconfirm
                  title="이 파이프라인을 삭제할까요?"
                  description="배포된 CDC 커넥터도 함께 삭제됩니다."
                  onConfirm={() => deleteMutation.mutate(record.id)}
                  disabled={!canWrite}
                >
                  <Button
                    danger
                    size="small"
                    disabled={!canWrite}
                    title={!canWrite ? "CDC 쓰기 권한이 없습니다" : undefined}
                  >
                    삭제
                  </Button>
                </Popconfirm>
              </Space>
            ),
          },
        ]}
        />
          </div>
        </div>
      </Card>


      <Drawer
        title={detailPipeline ? `파이프라인 상세 · ${detailPipeline.name}` : "파이프라인 상세"}
        open={detailPipelineId !== null}
        onClose={() => setDetailPipelineId(null)}
        width={640}
        destroyOnHidden
      >
        {detailPipeline && (
          <Tabs
            items={[
              {
                key: "info",
                label: "기본정보",
                children: (
                  <Descriptions column={1} bordered size="small">
                    <Descriptions.Item label="이름">{detailPipeline.name}</Descriptions.Item>
                    <Descriptions.Item label="유형">{detailPipeline.pipelineType}</Descriptions.Item>
                    <Descriptions.Item label="소스">
                      {detailPipeline.pipelineType === "LOG_FILE"
                        ? "Filebeat"
                        : `${detailPipeline.sourceDbType} · ${detailPipeline.sourceSchema}.${detailPipeline.sourceTable}`}
                    </Descriptions.Item>
                    <Descriptions.Item label="타겟">
                      {`${detailPipeline.targetDbType} · ${detailPipeline.targetSchema}.${detailPipeline.targetTable}`}
                    </Descriptions.Item>
                    <Descriptions.Item label="Topic">{detailPipeline.topicName}</Descriptions.Item>
                    {detailPipeline.pipelineType === "TABLE_CDC" && (
                      <Descriptions.Item label="스냅샷 모드">
                        {detailPipeline.snapshotMode === "NO_DATA" ? "기존 데이터 미적재 · 이후 CDC" : "초기 적재 후 CDC"}
                      </Descriptions.Item>
                    )}
                    <Descriptions.Item label="상태">
                      {renderRuntimeStatus(detailPipeline, runtimeByPipeline.get(detailPipeline.id))}
                    </Descriptions.Item>
                    <Descriptions.Item label="설명">{detailPipeline.description ?? "-"}</Descriptions.Item>
                    <Descriptions.Item label="생성 시각">{detailPipeline.createdAt}</Descriptions.Item>
                    <Descriptions.Item label="수정 시각">{detailPipeline.updatedAt}</Descriptions.Item>
                  </Descriptions>
                ),
              },
              {
                key: "runtime",
                label: "운영 상태",
                children: (() => {
                  const runtime = runtimeByPipeline.get(detailPipeline.id);
                  if (!runtime) return <Spin size="small" />;
                  return (
                    <>
                      {runtime.statusMismatch && (
                        <Alert
                          type="warning"
                          showIcon
                          message="저장 상태와 CDC 실측 상태가 다릅니다"
                          description={runtime.runtimeStatusReason}
                          style={{ marginBottom: 16 }}
                        />
                      )}
                      <Descriptions column={1} bordered size="small">
                        <Descriptions.Item label="화면 실측 상태">
                          <Tag color={RUNTIME_STATUS_COLOR[runtime.runtimeStatus] ?? "default"}>
                            {RUNTIME_STATUS_LABEL[runtime.runtimeStatus] ?? runtime.runtimeStatus}
                          </Tag>
                        </Descriptions.Item>
                        <Descriptions.Item label="메타데이터 저장 상태">
                          {CDC_STATUS_LABEL[runtime.storedStatus] ?? runtime.storedStatus}
                        </Descriptions.Item>
                        <Descriptions.Item label="Source Connector">
                          {runtime.sourceConnectorState ?? "—"} · Tasks {runtime.sourceTaskStates.join(", ") || "—"}
                        </Descriptions.Item>
                        <Descriptions.Item label="Sink Connector">
                          {runtime.sinkConnectorState ?? "—"} · Tasks {runtime.sinkTaskStates.join(", ") || "—"}
                        </Descriptions.Item>
                        <Descriptions.Item label="마지막 실측 시각">
                          {runtime.runtimeCheckedAt ?? "확인 기록 없음"}
                        </Descriptions.Item>
                        <Descriptions.Item label="최근 제어 요청">
                          {runtime.lastCommand
                            ? `${runtime.lastCommand} · ${runtime.lastCommandResult ?? "처리 중"} · ${runtime.lastCommandAt ?? "—"}`
                            : "이력 없음"}
                        </Descriptions.Item>
                        <Descriptions.Item label="최근 제어 메시지">
                          {runtime.lastCommandMessage ?? "—"}
                        </Descriptions.Item>
                      </Descriptions>
                    </>
                  );
                })(),
              },
              {
                key: "connectors",
                label: "Connector",
                children: (
                  <>
                    <Table
                      rowKey="id"
                      size="small"
                      pagination={false}
                      dataSource={detailPipeline.connectors}
                      style={{ marginBottom: 16 }}
                      columns={[
                        { title: "역할", dataIndex: "connectorRole" },
                        { title: "커넥터명", dataIndex: "connectorName" },
                        {
                          title: "상태",
                          dataIndex: "status",
                          render: (value: string) => <Tag>{value}</Tag>,
                        },
                      ]}
                    />
                    <Collapse
                      items={detailPipeline.connectors.map((c) => ({
                        key: c.id,
                        label: `${c.connectorName} · 설정/실행상태`,
                        children: (
                          <>
                            <div style={{ fontWeight: 600, marginBottom: 4 }}>Connector 설정</div>
                            <pre style={{ whiteSpace: "pre-wrap", fontSize: 12 }}>{c.connectorConfigJson}</pre>
                            <div style={{ fontWeight: 600, margin: "12px 0 4px" }}>실행상태</div>
                            <pre style={{ whiteSpace: "pre-wrap", fontSize: 12 }}>
                              {c.lastStatusJson ?? "아직 조회된 상태가 없습니다."}
                            </pre>
                          </>
                        ),
                      }))}
                    />
                  </>
                ),
              },
              {
                key: "consistency",
                label: "정합성",
                children: (
                  <>
                    <Alert
                      type="info"
                      showIcon
                      message="부하 제한 통계 검증"
                      description="운영 테이블을 COUNT(*)로 전체 스캔하지 않고 DB 옵티마이저 통계의 추정 행 수를 비교합니다. 불일치 시 실제 누락 확정이 아니라 추가 점검이 필요하다는 뜻입니다."
                      style={{ marginBottom: 16 }}
                    />
                    <Button
                      type="primary"
                      loading={consistencyMutation.isPending}
                      // 정합성 검증은 POST 라 백엔드에서 KAFKA WRITE 로 판정된다.
                      // 막아두지 않으면 조회자가 눌렀을 때 403 오류만 보게 된다.
                      disabled={!canWrite || detailPipeline.pipelineType === "LOG_FILE"}
                      title={!canWrite ? "CDC 쓰기 권한이 없습니다" : undefined}
                      onClick={() => consistencyMutation.mutate(detailPipeline.id)}
                      style={{ marginBottom: 16 }}
                    >지금 검증</Button>
                    <Table
                      rowKey="id"
                      size="small"
                      loading={consistencyLoading}
                      dataSource={consistencyChecks}
                      pagination={false}
                      scroll={{ x: 650 }}
                      columns={[
                        { title: "검증 시각", dataIndex: "checkedAt", width: 170 },
                        { title: "소스 추정", dataIndex: "sourceCount", width: 110, render: (value: number | null) => value == null ? "—" : value.toLocaleString() },
                        { title: "타깃 추정", dataIndex: "targetCount", width: 110, render: (value: number | null) => value == null ? "—" : value.toLocaleString() },
                        { title: "차이", dataIndex: "difference", width: 90, render: (value: number | null) => value == null ? "—" : value.toLocaleString() },
                        { title: "결과", dataIndex: "result", width: 100, render: (value: string) => <Tag color={value === "MATCH" ? "success" : value === "MISMATCH" ? "warning" : "default"}>{value === "MATCH" ? "일치" : value === "MISMATCH" ? "불일치" : "확인 불가"}</Tag> },
                      ]}
                    />
                  </>
                ),
              },
              {
                key: "history",
                label: "이력",
                children: (
                  <Table<PipelineCommandHistoryResponse>
                    rowKey="id"
                    size="small"
                    loading={historyLoading}
                    dataSource={history}
                    pagination={false}
                    columns={[
                      { title: "명령", dataIndex: "command" },
                      {
                        title: "결과",
                        dataIndex: "result",
                        render: (value: string | null) =>
                          value ? <Tag color={value === "SUCCESS" ? "success" : "error"}>{value}</Tag> : "-",
                      },
                      { title: "메시지", dataIndex: "message", ellipsis: true },
                      { title: "요청 시각", dataIndex: "requestedAt" },
                    ]}
                  />
                ),
              },
            ]}
          />
        )}
      </Drawer>
    </div>
  );
}
