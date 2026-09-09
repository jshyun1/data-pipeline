import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Card, Empty, Form, Input, message, Modal, Popconfirm, Space, Table, Tag, Tree } from "antd";
import { PlusOutlined, SyncOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import dayjs from "dayjs";
import { useAuth } from "../auth/AuthContext";
import { syncEtlJobs } from "../api/etlJobs";
import {
  createWorkflow,
  deleteWorkflow,
  listWorkflows,
  unpublishWorkflow,
  type WorkflowSummary,
} from "../api/workflows";
import {
  buildWorkflowHierarchy,
  flattenWorkflowTree,
  type WorkflowTreeItem,
} from "../utils/workflowTree";

// 워크플로우 목록. 여기서 만들고, 캔버스(설계)로 들어가 job을 배치한다.
//
// "게시" 여부가 곧 "Airflow에 DAG가 있는가"다. 저장만 해서는 DAG가 생기지 않으므로
// 목록에서 그 상태를 한눈에 보여준다.
//
// 왼쪽 트리는 «워크플로우 계층»이다. 최상단은 누구의 하위도 아닌 워크플로우이고, 그 아래는
// 캔버스에 노드로 얹어 둔 하위 워크플로우다. 고르면 그 워크플로우와 하위 전부를 표에 보여준다.
// 워크플로우는 업무 그룹(NiFi 그룹)에 속하지 않는다. 분류는 이 계층이 전부다.

export function WorkflowDesignPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { can, permissionsLoaded } = useAuth();
  // 생성·삭제는 설계(NIFI 쓰기), 게시 취소는 DAG 를 없애는 운영 동작(AIRFLOW 쓰기).
  const canWrite = !permissionsLoaded || can("NIFI", "WRITE");
  const canPublish = !permissionsLoaded || can("AIRFLOW", "WRITE");
  const [createOpen, setCreateOpen] = useState(false);
  const [selectedWorkflowId, setSelectedWorkflowId] = useState<number>();
  const [form] = Form.useForm();

  const { data, isLoading } = useQuery({ queryKey: ["workflows"], queryFn: listWorkflows });

  const refresh = () => queryClient.invalidateQueries({ queryKey: ["workflows"] });

  /**
   * ETL(NiFi)에서 만든 job 을 바로 끌어와 쓰고 싶을 때. 미러링은 5분 주기라 방금 만든 job 이
   * 캔버스 팔레트에 아직 없을 수 있는데, 이걸 누르면 기다리지 않고 반영된다.
   */
  const syncMutation = useMutation({
    mutationFn: syncEtlJobs,
    onSuccess: (result) => {
      const changed = result.created + result.deleted;
      message.success(changed > 0
          ? `동기화 완료 - job ${result.jobsSeen}개 (신규 ${result.created}, 삭제 ${result.deleted})`
          : `동기화 완료 - job ${result.jobsSeen}개 (변경 없음)`);
    },
    onError: (error: Error) => message.warning(`동기화 실패(목록만 새로고침합니다): ${error.message}`),
    onSettled: () => {
      void refresh();
    },
  });

  const createMutation = useMutation({
    mutationFn: createWorkflow,
    onSuccess: (created) => {
      message.success("워크플로우를 만들었습니다. 캔버스에서 job을 배치해주세요.");
      setCreateOpen(false);
      form.resetFields();
      void refresh();
      navigate(`/workflows/design/${created.id}`);
    },
    onError: (error: Error) => message.error(error.message),
  });

  const unpublishMutation = useMutation({
    mutationFn: unpublishWorkflow,
    onSuccess: () => {
      message.success("게시를 내렸습니다. 잠시 후 Airflow에서 DAG가 사라집니다.");
      void refresh();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const deleteMutation = useMutation({
    mutationFn: deleteWorkflow,
    onSuccess: () => {
      message.success("삭제했습니다.");
      void refresh();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const workflows = data ?? [];

  /**
   * 워크플로우 트리. 최상단은 «누구의 하위도 아닌» 워크플로우이고, 그 아래는 캔버스에
   * 노드로 얹어 둔 하위 워크플로우다. 이름 오른쪽 숫자는 그 워크플로우 캔버스에 놓인
   * job 수다(하위 워크플로우 노드는 세지 않는다 - 그건 자기 줄에서 다시 세어진다).
   *
   * 예전에는 NiFi 업무 그룹(DW/DZ/Template)으로 묶어 보여줬는데, 여기서 알고 싶은 것은
   * «total 을 돌리면 무엇이 같이 도는가»이고 그건 그룹이 아니라 캔버스에 그린 상하 관계다.
   */
  const workflowTree = buildWorkflowHierarchy(workflows);

  const toTreeData = (items: WorkflowTreeItem[]): Array<{
    key: string; title: React.ReactNode; children?: unknown[];
  }> => items.map((item) => ({
    key: String(item.workflow.id),
    title: (
      <span>
        {item.workflow.name}
        <span style={{ color: "#888" }}> ({item.workflow.jobCount})</span>
      </span>
    ),
    children: item.children.length ? toTreeData(item.children) : undefined,
  }));
  const workflowTreeData = toTreeData(workflowTree);
  // 최상단은 펼쳐 둔다(= 하위 워크플로우까지 바로 보인다). 계층이 깊지 않아 이 정도가 낫다.
  const workflowTreeOpenKeys = workflowTreeData.map((node) => node.key);

  /**
   * 트리에서 워크플로우를 고르면 그 워크플로우와 <b>하위 전부</b>를 표에 보여준다.
   * 상위 그룹을 고르면 하위까지 보여주던 예전 동작을 계층 기준으로 옮긴 것이다 -
   * total 을 고르면 total·dz_com_daily·dz_pop_daily 가 함께 보인다.
   */
  const findSubtree = (items: WorkflowTreeItem[], id: number): WorkflowTreeItem | undefined => {
    for (const item of items) {
      if (item.workflow.id === id) {
        return item;
      }
      const hit = findSubtree(item.children, id);
      if (hit) {
        return hit;
      }
    }
    return undefined;
  };
  const selectedSubtree = selectedWorkflowId === undefined
    ? undefined
    : findSubtree(workflowTree, selectedWorkflowId);
  const visible = selectedSubtree ? flattenWorkflowTree([selectedSubtree]) : workflows;

  const columns: ColumnsType<WorkflowSummary> = [
    {
      title: "워크플로우",
      render: (_, row) => (
        <Space direction="vertical" size={0}>
          <Button type="link" style={{ padding: 0, height: "auto", fontWeight: 600 }}
                  onClick={() => navigate(`/workflows/design/${row.id}`)}>
            {row.name}
          </Button>
          <span style={{ color: "#888", fontSize: 12 }}>{row.dagId}</span>
        </Space>
      ),
    },
    { title: "job 수", dataIndex: "jobCount", width: 90 },
    {
      title: "스케줄",
      width: 160,
      render: (_, row) => {
        // 선행이 있으면 시간이 아니라 "앞 워크플로우가 끝나면" 실행된다.
        if (row.upstreamCount) {
          return <Tag color="blue">선행 {row.upstreamCount}개 완료 후</Tag>;
        }
        return row.scheduleCron
          ? <span>{row.scheduleCron} <span style={{ color: "#888" }}>({row.timezone})</span></span>
          : <span style={{ color: "#888" }}>수동 실행</span>;
      },
    },
    {
      title: "게시 상태",
      width: 190,
      render: (_, row) => row.published
        ? (
          <Space direction="vertical" size={0}>
            <Tag color="success">게시됨</Tag>
            <span style={{ color: "#888", fontSize: 12 }}>
              {row.publishedAt ? dayjs(row.publishedAt).format("MM-DD HH:mm") : ""}
              {row.publishedBy ? ` · ${row.publishedBy}` : ""}
            </span>
          </Space>
        )
        : <Tag>미게시 (DAG 없음)</Tag>,
    },
    {
      title: "관리",
      width: 190,
      render: (_, row) => (
        <Space wrap>
          <Button size="small" onClick={() => navigate(`/workflows/design/${row.id}`)}>설계</Button>
          {row.published && (
            <Popconfirm title="게시를 내릴까요?" description="Airflow에서 DAG가 사라집니다."
                        onConfirm={() => unpublishMutation.mutate(row.id)}>
              <Button size="small" disabled={!canPublish}
                      title={canPublish ? undefined : "Airflow 쓰기 권한이 없습니다"}>게시 취소</Button>
            </Popconfirm>
          )}
          <Popconfirm title="이 워크플로우를 삭제할까요?"
                      description={row.published ? "게시 중이면 먼저 게시를 내려야 합니다." : undefined}
                      onConfirm={() => deleteMutation.mutate(row.id)}>
            <Button size="small" danger disabled={!canWrite}>삭제</Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Card
      className="wf-design-page"
      title="워크플로우 스케줄링"
      extra={
        <Space>
        <Button size="small" icon={<SyncOutlined />} loading={syncMutation.isPending}
                onClick={() => syncMutation.mutate()}>
          동기화
        </Button>
        <Button type="primary" icon={<PlusOutlined />} disabled={!canWrite}
                onClick={() => setCreateOpen(true)}>
          새 워크플로우
        </Button>
        </Space>
      }
    >
      <div className="wf-design-body">
        <Card size="small" title="워크플로우" style={{ width: 260, flex: "0 0 260px" }}
              extra={selectedWorkflowId !== undefined
                ? <Button size="small" type="link" onClick={() => setSelectedWorkflowId(undefined)}>전체</Button>
                : null}>
          {isLoading ? <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="불러오는 중" /> : (
            <Tree
              blockNode
              // 트리 데이터가 실린 뒤에 마운트되므로(위 isLoading 분기) 이 기본값이 그대로 먹는다.
              defaultExpandedKeys={workflowTreeOpenKeys}
              selectedKeys={selectedWorkflowId !== undefined ? [String(selectedWorkflowId)] : []}
              treeData={workflowTreeData as never}
              onSelect={(keys) => setSelectedWorkflowId(keys.length ? Number(keys[0]) : undefined)}
            />
          )}
        </Card>

        <Table<WorkflowSummary>
          rowKey="id"
          size="middle"
          className="wf-design-table"
          loading={isLoading}
          dataSource={visible}
          columns={columns}
          pagination={{ pageSize: 20, showTotal: (total) => `전체 ${total}건` }}
          locale={{
            emptyText: (
              <Empty description="아직 워크플로우가 없습니다. ETL에서 만든 job을 캔버스에 배치해 워크플로우를 구성하세요." />
            ),
          }}
        />
      </div>

      <Modal
        title="새 워크플로우"
        open={createOpen}
        onCancel={() => setCreateOpen(false)}
        onOk={() => form.submit()}
        confirmLoading={createMutation.isPending}
        okText="만들기"
        cancelText="취소"
      >
        <Form form={form} layout="vertical" onFinish={(values) => createMutation.mutate(values)}>
          {/* 워크플로우 키(=dag_id의 축)는 서버가 이름에서 만든다. 사용자가 짓게 하면
              규칙을 어긴 값이 게시 때 터진다. */}
          <Form.Item name="name" label="이름" rules={[{ required: true, message: "이름을 입력해주세요." }]}>
            <Input placeholder="DW 일배치" />
          </Form.Item>
          <Form.Item name="scheduleCron" label="스케줄"
                     extra="비워두면 수동 실행 전용입니다. 예: 0 2 * * * (매일 새벽 2시)">
            <Input placeholder="0 2 * * *" />
          </Form.Item>
        </Form>
      </Modal>
    </Card>
  );
}
