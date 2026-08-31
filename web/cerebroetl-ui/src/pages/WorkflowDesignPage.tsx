import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Card, Empty, Form, Input, message, Modal, Popconfirm, Space, Table, Tag, Tree, TreeSelect } from "antd";
import { PlusOutlined, SyncOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import dayjs from "dayjs";
import { useAuth } from "../auth/AuthContext";
import { getNifiProcessGroupTree, type NifiProcessGroupTreeNode } from "../api/platform";
import { syncEtlJobs } from "../api/etlJobs";
import {
  createWorkflow,
  deleteWorkflow,
  listWorkflows,
  unpublishWorkflow,
  type WorkflowSummary,
} from "../api/workflows";

// 워크플로우 목록. 여기서 만들고, 캔버스(설계)로 들어가 job을 배치한다.
//
// "게시" 여부가 곧 "Airflow에 DAG가 있는가"다. 저장만 해서는 DAG가 생기지 않으므로
// 목록에서 그 상태를 한눈에 보여준다.
//
// 워크플로우는 NiFi 그룹에 속한다(Informatica의 폴더와 같은 자리). 왼쪽 트리에서 그룹을
// 고르면 그 그룹 워크플로우만 보여주고, 새로 만들 때도 그 그룹으로 들어간다.

/** 그룹 트리에서 "그룹 미지정"을 고른 상태. 여러 그룹의 job을 모은 워크플로우가 여기 모인다. */
const UNGROUPED = "__ungrouped__";

export function WorkflowDesignPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { can, permissionsLoaded } = useAuth();
  const canWrite = !permissionsLoaded || can("NIFI", "WRITE");
  const [createOpen, setCreateOpen] = useState(false);
  const [selectedGroup, setSelectedGroup] = useState<string>();
  const [form] = Form.useForm();

  const { data, isLoading } = useQuery({ queryKey: ["workflows"], queryFn: listWorkflows });
  const treeQuery = useQuery({ queryKey: ["nifi-pg-tree"], queryFn: getNifiProcessGroupTree });

  const refresh = () => queryClient.invalidateQueries({ queryKey: ["workflows"] });

  /**
   * 업무 그룹 트리는 NiFi 미러링 결과라, 캔버스에서 그룹을 만든 직후에는 여기 나오지 않는다
   * (미러링은 5분 주기). 기다리지 않고 바로 반영하고 싶을 때 쓴다.
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
      void treeQuery.refetch();
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
   * 그룹 트리. ETL 화면과 같은 NiFi 그룹 계층을 쓰되, 워크플로우를 담을 수 있는
   * GROUPING 그룹만 남긴다(job 그룹은 워크플로우의 재료지 보관함이 아니다).
   * 오른쪽 숫자는 그 그룹과 하위에 있는 워크플로우 수다.
   */
  const countByGroup = new Map<string, number>();
  workflows.forEach((w) => {
    const key = w.nifiGroupPgId ?? UNGROUPED;
    countByGroup.set(key, (countByGroup.get(key) ?? 0) + 1);
  });

  const buildGroupTree = (node: NifiProcessGroupTreeNode): {
    key: string; title: React.ReactNode; children?: unknown[]; count: number;
  } | null => {
    if (node.groupType === "JOB") {
      return null;
    }
    const children = (node.children ?? []).map(buildGroupTree).filter(Boolean) as Array<{
      key: string; title: React.ReactNode; children?: unknown[]; count: number;
    }>;
    const count = (countByGroup.get(node.id) ?? 0)
      + children.reduce((sum, child) => sum + child.count, 0);
    return {
      key: node.id,
      count,
      title: (
        <span>
          {node.name}
          {count ? <span style={{ color: "#888" }}> ({count})</span> : null}
        </span>
      ),
      children: children.length ? children : undefined,
    };
  };

  const groupTree = treeQuery.data ? [buildGroupTree(treeQuery.data)].filter(Boolean) : [];

  const groupNames = new Map<string, string>();
  const collectNames = (node: NifiProcessGroupTreeNode) => {
    groupNames.set(node.id, node.name);
    (node.children ?? []).forEach(collectNames);
  };
  if (treeQuery.data) {
    collectNames(treeQuery.data);
  }
  const groupNameOf = (pgId: string) => groupNames.get(pgId) ?? pgId.slice(0, 8);
  const ungroupedCount = countByGroup.get(UNGROUPED) ?? 0;

  /** 그룹 id -> 그 그룹의 하위 전체 id. 상위 그룹을 고르면 하위 워크플로우까지 함께 보여준다. */
  const subtreeIds = (root: NifiProcessGroupTreeNode | undefined, target: string): Set<string> => {
    const found = new Set<string>();
    const collect = (node: NifiProcessGroupTreeNode) => {
      found.add(node.id);
      (node.children ?? []).forEach(collect);
    };
    const find = (node: NifiProcessGroupTreeNode): boolean => {
      if (node.id === target) {
        collect(node);
        return true;
      }
      return (node.children ?? []).some(find);
    };
    if (root) {
      find(root);
    }
    return found;
  };

  const visible = !selectedGroup
    ? workflows
    : selectedGroup === UNGROUPED
      ? workflows.filter((w) => !w.nifiGroupPgId)
      : (() => {
          const ids = subtreeIds(treeQuery.data, selectedGroup);
          return workflows.filter((w) => w.nifiGroupPgId && ids.has(w.nifiGroupPgId));
        })();

  /** 그룹 선택기(생성 모달)용. 워크플로우를 담을 수 있는 그룹만 고를 수 있다. */
  const buildGroupOptions = (node: NifiProcessGroupTreeNode): {
    value: string; title: string; children?: unknown[];
  } | null => {
    if (node.groupType === "JOB") {
      return null;
    }
    const children = (node.children ?? []).map(buildGroupOptions).filter(Boolean);
    return { value: node.id, title: node.name, children: children.length ? children : undefined };
  };
  const groupOptions = treeQuery.data
    ? [buildGroupOptions(treeQuery.data)].filter(Boolean)
    : [];

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
    {
      title: "업무 그룹",
      width: 130,
      render: (_, row) => row.nifiGroupPgId
        ? <span>{groupNameOf(row.nifiGroupPgId)}</span>
        : <span style={{ color: "#888" }}>미지정</span>,
    },
    { title: "job 수", dataIndex: "nodeCount", width: 90 },
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
              <Button size="small" disabled={!canWrite}>게시 취소</Button>
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
      title="워크플로우 스케줄링"
      extra={
        <Space>
        <Button size="small" icon={<SyncOutlined />} loading={syncMutation.isPending}
                onClick={() => syncMutation.mutate()}>
          동기화
        </Button>
        <Button type="primary" icon={<PlusOutlined />} disabled={!canWrite}
                onClick={() => {
                  // 트리에서 그룹을 골라둔 채로 만들면 그 그룹으로 들어가는 게 자연스럽다.
                  form.setFieldsValue({
                    nifiGroupPgId: selectedGroup && selectedGroup !== UNGROUPED ? selectedGroup : undefined,
                  });
                  setCreateOpen(true);
                }}>
          새 워크플로우
        </Button>
        </Space>
      }
    >
      <div style={{ display: "flex", gap: 16, alignItems: "flex-start" }}>
        <Card size="small" title="업무 그룹" style={{ width: 260, flex: "0 0 260px" }}
              styles={{ body: { maxHeight: 560, overflowY: "auto" } }}
              extra={selectedGroup
                ? <Button size="small" type="link" onClick={() => setSelectedGroup(undefined)}>전체</Button>
                : null}>
          {treeQuery.isLoading ? <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="불러오는 중" /> : (
            <>
              <Tree
                blockNode
                defaultExpandAll
                selectedKeys={selectedGroup ? [selectedGroup] : []}
                treeData={groupTree as never}
                onSelect={(keys) => setSelectedGroup(keys.length ? String(keys[0]) : undefined)}
              />
              {/* 그룹을 안 정한 워크플로우도 어딘가에서는 보여야 한다. */}
              {ungroupedCount > 0 && (
                <Tree
                  blockNode
                  selectedKeys={selectedGroup === UNGROUPED ? [UNGROUPED] : []}
                  treeData={[{ key: UNGROUPED, title: `그룹 미지정 (${ungroupedCount})` }] as never}
                  onSelect={(keys) => setSelectedGroup(keys.length ? UNGROUPED : undefined)}
                />
              )}
            </>
          )}
        </Card>

        <Table<WorkflowSummary>
          rowKey="id"
          size="middle"
          style={{ flex: 1, minWidth: 0 }}
          loading={isLoading}
          dataSource={visible}
          columns={columns}
          pagination={{ pageSize: 20, showTotal: (total) => `전체 ${total}건` }}
          locale={{
            emptyText: (
              <Empty description={selectedGroup
                ? "이 그룹에는 아직 워크플로우가 없습니다."
                : "아직 워크플로우가 없습니다. ETL에서 만든 job을 캔버스에 배치해 워크플로우를 구성하세요."} />
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
          {/* 워크플로우 키(=dag_id의 축)는 서버가 그룹·이름에서 만든다. 사용자가 짓게 하면
              그룹이 달라도 같은 이름을 써서 부딪히고, 규칙을 어긴 값이 게시 때 터진다. */}
          <Form.Item name="nifiGroupPgId" label="업무 그룹"
                     extra="이 그룹의 job으로 워크플로우를 구성합니다. 비우면 그룹 없이 만듭니다."
                     rules={[{ required: true, message: "업무 그룹을 골라주세요." }]}>
            <TreeSelect treeDefaultExpandAll placeholder="그룹 선택"
                        treeData={groupOptions as never} />
          </Form.Item>
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
