import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { Alert, Button, Card, Modal, Popconfirm, Radio, Space, Table, Tag, Tooltip, message } from "antd";
import { PlusOutlined } from "@ant-design/icons";
import dayjs from "dayjs";
import {
  BILLING_LABEL,
  DEPLOYMENT_LABEL,
  PROVIDER_LABEL,
  activateLlmProvider,
  deactivateLlmProvider,
  deleteLlmProvider,
  listLlmProviders,
  testSavedLlmProvider,
  type LlmProviderResponse,
} from "../../api/llm";

/**
 * GPT 관리. 등록 내역 중 <b>하나만</b> 선택한다(DB 부분 유니크 인덱스가 강제). 선택이 없으면
 * GPT 기능이 원래 없던 것처럼 화면에서 사라진다 - 그래서 «선택 해제» 버튼이 따로 있다(라디오는 스스로 해제할 수 없다).
 */
export function GptManagePage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { data: providers = [], isLoading } = useQuery({ queryKey: ["llm-providers"], queryFn: listLlmProviders });
  const active = providers.find((p) => p.active);

  const refresh = () => {
    void queryClient.invalidateQueries({ queryKey: ["llm-providers"] });
    void queryClient.invalidateQueries({ queryKey: ["llm-status"] });
  };
  const onError = (error: Error) => {
    message.error(error.message);
    refresh();   // 동시 변경으로 거절됐으면 최신 선택 상태로 다시 그린다.
  };

  const activateMutation = useMutation({
    mutationFn: activateLlmProvider,
    onSuccess: (res) => { message.success(`${res.name} 을(를) 선택했습니다.`); refresh(); },
    onError,
  });
  const deactivateMutation = useMutation({
    mutationFn: deactivateLlmProvider,
    onSuccess: () => { message.success("선택을 해제했습니다. GPT 기능이 화면에 표시되지 않습니다."); refresh(); },
    onError,
  });
  const deleteMutation = useMutation({
    mutationFn: deleteLlmProvider,
    onSuccess: () => { message.success("삭제했습니다."); refresh(); },
    onError,
  });
  const testMutation = useMutation({
    mutationFn: testSavedLlmProvider,
    onSuccess: (res) => {
      if (res.lastTestStatus === "SUCCESS") message.success(`${res.name}: ${res.lastTestMessage}`);
      else message.error(`${res.name}: ${res.lastTestMessage}`);
      refresh();
    },
    onError,
  });

  const select = (row: LlmProviderResponse) => {
    if (row.active) return;
    if (row.deployment === "EXTERNAL") {
      Modal.confirm({
        title: `${row.name} 을(를) 선택합니다`,
        content: (
          <div>
            <p>외부 제공자입니다. GPT 기능을 쓰면 job 의 SQL·테이블 정보와 오류 로그가 <b>{PROVIDER_LABEL[row.provider]}</b> 로 전송됩니다.</p>
            <p style={{ marginBottom: 0 }}>개인정보 마스킹: <b>{row.maskSensitive ? "켜짐" : "꺼짐"}</b></p>
          </div>
        ),
        okText: "선택",
        cancelText: "취소",
        onOk: () => activateMutation.mutateAsync(row.id),
      });
      return;
    }
    activateMutation.mutate(row.id);
  };

  return (
    <Card title="GPT 관리">
      <Space direction="vertical" style={{ width: "100%" }} size={16}>
        {active ? (
          <Alert
            type="info"
            showIcon
            message={<span>현재 선택: <b>{active.name}</b> ({DEPLOYMENT_LABEL[active.deployment]} · {PROVIDER_LABEL[active.provider]} · {active.modelName})</span>}
            action={(
              <Popconfirm
                title="선택을 해제할까요?"
                description="속성창 GPT 탭과 오류 분석 버튼이 화면에서 사라집니다."
                okText="해제"
                cancelText="취소"
                onConfirm={() => deactivateMutation.mutateAsync()}
              >
                <Button size="small" loading={deactivateMutation.isPending}>선택 해제</Button>
              </Popconfirm>
            )}
          />
        ) : (
          <Alert type="warning" showIcon message="선택된 GPT 가 없습니다. GPT 기능이 화면에 표시되지 않습니다." />
        )}

        <div style={{ display: "flex", justifyContent: "flex-end" }}>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => navigate("/gpt/register")}>등록</Button>
        </div>

        <Table<LlmProviderResponse>
          rowKey="id"
          size="middle"
          loading={isLoading}
          dataSource={providers}
          pagination={{ pageSize: 10, hideOnSinglePage: true, showTotal: (total) => `전체 ${total}건` }}
          scroll={{ x: 1100 }}
          locale={{ emptyText: "등록된 GPT 가 없습니다. 오른쪽 위 «등록»으로 추가하십시오." }}
          rowClassName={(row) => (row.active ? "ant-table-row-selected" : "")}
          columns={[
            {
              title: "선택",
              width: 64,
              align: "center",
              render: (_, row) => (
                <Radio checked={row.active} disabled={activateMutation.isPending} onChange={() => select(row)} />
              ),
            },
            { title: "이름", dataIndex: "name", render: (v: string) => <strong>{v}</strong> },
            {
              title: "배치",
              dataIndex: "deployment",
              width: 80,
              render: (v: LlmProviderResponse["deployment"]) => <Tag color={v === "EXTERNAL" ? "blue" : "green"}>{DEPLOYMENT_LABEL[v]}</Tag>,
            },
            { title: "제공자", dataIndex: "provider", width: 120, render: (v: LlmProviderResponse["provider"]) => PROVIDER_LABEL[v] },
            {
              title: "모델",
              dataIndex: "modelName",
              render: (v: string, row) => (
                <Tooltip title={row.effectiveBaseUrl ?? undefined}><span>{v}</span></Tooltip>
              ),
            },
            { title: "과금", dataIndex: "billingType", width: 70, render: (v: LlmProviderResponse["billingType"]) => BILLING_LABEL[v] },
            { title: "마스킹", dataIndex: "maskSensitive", width: 80, render: (v: boolean) => (v ? "켜짐" : "꺼짐") },
            { title: "키", width: 90, render: (_, row) => (row.apiKeySet ? row.apiKeyHint : "-") },
            {
              title: "최근 테스트",
              width: 190,
              render: (_, row) => {
                if (!row.lastTestStatus) return <span style={{ color: "#9ca3af" }}>테스트 안 함</span>;
                const ok = row.lastTestStatus === "SUCCESS";
                return (
                  <Tooltip title={row.lastTestMessage ?? undefined}>
                    <Space size={4}>
                      <Tag color={ok ? "success" : "error"}>{ok ? "성공" : "실패"}</Tag>
                      <span style={{ fontSize: 12 }}>{row.lastTestedAt ? dayjs(row.lastTestedAt).format("MM-DD HH:mm") : ""}</span>
                    </Space>
                  </Tooltip>
                );
              },
            },
            {
              title: "관리",
              width: 210,
              render: (_, row) => (
                <Space size={4}>
                  <Button
                    size="small"
                    loading={testMutation.isPending && testMutation.variables === row.id}
                    onClick={() => testMutation.mutate(row.id)}
                  >
                    테스트
                  </Button>
                  <Button size="small" onClick={() => navigate(`/gpt/register?id=${row.id}`)}>수정</Button>
                  {row.active ? (
                    <Tooltip title="선택된 항목은 삭제할 수 없습니다. 먼저 선택을 해제하십시오.">
                      <Button size="small" danger disabled>삭제</Button>
                    </Tooltip>
                  ) : (
                    <Popconfirm title={`${row.name} 을(를) 삭제할까요?`} okText="삭제" cancelText="취소"
                                okButtonProps={{ danger: true }} onConfirm={() => deleteMutation.mutateAsync(row.id)}>
                      <Button size="small" danger>삭제</Button>
                    </Popconfirm>
                  )}
                </Space>
              ),
            },
          ]}
        />
      </Space>
    </Card>
  );
}
