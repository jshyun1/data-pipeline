import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Empty, Modal, Table } from "antd";
import { GptErrorAnalysis } from "./gpt/GptErrorAnalysis";
import { useGptEnabled } from "./gpt/useGptEnabled";
import type { LlmTargetType } from "../api/llm";

/**
 * 대시보드에서 «장애 N건 · 오류 그룹 N건»을 눌렀을 때의 실패 상세.
 *
 * <p>모양과 순서를 <b>각 로그 화면의 실패 줄과 같게</b> 맞춘다(2026-09-21 요청) - 줄을 펼치면
 * 항목(이름 · 대상 · 상태…) → 오류 원문 → [GPT 해석] 이다. 예전 대시보드는 원문만 보여 줘서
 * «이게 어느 파이프라인의 무슨 오류인지»를 로그 화면에서 다시 찾아야 했다.
 *
 * <p>GPT 대상(targetType · target)은 부르는 쪽이 정한다 - CDC 는 CDC_PIPELINE, ETL 은 ETL_JOB 으로
 * 로그 화면과 같은 정보가 모이게 한다.
 */
export interface FailureDetailItem {
  id: string;
  /** 표에 보이는 한 줄 요약(로그 화면의 «대상» 칸에 해당). */
  title: string;
  datetime?: string;
  /** 펼쳤을 때 위에 적는 항목들. 로그 화면과 같은 «이름 / 값» 두 칸 형식이다. */
  lines: Array<[string, string]>;
  /** 오류 원문. 트레이스처럼 길 수 있어 펼칠 때 한 번만 불러온다. */
  loadError: () => Promise<string>;
  /** GPT 해석 대상. 없으면 버튼을 그리지 않는다(수집기가 없는 대상). */
  gpt?: { targetType: LlmTargetType; target: Record<string, string> };
}

interface Props {
  open: boolean;
  title: string;
  loading?: boolean;
  items: FailureDetailItem[];
  /**
   * 줄마다 붙는 버튼(워크플로우 실패의 [로그]). 워크플로우는 한 실행 안에 실패 task 가 여럿이고 시도별 로그 ·
   * 하위 워크플로우까지 따라가야 해서, 펼친 줄 대신 전용 창으로 보낸다(2026-09-28).
   */
  rowAction?: { label: string; busyId?: string; onClick: (item: FailureDetailItem) => void };
  onClose: () => void;
}

export function FailureDetailModal({ open, title, loading, items, rowAction, onClose }: Props) {
  const [expanded, setExpanded] = useState<string[]>([]);

  return (
    <Modal
      open={open}
      onCancel={onClose}
      onOk={onClose}
      title={title}
      width={880}
      footer={null}
      destroyOnHidden
      rootClassName="cerebro-modal history-modal"
    >
      <Table<FailureDetailItem>
        rowKey="id"
        size="small"
        tableLayout="fixed"
        loading={loading}
        dataSource={items}
        pagination={{ pageSize: 10, hideOnSinglePage: true }}
        locale={{ emptyText: <Empty description="실패 내역이 없습니다." /> }}
        expandable={{
          expandedRowKeys: expanded,
          onExpand: (isOpen, record) =>
            setExpanded((prev) => (isOpen ? [...prev, record.id] : prev.filter((key) => key !== record.id))),
          expandedRowRender: (record) => <FailureBody item={record} />,
        }}
        columns={[
          { title: "대상", dataIndex: "title", ellipsis: true },
          { title: "시각", dataIndex: "datetime", width: 170, render: (v?: string) => formatTime(v) },
          ...(rowAction
            ? [{
                title: "",
                key: "action",
                width: 72,
                render: (_: unknown, record: FailureDetailItem) => (
                  <Button size="small" loading={rowAction.busyId === record.id}
                          onClick={(event) => { event.stopPropagation(); rowAction.onClick(record); }}>
                    {rowAction.label}
                  </Button>
                ),
              }]
            : []),
        ]}
      />
    </Modal>
  );
}

/** 로그 화면의 펼친 줄과 같은 차례 - 항목들 → 오류 원문 → GPT 해석. */
function FailureBody({ item }: { item: FailureDetailItem }) {
  const gptEnabled = useGptEnabled();
  const [analyzing, setAnalyzing] = useState(false);
  const errorQuery = useQuery({
    queryKey: ["dashboard-failure-detail", item.id],
    queryFn: item.loadError,
    staleTime: 60_000,
  });
  const errorText = errorQuery.data ?? "";

  return (
    <div style={{ fontSize: 12 }}>
      {item.lines.map(([label, value]) => (
        <div key={label} style={{ display: "flex", gap: 12, lineHeight: 1.8 }}>
          <span style={{ width: 110, color: "#888", flexShrink: 0 }}>{label}</span>
          <span style={{ wordBreak: "break-all" }}>{value}</span>
        </div>
      ))}
      <pre style={{ whiteSpace: "pre-wrap", margin: "6px 0 0", fontSize: 12 }}>
        {errorQuery.isLoading ? "오류 내용을 불러오는 중입니다..." : errorText || "오류 내용을 찾을 수 없습니다."}
      </pre>
      {item.gpt && gptEnabled && !analyzing && (
        <Button size="small" className="gpt-analysis-open" style={{ marginTop: 8 }}
                disabled={errorQuery.isLoading}
                onClick={() => setAnalyzing(true)}>
          GPT 해석
        </Button>
      )}
      {item.gpt && analyzing && (
        <GptErrorAnalysis
          targetType={item.gpt.targetType}
          target={item.gpt.target}
          errorText={[item.title, ...item.lines.map(([l, v]) => `${l}: ${v}`), errorText].join("\n")}
        />
      )}
    </div>
  );
}

function formatTime(value?: string) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : new Intl.DateTimeFormat("ko-KR", {
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  }).format(parsed);
}
