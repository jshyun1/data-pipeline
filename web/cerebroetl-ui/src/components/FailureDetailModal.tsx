import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Empty, Modal, Table } from "antd";
import { GptErrorAnalysis } from "./gpt/GptErrorAnalysis";
import { useGptEnabled } from "./gpt/useGptEnabled";
import type { LlmTargetType } from "../api/llm";

/**
 * 대시보드에서 «실패 N건»을 눌렀을 때의 실패 상세.
 *
 * <p>모양과 순서를 <b>각 로그 화면과 같게</b> 맞춘다(2026-09-21 · 2026-10-13 요청) - 표는
 * «대상 · 구분 · 시각», 줄을 펼치면 <b>GPT 해석이 먼저</b> 나오고 그 아래에 «에러 로그» 상자가 온다.
 * CDC 로그 화면의 상세 창과 같은 차례다. 예전에는 항목 → 원문 → [GPT 해석] 단추 순서라
 * 실패를 볼 때마다 한 번 더 눌러야 했다.
 *
 * <p>GPT 대상(targetType · target)은 부르는 쪽이 정한다 - CDC 는 CDC_PIPELINE, ETL 은 ETL_JOB 으로
 * 로그 화면과 같은 정보가 모이게 한다.
 */
export interface FailureDetailItem {
  id: string;
  /** 표에 보이는 한 줄 요약(로그 화면의 «대상» 칸에 해당). */
  title: string;
  /** 로그 화면의 «구분» 칸(CDC: 처리 · 런타임 감시 …). 한 줄도 없으면 그 열을 그리지 않는다. */
  category?: string;
  datetime?: string;
  /** 화면에는 안 보이고 GPT 에만 같이 보내는 «이름 / 값» 항목들(소스 → 타깃 · DAG 등). */
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
  onClose: () => void;
}

export function FailureDetailModal({ open, title, loading, items, onClose }: Props) {
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
      // 맨 위 강조 띠(accent-modal) - 로그 창 셋과 같은 모양(2026-10-13 요청).
      rootClassName="cerebro-modal history-modal accent-modal"
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
          ...(items.some((item) => item.category)
            ? [{ title: "구분", dataIndex: "category", width: 140, ellipsis: true,
                 render: (value?: string) => value ?? "-" }]
            : []),
          { title: "시각", dataIndex: "datetime", width: 170, render: (v?: string) => formatTime(v) },
        ]}
      />
    </Modal>
  );
}

/** 로그 화면의 상세 창과 같은 차례 - GPT 해석 → 에러 로그. 표에 있는 값은 다시 적지 않는다. */
function FailureBody({ item }: { item: FailureDetailItem }) {
  const gptEnabled = useGptEnabled();
  // CDC 로그 상세 창처럼 펼치자마자 해석을 시작한다(2026-10-13 요청). 해석 창에 «닫기»가 없어진 뒤로
  // (2026-10-13, 로그 화면과 함께) 접었다 펴는 상태가 없다 - 줄을 접으면 그대로 사라진다.
  const errorQuery = useQuery({
    queryKey: ["dashboard-failure-detail", item.id],
    queryFn: item.loadError,
    staleTime: 60_000,
  });
  const errorText = errorQuery.data ?? "";
  const analyzable = Boolean(item.gpt) && gptEnabled;

  return (
    <section className="failure-log-task">
      {/* 오류 원문을 다 받은 뒤에 연다 - GptErrorAnalysis 는 뜨자마자 보내므로 먼저 열면 빈 내용으로 묻는다. */}
      {analyzable && !errorQuery.isLoading && (
        <GptErrorAnalysis
          targetType={item.gpt!.targetType}
          target={item.gpt!.target}
          errorText={[
            item.title,
            ...(item.category ? [`구분: ${item.category}`] : []),
            ...item.lines.map(([label, value]) => `${label}: ${value}`),
            errorText,
          ].join("\n")}
        />
      )}
      {/* «항목» 상자는 뺐다(2026-10-13 요청) - 대상 · 구분 · 시각이 표에 이미 있어 같은 말이 두 번 나왔다.
          lines 는 화면에 적지 않고 GPT 에 보내는 대상 설명으로만 쓴다. */}
      <div className="failure-log-block">
        <div className="failure-log-label">에러 로그</div>
        {errorQuery.isLoading
          ? <div className="failure-log-empty">오류 내용을 불러오는 중입니다...</div>
          : errorText
            ? <pre className="failure-log-exception">{errorText}</pre>
            : <div className="failure-log-empty">오류 내용을 찾을 수 없습니다.</div>}
      </div>
    </section>
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
