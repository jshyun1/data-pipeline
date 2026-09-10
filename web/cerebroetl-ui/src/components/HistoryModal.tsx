import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Empty, Modal, Table } from "antd";
import type { DagCategory } from "../utils/dagHistory";

export interface HistoryEntry {
  id: string;
  basicContent: string;
  category: DagCategory;
  datetime?: string;
  fetchLog?: () => Promise<string>;
}

const CATEGORY_CLASS: Record<DagCategory, string> = {
  ETL: "history-category-badge",
  CDC: "history-category-badge history-category-badge--cdc",
  기타: "history-category-badge history-category-badge--etc",
};

function formatDateTime(value?: string) {
  if (!value) {
    return "실행 이력 없음";
  }
  return new Intl.DateTimeFormat("ko-KR", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function LogPanel({ entry }: { entry: HistoryEntry }) {
  const logQuery = useQuery({
    queryKey: ["history-log", entry.id],
    queryFn: () => entry.fetchLog!(),
    enabled: Boolean(entry.fetchLog),
    staleTime: 60000,
  });

  return (
    <pre className="history-log-panel">
      {logQuery.isLoading ? "로그를 불러오는 중입니다..." : (logQuery.data ?? "로그를 불러올 수 없습니다.")}
    </pre>
  );
}

interface HistoryModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  loading?: boolean;
  rows: HistoryEntry[];
}

// DAG/태스크/성공/실패/지연 타일을 클릭했을 때 뜨는 공용 상세 내역 모달.
// 행마다 있는 "상세" 버튼은 팝업을 새로 띄우는 대신 antd Table의 expandedRowRender로
// 해당 행 바로 아래에 로그를 펼쳐 보여준다(로그는 펼칠 때 처음 한 번만 불러옴).
// 모양은 디자인 초안의 «ETL 오류 상세» 팝업(펼치면 빨간 접기 버튼 + 어두운 콘솔 상자)을 따른다.
export function HistoryModal({ open, onClose, title, loading, rows }: HistoryModalProps) {
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);

  return (
    <Modal
      open={open}
      onCancel={onClose}
      onOk={onClose}
      title={title}
      width={860}
      footer={null}
      destroyOnHidden
      rootClassName="cerebro-modal history-modal"
    >
      {/* 오류 메시지가 공백 없는 한 줄(SQL·스택트레이스)이라 기본 레이아웃에서는 열이 계속
          넓어져 표가 모달 밖으로 삐져나갔다. tableLayout=fixed 로 폭을 못박고 셀 안에서
          줄바꿈시킨다. */}
      <Table<HistoryEntry>
        rowKey="id"
        size="small"
        tableLayout="fixed"
        loading={loading}
        dataSource={rows}
        pagination={{ pageSize: 10 }}
        locale={{ emptyText: <Empty description="내역이 없습니다." /> }}
        expandable={{
          expandedRowKeys: expandedKeys,
          rowExpandable: (record) => Boolean(record.fetchLog),
          onExpand: (expanded, record) => {
            setExpandedKeys((previous) =>
              expanded ? [...previous, record.id] : previous.filter((key) => key !== record.id),
            );
          },
          expandedRowRender: (record) => <LogPanel entry={record} />,
          expandIcon: ({ expanded, onExpand, record }) =>
            record.fetchLog ? (
              <button
                type="button"
                className={expanded ? "btn-etl-detail-toggle active" : "btn-etl-detail-toggle"}
                onClick={(event) => onExpand(record, event)}
              >
                {expanded ? "접기" : "상세"}
              </button>
            ) : null,
        }}
        columns={[
          {
            title: "기본 내용",
            dataIndex: "basicContent",
            render: (value: string) => (
              <div style={{ whiteSpace: "pre-wrap", wordBreak: "break-word", overflowWrap: "anywhere" }}>
                {value}
              </div>
            ),
          },
          {
            title: "구분",
            dataIndex: "category",
            width: 90,
            align: "center",
            render: (value: DagCategory) => <span className={CATEGORY_CLASS[value]}>{value}</span>,
          },
          { title: "일시", dataIndex: "datetime", width: 190, render: formatDateTime },
        ]}
      />
    </Modal>
  );
}
