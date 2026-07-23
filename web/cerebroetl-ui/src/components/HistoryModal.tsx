import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Empty, Modal, Table, Tag } from "antd";
import type { DagCategory } from "../utils/dagHistory";

export interface HistoryEntry {
  id: string;
  basicContent: string;
  category: DagCategory;
  datetime?: string;
  fetchLog?: () => Promise<string>;
}

const CATEGORY_COLOR: Record<DagCategory, string> = {
  ETL: "blue",
  CDC: "purple",
  기타: "default",
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
export function HistoryModal({ open, onClose, title, loading, rows }: HistoryModalProps) {
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);

  return (
    <Modal open={open} onCancel={onClose} onOk={onClose} title={title} width={860} footer={null} destroyOnHidden>
      <Table<HistoryEntry>
        rowKey="id"
        size="small"
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
              <Button size="small" onClick={(event) => onExpand(record, event)}>
                {expanded ? "접기" : "상세"}
              </Button>
            ) : null,
        }}
        columns={[
          { title: "기본 내용", dataIndex: "basicContent" },
          {
            title: "구분",
            dataIndex: "category",
            width: 90,
            render: (value: DagCategory) => <Tag color={CATEGORY_COLOR[value]}>{value}</Tag>,
          },
          { title: "일시", dataIndex: "datetime", width: 190, render: formatDateTime },
        ]}
      />
    </Modal>
  );
}
