import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, DatePicker, Input, Space, Table, Tag } from "antd";
import dayjs, { type Dayjs } from "dayjs";
import { listNifiExecutionLogs, type NifiExecutionLogEntry } from "../api/platform";

const { RangePicker } = DatePicker;

const STATUS_LABEL: Record<string, string> = {
  SUCCESS: "성공",
  FAILED: "실패",
};

const STATUS_COLOR: Record<string, string> = {
  SUCCESS: "success",
  FAILED: "error",
};

function formatDateTime(value?: string) {
  if (!value) {
    return "-";
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

// NiFi에는 Airflow의 dag_run 같은 "실행 이력" 개념이 없다 - Provenance 조회는 이 환경에서
// 인덱스/이벤트파일 불일치로 구조적으로 안 되는 것으로 확인됐고(재시작/저장소 재구축 후에도
// 재현), 프로세서 단위 Status History도 항상 비어 있어 조회가 안 된다(그룹 단위는 되지만
// 그룹 안 여러 테이블이 섞여서 프로세서별 구분이 안 됨). 그래서 적재 프로세서(PutDatabaseRecord)의
// 누적 카운터가 60초 주기로 증가했는지를 백엔드가 감지해서 남긴 행을 그대로 보여준다 -
// "실행 1회 = 행 1개"가 정확히는 아니고 "60초 구간 안에 증가가 있었다 = 행 1개"에 가깝지만,
// 지금 파이프라인들의 실행 빈도상 대부분 실제 실행 횟수와 근접하게 나온다.
async function getEtlLogs(range: [Dayjs, Dayjs]): Promise<NifiExecutionLogEntry[]> {
  const entries = await listNifiExecutionLogs(
    range[0].format("YYYY-MM-DD"),
    range[1].format("YYYY-MM-DD"),
  );
  return [...entries].sort((a, b) => new Date(b.occurredAt).getTime() - new Date(a.occurredAt).getTime());
}

export function EtlLogsPage() {
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(6, "day"), dayjs()]);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(dateRange);
  const [nameFilter, setNameFilter] = useState("");

  const logsQuery = useQuery({
    queryKey: ["etl-logs", appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD")],
    queryFn: () => getEtlLogs(appliedRange),
    placeholderData: (previousData) => previousData,
  });

  const rows = logsQuery.data ?? [];
  const filteredRows = useMemo(() => {
    const keyword = nameFilter.trim().toLowerCase();
    if (!keyword) {
      return rows;
    }
    return rows.filter(
      (row) =>
        row.processorName.toLowerCase().includes(keyword) ||
        (row.groupName ?? "").toLowerCase().includes(keyword),
    );
  }, [rows, nameFilter]);

  const showInitialLoading = logsQuery.isLoading && !logsQuery.data;

  return (
    <div>
      <Card title="NiFi ETL 로그">
        <Space style={{ marginBottom: 16 }} wrap>
          <RangePicker
            value={dateRange}
            onChange={(value) => {
              if (value && value[0] && value[1]) {
                setDateRange([value[0], value[1]]);
              }
            }}
            allowClear={false}
          />
          <Input
            placeholder="프로세서/그룹 이름 검색"
            allowClear
            style={{ width: 220 }}
            value={nameFilter}
            onChange={(event) => setNameFilter(event.target.value)}
          />
          <Button type="primary" onClick={() => setAppliedRange(dateRange)}>
            조회
          </Button>
        </Space>
        <Table<NifiExecutionLogEntry>
          rowKey="id"
          size="small"
          loading={showInitialLoading}
          dataSource={filteredRows}
          pagination={{ pageSize: 20 }}
          columns={[
            { title: "프로세서", dataIndex: "processorName" },
            { title: "그룹(파이프라인)", dataIndex: "groupName", render: (value?: string) => value ?? "-" },
            {
              title: "상태",
              dataIndex: "status",
              width: 100,
              render: (value: string) => <Tag color={STATUS_COLOR[value] ?? "default"}>{STATUS_LABEL[value] ?? value}</Tag>,
            },
            { title: "실행 시각", dataIndex: "occurredAt", width: 190, render: formatDateTime },
            { title: "적재 건수", dataIndex: "insertedCount", width: 100, align: "right" },
          ]}
        />
      </Card>
    </div>
  );
}
