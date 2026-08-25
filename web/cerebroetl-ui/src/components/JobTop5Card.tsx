import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Card, Empty, Segmented } from "antd";
import { Column } from "@ant-design/plots";
import type { Dayjs } from "dayjs";
import { getDashboardTop5, type Top5Item, type Top5Metric } from "../api/dashboard";

// 하단 Job Top 5 — 건수/소요시간/실패 구분. 상단 RangePicker(from~to) 날짜범위를 그대로 따른다.
// 값은 차트와 툴팁으로만 보여준다(막대 옆 목록은 화면이 복잡해져 걷어냄).
const TOP5_TABS: Array<{ label: string; value: Top5Metric; unit: string }> = [
  { label: "건수", value: "count", unit: "건" },
  { label: "소요시간", value: "duration", unit: "초" },
  { label: "실패", value: "failure", unit: "회" },
];

export function JobTop5Card({ title, from, to }: { title: string; from: Dayjs; to: Dayjs }) {
  const navigate = useNavigate();
  const [metric, setMetric] = useState<Top5Metric>("count");
  const fromStr = from.format("YYYY-MM-DD");
  const toStr = to.format("YYYY-MM-DD");

  const q = useQuery({
    queryKey: ["job-top5", metric, fromStr, toStr],
    queryFn: () => getDashboardTop5(metric, { from: fromStr, to: toStr }),
    placeholderData: (prev) => prev,
  });

  const items = q.data?.items ?? [];
  const tab = TOP5_TABS.find((t) => t.value === metric)!;

  function drillDown(jobId: number | null) {
    if (jobId == null) return;
    navigate(`/etl/logs?jobId=${jobId}`);
  }

  return (
    <Card
      className="dashboard-panel dashboard-chart-card trend-top5-card"
      title={
        <div className="trend-top5-head">
          <span>{title}</span>
          <Segmented
            size="small"
            value={metric}
            options={TOP5_TABS.map((t) => ({ label: t.label, value: t.value }))}
            onChange={(v) => setMetric(v as Top5Metric)}
          />
        </div>
      }
      loading={q.isLoading && !q.data}
    >
      {items.length > 0 ? (
        <div className="chart-fit-shell">
            <Column
              autoFit
              data={items}
              xField="label"
              yField="value"
              style={{ maxWidth: 24, radiusTopLeft: 4, radiusTopRight: 4 }}
              axis={{ x: { labelAutoHide: false, labelAutoRotate: true }, y: { nice: true } }}
              tooltip={{
                title: (d: { label: string }) => d.label,
                // 목록을 없앤 뒤로 수치를 읽을 곳이 툴팁뿐이라 단위까지 붙인다.
                items: [
                  {
                    field: "value",
                    name: tab.label,
                    valueFormatter: (v: number) => `${v.toLocaleString()} ${tab.unit}`,
                  },
                ],
              }}
              // 목록을 없앴으므로 드릴다운은 막대 클릭으로 남긴다.
              onReady={({ chart }) => {
                chart.on("element:click", (ev: { data?: { data?: Top5Item } }) => {
                  drillDown(ev?.data?.data?.job_id ?? null);
                });
              }}
            />
        </div>
      ) : (
        <div className="empty-chart-placeholder">
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="조회 기간에 집계할 실행이 없습니다." />
        </div>
      )}
    </Card>
  );
}
