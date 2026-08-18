import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Card, Empty, Segmented, Tooltip } from "antd";
import { Column } from "@ant-design/plots";
import type { Dayjs } from "dayjs";
import { getDashboardTop5, type Top5Metric } from "../api/dashboard";

// 하단 Job Top 5 — 건수/소요시간/실패 구분. 상단 RangePicker(from~to) 날짜범위를 그대로 따른다.
const TOP5_TABS: Array<{ label: string; value: Top5Metric; unit: string; hint: string }> = [
  { label: "건수", value: "count", unit: "건", hint: "조회 기간 내 적재 건수 합계" },
  {
    label: "소요시간",
    value: "duration",
    unit: "초",
    hint: "실행 1회 평균 소요시간. 수집 주기가 15초라 ±15초 오차가 있습니다",
  },
  { label: "실패", value: "failure", unit: "회", hint: "조회 기간 내 실패 기록 수" },
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
        <>
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
                items: [{ field: "value", name: tab.label }],
              }}
            />
          </div>
          <ul className="trend-top5-list">
            {items.map((item) => (
              <li key={item.label}>
                <button
                  type="button"
                  disabled={item.job_id == null}
                  onClick={() => drillDown(item.job_id)}
                  title={item.job_id == null ? "이동할 화면을 특정할 수 없습니다" : "실행 이력으로 이동"}
                >
                  <span className="trend-top5-name">{item.label}</span>
                  <span className="trend-top5-value">
                    {item.value.toLocaleString()} {tab.unit}
                  </span>
                </button>
              </li>
            ))}
          </ul>
          <Tooltip title={tab.hint}>
            <div className="trend-top5-hint">ⓘ {tab.hint}</div>
          </Tooltip>
        </>
      ) : (
        <div className="empty-chart-placeholder">
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="조회 기간에 집계할 실행이 없습니다." />
        </div>
      )}
    </Card>
  );
}
