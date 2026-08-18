import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Card, Empty, Segmented, Space, Tooltip } from "antd";
import { Column, Line } from "@ant-design/plots";
import {
  getDashboardTop5,
  getDashboardTrends,
  type TrendPreset,
  type Top5Metric,
} from "../api/dashboard";

/**
 * 차트 영역 보강 — 원본 문서 5-5.
 *
 * <p>원본 지적 4가지를 여기서 채운다:
 * ① 기본 기간을 24시간으로 내리고 빠른 선택 칩(1h/24h/7d/30d)을 둔다,
 * ② 자동 갱신 토글(off/30초/60초),
 * ④ 실패 건수 추이와 지연 추이 — "이상 감지에 더 유용하다",
 * ⑤⑥ Top 5 드릴다운 + 소요시간·실패 Top 5.
 */

const PRESETS: Array<{ label: string; value: TrendPreset }> = [
  { label: "1시간", value: "1h" },
  { label: "24시간", value: "24h" },
  { label: "7일", value: "7d" },
  { label: "30일", value: "30d" },
];

/** 원본 5-5 "자동 갱신 토글 (off / 30초 / 60초)". */
const REFRESH_OPTIONS = [
  { label: "수동", value: 0 },
  { label: "30초", value: 30000 },
  { label: "60초", value: 60000 },
];

const TOP5_TABS: Array<{ label: string; value: Top5Metric; unit: string; hint: string }> = [
  { label: "건수", value: "count", unit: "건", hint: "기간 내 적재 건수 합계" },
  {
    label: "소요시간",
    value: "duration",
    unit: "초",
    hint: "실행 1회 평균 소요시간. 수집 주기가 15초라 ±15초 오차가 있습니다",
  },
  { label: "실패", value: "failure", unit: "회", hint: "기간 내 실패 기록 수" },
];

const FAIL_COLOR = "#e5484d";
const LAG_COLOR = "#d98b00";

export function TrendCharts() {
  const navigate = useNavigate();
  // 원본 5-5 ① "기본 기간 7일 → 기본 24시간".
  const [preset, setPreset] = useState<TrendPreset>("24h");
  const [refreshMs, setRefreshMs] = useState<number>(30000);
  const [top5Metric, setTop5Metric] = useState<Top5Metric>("count");

  const trendsQuery = useQuery({
    queryKey: ["dashboard-trends", preset],
    queryFn: () => getDashboardTrends(preset),
    refetchInterval: refreshMs === 0 ? false : refreshMs,
    placeholderData: (prev) => prev,
  });

  const top5Query = useQuery({
    queryKey: ["dashboard-top5", top5Metric, preset],
    queryFn: () => getDashboardTop5(top5Metric, preset),
    refetchInterval: refreshMs === 0 ? false : refreshMs,
    placeholderData: (prev) => prev,
  });

  const failures = trendsQuery.data?.failures ?? [];
  const lag = trendsQuery.data?.lag ?? [];
  const top5 = top5Query.data?.items ?? [];
  const activeTab = TOP5_TABS.find((t) => t.value === top5Metric)!;

  // 원본 5-5 ⑤ "막대 클릭 → 해당 작업 실행 이력으로 이동".
  // job_id 가 없으면 어디로 보낼지 알 수 없으므로 클릭을 아예 걸지 않는다.
  function drillDown(jobId: number | null) {
    if (jobId == null) return;
    navigate(`/etl/logs?jobId=${jobId}`);
  }

  return (
    <section className="trend-charts">
      <header className="trend-charts-toolbar">
        <Space size="small" wrap>
          <span className="trend-charts-label">기간</span>
          <Segmented
            size="small"
            value={preset}
            options={PRESETS}
            onChange={(v) => setPreset(v as TrendPreset)}
          />
        </Space>
        <Space size="small" wrap>
          <span className="trend-charts-label">자동 갱신</span>
          <Segmented
            size="small"
            value={refreshMs}
            options={REFRESH_OPTIONS}
            onChange={(v) => setRefreshMs(Number(v))}
          />
        </Space>
      </header>

      <div className="trend-charts-grid">
        <Card
          className="dashboard-panel dashboard-chart-card"
          title="실패 건수 추이"
          loading={trendsQuery.isLoading && !trendsQuery.data}
        >
          {failures.some((p) => p.count > 0) ? (
            <div className="chart-fit-shell">
              <Column
                autoFit
                data={failures}
                xField="at"
                yField="count"
                style={{ fill: FAIL_COLOR, maxWidth: 20, radiusTopLeft: 3, radiusTopRight: 3 }}
                axis={{ x: { labelAutoHide: true, labelAutoRotate: false }, y: { nice: true } }}
                tooltip={{ title: (d: { at: string }) => d.at, items: [{ field: "count", name: "실패" }] }}
              />
            </div>
          ) : (
            <div className="empty-chart-placeholder">
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="이 기간에 기록된 실패가 없습니다." />
            </div>
          )}
        </Card>

        <Card
          className="dashboard-panel dashboard-chart-card"
          title="지연(미처리) 추이"
          loading={trendsQuery.isLoading && !trendsQuery.data}
        >
          {lag.some((p) => p.count > 0) ? (
            <div className="chart-fit-shell">
              <Line
                autoFit
                data={lag}
                xField="at"
                yField="count"
                style={{ stroke: LAG_COLOR, lineWidth: 2 }}
                axis={{ x: { labelAutoHide: true, labelAutoRotate: false }, y: { nice: true } }}
                tooltip={{ title: (d: { at: string }) => d.at, items: [{ field: "count", name: "최대 미처리" }] }}
              />
            </div>
          ) : (
            <div className="empty-chart-placeholder">
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="이 기간에 미처리가 관측되지 않았습니다." />
            </div>
          )}
        </Card>

        <Card
          className="dashboard-panel dashboard-chart-card trend-top5-card"
          title={
            <div className="trend-top5-head">
              <span>Top 5</span>
              <Segmented
                size="small"
                value={top5Metric}
                options={TOP5_TABS.map((t) => ({ label: t.label, value: t.value }))}
                onChange={(v) => setTop5Metric(v as Top5Metric)}
              />
            </div>
          }
          loading={top5Query.isLoading && !top5Query.data}
        >
          {top5.length > 0 ? (
            <>
              <div className="chart-fit-shell">
                <Column
                  autoFit
                  data={top5}
                  xField="label"
                  yField="value"
                  style={{ maxWidth: 24, radiusTopLeft: 4, radiusTopRight: 4 }}
                  axis={{ x: { labelAutoHide: false, labelAutoRotate: true }, y: { nice: true } }}
                  tooltip={{
                    title: (d: { label: string }) => d.label,
                    items: [{ field: "value", name: activeTab.label }],
                  }}
                  onReady={(ready: unknown) => {
                    // 막대 클릭 드릴다운은 "있으면 좋은" 경로다. plots 버전에 따라 onReady 시그니처가
                    // 달라질 수 있으므로, 못 붙이면 조용히 포기하고 아래 목록 버튼으로 대신한다 —
                    // 차트 하나 때문에 대시보드 전체가 죽는 쪽이 훨씬 나쁘다.
                    const chart = (ready as { chart?: { on?: unknown } })?.chart;
                    if (!chart || typeof chart.on !== "function") return;
                    try {
                      (chart.on as (e: string, cb: (evt: unknown) => void) => void)(
                        "element:click",
                        (evt: unknown) => {
                          const data = (evt as { data?: { data?: { job_id?: number | null } } })?.data?.data;
                          drillDown(data?.job_id ?? null);
                        },
                      );
                    } catch {
                      /* 클릭 바인딩 실패는 무시한다(목록 버튼이 같은 동작을 제공한다). */
                    }
                  }}
                />
              </div>
              <ul className="trend-top5-list">
                {top5.map((item) => (
                  <li key={item.label}>
                    <button
                      type="button"
                      disabled={item.job_id == null}
                      onClick={() => drillDown(item.job_id)}
                      title={item.job_id == null ? "이동할 화면을 특정할 수 없습니다" : "실행 이력으로 이동"}
                    >
                      <span className="trend-top5-name">{item.label}</span>
                      <span className="trend-top5-value">
                        {item.value.toLocaleString()} {activeTab.unit}
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
              <Tooltip title={activeTab.hint}>
                <div className="trend-top5-hint">ⓘ {activeTab.hint}</div>
              </Tooltip>
            </>
          ) : (
            <div className="empty-chart-placeholder">
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="이 기간에 집계할 실행이 없습니다." />
            </div>
          )}
        </Card>
      </div>
    </section>
  );
}
