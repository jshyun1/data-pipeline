import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, Empty, Segmented, Space, Statistic } from "antd";
import { DualAxes } from "@ant-design/plots";
import dayjs from "dayjs";
import { getKpiSummary, getKpiTimeline, type KpiPreset } from "../api/kpi";

// 계열색: 대시보드 요약 카드와 동일 값(같은 엔진이면 화면 어디서나 같은 색).
const CDC_COLOR = "#2878d0";
const NIFI_COLOR = "#e5484d";

const PRESET_OPTIONS: Array<{ label: string; value: KpiPreset }> = [
  { label: "1시간", value: "1h" },
  { label: "24시간", value: "24h" },
  { label: "7일", value: "7d" },
  { label: "30일", value: "30d" },
];

// 롤업 bucket_start("2026-08-18 03:00:00.0") 를 프리셋 입도에 맞춰 축약한다.
function formatBucket(raw: string, granularity: string): string {
  const d = dayjs(raw.slice(0, 19).replace(" ", "T"));
  if (!d.isValid()) return raw;
  if (granularity === "DAY") return d.format("MM/DD");
  if (granularity === "MIN5") return d.format("HH:mm");
  return d.format("MM/DD HH:00");
}

interface TrendRow {
  time: string;
  loaded: number;
  obs: number;
}

export function KpiLoadTrend() {
  const [preset, setPreset] = useState<KpiPreset>("24h");

  const summaryQuery = useQuery({
    queryKey: ["kpi-summary", preset],
    queryFn: () => getKpiSummary(preset),
    refetchInterval: 30000,
    placeholderData: (prev) => prev,
  });
  const timelineQuery = useQuery({
    queryKey: ["kpi-timeline", preset],
    queryFn: () => getKpiTimeline(preset),
    refetchInterval: 30000,
    placeholderData: (prev) => prev,
  });

  const summary = summaryQuery.data ?? [];
  const totalLoaded = summary.reduce((acc, r) => acc + Number(r.loaded ?? 0), 0);
  const totalObs = summary.reduce((acc, r) => acc + Number(r.obs ?? 0), 0);

  // 소스별 버킷을 시각별 합계로 접어 2축(적재량·관측수) 하나의 시계열로 만든다.
  const trendRows = useMemo<TrendRow[]>(() => {
    const buckets = timelineQuery.data?.buckets ?? [];
    const gran = timelineQuery.data?.granularity ?? "HOUR";
    const byTime = new Map<string, TrendRow>();
    for (const b of buckets) {
      const time = formatBucket(b.bucketStart, gran);
      const cur = byTime.get(time) ?? { time, loaded: 0, obs: 0 };
      cur.loaded += Number(b.loadedCount ?? 0);
      cur.obs += Number(b.observationCount ?? 0);
      byTime.set(time, cur);
    }
    return Array.from(byTime.values());
  }, [timelineQuery.data]);

  const chartConfig = {
    height: 260,
    xField: "time",
    legend: false as const,
    children: [
      {
        data: trendRows,
        type: "interval",
        yField: "loaded",
        style: { fill: CDC_COLOR, maxWidth: 28 },
      },
      {
        data: trendRows,
        type: "line",
        yField: "obs",
        style: { stroke: NIFI_COLOR, lineWidth: 2 },
        axis: { y: { position: "right" } },
      },
    ],
  };

  const loading = summaryQuery.isLoading || timelineQuery.isLoading;

  return (
    <Card
      className="dashboard-panel"
      title="적재 KPI · 처리량·관측 2축"
      loading={loading && !summaryQuery.data}
      extra={
        <Segmented
          size="small"
          options={PRESET_OPTIONS}
          value={preset}
          onChange={(v) => setPreset(v as KpiPreset)}
        />
      }
    >
      <Space size="large" wrap style={{ marginBottom: 12 }}>
        <Statistic title="총 적재 건수" value={totalLoaded} groupSeparator="," suffix="건" />
        <Statistic title="관측 횟수" value={totalObs} groupSeparator="," suffix="회" />
        {summary.map((r) => (
          <Statistic
            key={r.pipeline_source}
            title={`${r.pipeline_source} 적재`}
            value={Number(r.loaded ?? 0)}
            groupSeparator=","
            suffix="건"
          />
        ))}
      </Space>
      {trendRows.length ? (
        <DualAxes {...chartConfig} />
      ) : (
        <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="집계된 적재 롤업이 없습니다" />
      )}
    </Card>
  );
}
