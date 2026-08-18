import { Card, Tag, Tooltip } from "antd";
import { useQuery } from "@tanstack/react-query";
import {
  getDiskBreakdown,
  getResourceTimeseries,
  type HostResourceResponse,
  type ProcessGroup,
} from "../api/infra";
import { Sparkline, type SparkPoint } from "./Sparkline";
import { ProcessHealthPanel } from "./ProcessHealthPanel";

interface InfraRegionProps {
  resources?: HostResourceResponse;
  resourcesLoading: boolean;
  processes?: ProcessGroup[];
  processesLoading: boolean;
  /** KPI 카드가 내린 데이터 흐름 판정을 그대로 받아 프로세스 헬스와 축을 나눠 보여준다. */
  dataFlow?: { tone: "ok" | "warn" | "unknown"; text: string };
}

const WARN_PERCENT = 80;
const DANGER_PERCENT = 90;

type Level = "ok" | "warn" | "danger";

function meterLevel(percent: number): Level {
  if (percent >= DANGER_PERCENT) {
    return "danger";
  }
  return percent >= WARN_PERCENT ? "warn" : "ok";
}

const LEVEL_BADGE: Record<Level, { text: string; color: string } | null> = {
  ok: { text: "정상", color: "success" },
  warn: { text: "⚠ 경고", color: "warning" },
  danger: { text: "🔴 위험", color: "error" },
};

function formatBytes(bytes: number) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(value >= 100 || unitIndex === 0 ? 0 : 1)}${units[unitIndex]}`;
}

function formatUptime(seconds: number) {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) {
    return `${days}일 ${hours}시간`;
  }
  return hours > 0 ? `${hours}시간 ${minutes}분` : `${minutes}분`;
}

interface ResourceMeterProps {
  title: string;
  percent: number | null;
  headline: string;
  caption: string;
  loading: boolean;
  /** 최근 1시간 추이(원본 5-3 #11). 표본이 2개 미만이면 컴포넌트가 알아서 "—"로 그린다. */
  spark?: SparkPoint[];
}

/**
 * 값 하나를 보여주는 자리라 차트를 쓰지 않고 수치 + 막대 하나로 둔다. 색이 경고를
 * 담당하되 퍼센트 숫자를 항상 같이 적어서 색 없이도 읽힌다.
 *
 * <p>원본 5-3 요구 3가지를 여기서 채운다 — 임계 판정 배지, 스파크라인, 그리고 임계를 넘었을 때
 * 조치 대기열로 보내는 링크(원본 표현: "색상 + 배지 + 조치 대기열 연동").
 */
function ResourceMeter({ title, percent, headline, caption, loading, spark }: ResourceMeterProps) {
  const level: Level = percent === null ? "ok" : meterLevel(percent);
  const badge = percent === null ? null : LEVEL_BADGE[level];
  return (
    <Card className="infra-tile" loading={loading}>
      <div className="infra-tile-title">
        <span>{title}</span>
        {badge ? <Tag color={badge.color}>{badge.text}</Tag> : null}
      </div>
      {percent === null ? (
        <div className="infra-tile-unavailable">측정 불가</div>
      ) : (
        <>
          <div className="infra-tile-headrow">
            <div className={`infra-tile-value infra-tile-value--${level}`}>
              {percent.toFixed(1)}
              <small>%</small>
            </div>
            {spark ? (
              <Tooltip title="최근 1시간 추이 · 수집이 끊긴 구간은 선을 끊습니다">
                <span className="infra-tile-spark">
                  <Sparkline points={spark} tone={level} />
                </span>
              </Tooltip>
            ) : null}
          </div>
          <div className="infra-meter" role="img" aria-label={`${title} ${percent.toFixed(1)}%`}>
            <span
              className={`infra-meter-fill infra-meter-fill--${level}`}
              style={{ width: `${Math.min(100, percent)}%` }}
            />
          </div>
          <div className="infra-tile-headline">{headline}</div>
          {level !== "ok" ? (
            <a className="infra-tile-queue-link" href="#action-queue">
              조치 대기열 보기 →
            </a>
          ) : null}
        </>
      )}
      <div className="infra-tile-caption">{caption}</div>
    </Card>
  );
}

/**
 * 디스크 용도별 분해 — 원본 5-3 "#12 루트 단일 값 → Kafka / NiFi / 컨테이너로 분리".
 *
 * <p>"컨테이너·이미지" 항목은 제공하지 않는다. 그 값을 재려면 /var/lib/docker 를 마운트해야 하는데
 * 그건 루트 권한 등가라서(시크릿 평문 포함) 관측 목적으로 열 수 없다. 대신 관측 가능한
 * Kafka·NiFi 볼륨만 읽기전용으로 재고, 마운트가 없으면 이 구역 자체를 감춘다.
 */
function DiskBreakdownList() {
  const { data } = useQuery({
    queryKey: ["infra-disk-breakdown"],
    queryFn: getDiskBreakdown,
    // 서버가 5분 주기로 계산한 캐시를 읽을 뿐이라 자주 물어볼 이유가 없다.
    refetchInterval: 300000,
    placeholderData: (prev) => prev,
  });

  const entries = (data?.entries ?? []).filter((e) => e.error == null);
  if (entries.length === 0) {
    return null;
  }
  const apparent = entries.some((e) => e.measuredBy === "APPARENT");
  return (
    <div className="infra-disk-breakdown">
      <div className="infra-disk-breakdown-title">용도별</div>
      {entries.map((e) => (
        <div key={e.path} className="infra-disk-breakdown-row">
          <span className="infra-disk-breakdown-label">└ {e.label}</span>
          <span className="infra-disk-breakdown-value">
            {formatBytes(e.usedBytes)}
            {e.isLowerBound ? "+" : ""}
          </span>
        </div>
      ))}
      {apparent ? (
        <div className="infra-disk-breakdown-note">
          일부 항목은 파일 길이 합계라 실제 점유량보다 클 수 있습니다.
        </div>
      ) : null}
    </div>
  );
}

/** 대시보드 오른쪽 인프라 열: 서버 리소스 3종 + CDC/NiFi/Airflow 주요 프로세스 현황. */
export function InfraRegion({ resources, resourcesLoading, processes, processesLoading, dataFlow }: InfraRegionProps) {
  const cpu = resources?.cpu ?? null;
  const memory = resources?.memory ?? null;
  const disk = resources?.disks?.[0] ?? null;
  const system = resources?.system ?? null;

  // 원본 5-3 #11 스파크라인용 최근 1시간 표본. 1분 간격이라 60점 안쪽이다.
  const { data: series } = useQuery({
    queryKey: ["infra-resource-timeseries"],
    queryFn: () => getResourceTimeseries(60),
    refetchInterval: 60000,
    placeholderData: (prev) => prev,
  });

  function sparkOf(key: string): SparkPoint[] | undefined {
    const points = series?.[key];
    if (!points || points.length === 0) return undefined;
    return points.map((p) => ({ at: p.at, value: p.usedPercent }));
  }

  const cpuHeadline = cpu ? `${cpu.cores} 코어` : "-";
  // 원본 5-3 "가동 시간 맥락: 최근 재시작 이력 표시".
  const uptimeText = system ? `가동 ${formatUptime(system.uptimeSeconds)}` : null;

  return (
    <aside className="dashboard-infra-column">
      {uptimeText ? (
        <div className="infra-column-heading">
          <span>시스템 리소스</span>
          <span className="infra-column-uptime">{uptimeText}</span>
        </div>
      ) : null}
      <ResourceMeter
        title="CPU 사용률"
        percent={cpu ? cpu.usedPercent : null}
        headline={cpuHeadline}
        caption="서버(호스트) 기준"
        loading={resourcesLoading}
        spark={sparkOf("CPU")}
      />
      <ResourceMeter
        title="메모리 사용률"
        percent={memory ? memory.usedPercent : null}
        headline={memory ? `${formatBytes(memory.usedBytes)} / ${formatBytes(memory.totalBytes)}` : "-"}
        caption={memory ? `여유 ${formatBytes(memory.availableBytes)}` : "서버(호스트) 기준"}
        loading={resourcesLoading}
        spark={sparkOf("MEMORY")}
      />
      <Card className="infra-tile infra-tile--disk" loading={resourcesLoading}>
        <ResourceMeter
          title="디스크 사용률"
          percent={disk ? disk.usedPercent : null}
          headline={disk ? `${formatBytes(disk.usedBytes)} / ${formatBytes(disk.totalBytes)}` : "-"}
          caption={disk ? `${disk.mount} · 여유 ${formatBytes(disk.availableBytes)}` : "마운트 조회 불가"}
          loading={false}
          spark={disk ? sparkOf(`DISK:${disk.mount}`) : undefined}
        />
        <DiskBreakdownList />
      </Card>

      <ProcessHealthPanel groups={processes} loading={processesLoading} dataFlow={dataFlow} />
    </aside>
  );
}
