import { Card, Tag } from "antd";
import type {
  HostResourceResponse,
  ProcessGroup,
  ProcessItem,
  ProcessStatus,
} from "../api/infra";

interface InfraRegionProps {
  resources?: HostResourceResponse;
  resourcesLoading: boolean;
  processes?: ProcessGroup[];
  processesLoading: boolean;
}

// 상태색은 계열색(NiFi 빨강 / CDC 파랑)과 섞이면 안 되는 예약색이라 항상 글자 라벨과
// 같이 쓴다(색만으로 상태를 알려주지 않는다).
const STATUS_LABEL: Record<ProcessStatus, string> = {
  UP: "정상",
  STOPPED: "중지",
  DEGRADED: "경고",
  DOWN: "중단",
  UNKNOWN: "확인 불가",
};

// STOPPED는 일부러 멈춘 상태라 경고색을 쓰지 않는다(빨강/주황을 남발하면 아무도 안 본다).
const STATUS_TAG_COLOR: Record<ProcessStatus, string> = {
  UP: "success",
  STOPPED: "default",
  DEGRADED: "warning",
  DOWN: "error",
  UNKNOWN: "default",
};

/** 백엔드가 내려주는 그룹 순서와 같게 둔다(로딩 중 자리만 잡아주는 용도). */
const PROCESS_GROUP_PLACEHOLDERS = ["CDC", "ETL", "Airflow"];

const WARN_PERCENT = 70;
const DANGER_PERCENT = 85;

function meterLevel(percent: number): "ok" | "warn" | "danger" {
  if (percent >= DANGER_PERCENT) {
    return "danger";
  }
  return percent >= WARN_PERCENT ? "warn" : "ok";
}

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
}

/**
 * 값 하나를 보여주는 자리라 차트를 쓰지 않고 수치 + 막대 하나로 둔다. 색이 경고를
 * 담당하되 퍼센트 숫자를 항상 같이 적어서 색 없이도 읽힌다.
 */
function ResourceMeter({ title, percent, headline, caption, loading }: ResourceMeterProps) {
  const level = percent === null ? "ok" : meterLevel(percent);
  return (
    <Card className="infra-tile" loading={loading}>
      <div className="infra-tile-title">{title}</div>
      {percent === null ? (
        <div className="infra-tile-unavailable">측정 불가</div>
      ) : (
        <>
          <div className={`infra-tile-value infra-tile-value--${level}`}>
            {percent.toFixed(1)}
            <small>%</small>
          </div>
          <div className="infra-meter" role="img" aria-label={`${title} ${percent.toFixed(1)}%`}>
            <span
              className={`infra-meter-fill infra-meter-fill--${level}`}
              style={{ width: `${Math.min(100, percent)}%` }}
            />
          </div>
          <div className="infra-tile-headline">{headline}</div>
        </>
      )}
      <div className="infra-tile-caption">{caption}</div>
    </Card>
  );
}

function ProcessRow({ process }: { process: ProcessItem }) {
  return (
    <div className="infra-process-row">
      <span className="infra-process-name">{process.name}</span>
      <span className="infra-process-detail">{process.detail ?? "-"}</span>
      <Tag color={STATUS_TAG_COLOR[process.status]}>{STATUS_LABEL[process.status]}</Tag>
    </div>
  );
}

/** 대시보드 오른쪽 인프라 열: 서버 리소스 3종 + CDC/NiFi/Airflow 주요 프로세스 현황. */
export function InfraRegion({ resources, resourcesLoading, processes, processesLoading }: InfraRegionProps) {
  const cpu = resources?.cpu ?? null;
  const memory = resources?.memory ?? null;
  const disk = resources?.disks?.[0] ?? null;
  const system = resources?.system ?? null;

  const cpuHeadline = cpu ? `${cpu.cores} 코어` : "-";
  const cpuCaption = system
    ? `서버(호스트) 기준 · 가동 ${formatUptime(system.uptimeSeconds)}`
    : "서버(호스트) 기준";

  return (
    <aside className="dashboard-infra-column">
      <ResourceMeter
        title="CPU 사용률"
        percent={cpu ? cpu.usedPercent : null}
        headline={cpuHeadline}
        caption={cpuCaption}
        loading={resourcesLoading}
      />
      <ResourceMeter
        title="메모리 사용률"
        percent={memory ? memory.usedPercent : null}
        headline={memory ? `${formatBytes(memory.usedBytes)} / ${formatBytes(memory.totalBytes)}` : "-"}
        caption={memory ? `여유 ${formatBytes(memory.availableBytes)}` : "서버(호스트) 기준"}
        loading={resourcesLoading}
      />
      <ResourceMeter
        title="디스크 사용률"
        percent={disk ? disk.usedPercent : null}
        headline={disk ? `${formatBytes(disk.usedBytes)} / ${formatBytes(disk.totalBytes)}` : "-"}
        caption={disk ? `${disk.mount} · 여유 ${formatBytes(disk.availableBytes)}` : "마운트 조회 불가"}
        loading={resourcesLoading}
      />

      <Card
        className="dashboard-panel infra-process-card"
        title="주요 프로세스 현황"
        loading={processesLoading && !processes}
      >
        {(processes ?? []).map((group) => (
          <section key={group.key} className="infra-process-group">
            <header className="infra-process-group-heading">
              <span>{group.label}</span>
              <Tag color={STATUS_TAG_COLOR[group.status]}>{STATUS_LABEL[group.status]}</Tag>
            </header>
            {group.processes.map((process) => (
              <ProcessRow key={process.name} process={process} />
            ))}
          </section>
        ))}
        {!processes && !processesLoading
          ? PROCESS_GROUP_PLACEHOLDERS.map((label) => (
              <section key={label} className="infra-process-group">
                <header className="infra-process-group-heading">
                  <span>{label}</span>
                </header>
                <div className="infra-tile-unavailable">상태를 불러올 수 없습니다.</div>
              </section>
            ))
          : null}
      </Card>
    </aside>
  );
}
