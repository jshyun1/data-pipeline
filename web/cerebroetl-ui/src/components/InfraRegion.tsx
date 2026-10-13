import { Alert, Button, Card, Modal, Table, Tag, Tooltip } from "antd";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import type { ColumnsType } from "antd/es/table";
import {
  getDiskBreakdown,
  getHostProcesses,
  getMemoryBreakdown,
  type HostProcess,
  type HostResourceResponse,
  type ProcessGroup,
} from "../api/infra";
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

type ResourceKind = "cpu" | "memory" | "disk";

interface ResourceMeterProps {
  title: string;
  /** 제목 줄 오른쪽에 둘 단추(예: CPU 의 «상세»). 없으면 그리지 않는다. */
  action?: React.ReactNode;
  /** 막대 색 계열(디자인 초안: CPU 파랑·메모리 초록·디스크 청록). 정상일 때만 쓰고 경고·위험은 상태색이 이긴다. */
  kind: ResourceKind;
  percent: number | null;
  headline: string;
  caption: string;
  loading: boolean;
}

/**
 * 값 하나를 보여주는 자리라 차트를 쓰지 않고 수치 + 막대 하나로 둔다. 색이 경고를
 * 담당하되 퍼센트 숫자를 항상 같이 적어서 색 없이도 읽힌다.
 *
 * <p>원본 5-3 요구 3가지를 여기서 채운다 — 임계 판정 배지, 스파크라인, 그리고 임계를 넘었을 때
 * 조치 대기열로 보내는 링크(원본 표현: "색상 + 배지 + 조치 대기열 연동").
 */
function ResourceMeter({ title, kind, percent, headline, caption, loading, action }: ResourceMeterProps) {
  const level: Level = percent === null ? "ok" : meterLevel(percent);
  const badge = percent === null ? null : LEVEL_BADGE[level];
  return (
    <Card className="infra-tile" loading={loading}>
      <div className="infra-tile-title">
        <span>{title}</span>
        <span className="infra-tile-title-right">
          {action}
          {badge ? <Tag color={badge.color}>{badge.text}</Tag> : null}
        </span>
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
          </div>
          <div className="infra-meter" role="img" aria-label={`${title} ${percent.toFixed(1)}%`}>
            <span
              className={`infra-meter-fill infra-meter-fill--${level} infra-meter-fill--${kind}`}
              style={{ width: `${Math.min(100, percent)}%` }}
            />
          </div>
        </>
      )}
      {/* 디자인 초안처럼 요약(왼쪽)과 기준(오른쪽)을 한 줄에 둔다. */}
      <div className="infra-tile-desc">
        {percent === null ? null : <span className="infra-tile-headline">{headline}</span>}
        <span className="infra-tile-caption">{caption}</span>
      </div>
      {percent !== null && level !== "ok" ? (
        <a className="infra-tile-queue-link" href="#action-queue">
          조치 대기열 보기 →
        </a>
      ) : null}
    </Card>
  );
}

/**
 * 상세 사용량 접기/펼치기 — 기본은 접힘이다.
 *
 * <p>평소에는 사용률 한 줄이면 충분하고, "무엇이 채우고 있나"는 이상할 때만 궁금하다.
 * 항상 펼쳐두면 타일이 길어져 정작 봐야 할 수치가 밀린다.
 *
 * <p>펼치기 전에는 조회조차 하지 않는다({@code enabled}). 디스크 상세는 서버가 디렉터리를
 * 걷는 비싼 작업이라, 아무도 안 보는 동안 5분마다 도는 것을 피한다.
 */
function BreakdownSection({
  title,
  rows,
  loading,
  expanded,
  onToggle,
  note,
}: {
  title: string;
  rows: Array<{ label: string; usedBytes: number; note?: string; suffix?: string }>;
  loading: boolean;
  expanded: boolean;
  onToggle: () => void;
  note?: string | null;
}) {
  return (
    <div className="infra-disk-breakdown">
      <button type="button" className="infra-breakdown-toggle" onClick={onToggle} aria-expanded={expanded}>
        <span>{title}</span>
        <span className="infra-breakdown-caret">{expanded ? "접기 ▴" : "펼치기 ▾"}</span>
      </button>
      {expanded ? (
        loading ? (
          <div className="infra-disk-breakdown-note">불러오는 중…</div>
        ) : rows.length === 0 ? (
          <div className="infra-disk-breakdown-note">상세를 관측할 수 없는 환경입니다.</div>
        ) : (
          <>
            {rows.map((r) => (
              <Tooltip key={r.label} title={r.note}>
                <div className="infra-disk-breakdown-row">
                  <span className="infra-disk-breakdown-label">└ {r.label}</span>
                  <span className="infra-disk-breakdown-value">
                    {formatBytes(r.usedBytes)}
                    {r.suffix ?? ""}
                  </span>
                </div>
              </Tooltip>
            ))}
            {note ? <div className="infra-disk-breakdown-note">{note}</div> : null}
          </>
        )
      ) : null}
    </div>
  );
}

/**
 * 디스크 용도별 분해 — 원본 5-3 "#12 루트 단일 값 → Kafka / NiFi / 컨테이너로 분리".
 *
 * <p>"컨테이너·이미지" 항목은 제공하지 않는다. 그 값을 재려면 /var/lib/docker 를 마운트해야 하는데
 * 루트 권한 등가라(시크릿 평문 포함) 관측 목적으로 열 수 없다.
 */
function DiskBreakdownList() {
  const [expanded, setExpanded] = useState(false);
  const { data, isLoading } = useQuery({
    queryKey: ["infra-disk-breakdown"],
    queryFn: getDiskBreakdown,
    enabled: expanded,
    refetchInterval: expanded ? 300000 : false,
    placeholderData: (prev) => prev,
  });

  const entries = (data?.entries ?? []).filter((e) => e.error == null);
  const apparent = entries.some((e) => e.measuredBy === "APPARENT");
  return (
    <BreakdownSection
      title="디스크 상세"
      loading={isLoading && !data}
      expanded={expanded}
      onToggle={() => setExpanded(!expanded)}
      rows={entries.map((e) => ({
        label: e.label,
        usedBytes: e.usedBytes,
        suffix: e.isLowerBound ? "+" : "",
        note: e.path,
      }))}
      note={apparent ? "일부 항목은 파일 길이 합계라 실제 점유량보다 클 수 있습니다." : null}
    />
  );
}

/** 메모리 용도별 분해. 82%가 프로세스인지 회수 가능한 캐시인지에 따라 조치가 완전히 다르다. */
function MemoryBreakdownList() {
  const [expanded, setExpanded] = useState(false);
  const { data, isLoading } = useQuery({
    queryKey: ["infra-memory-breakdown"],
    queryFn: getMemoryBreakdown,
    enabled: expanded,
    refetchInterval: expanded ? 30000 : false,
    placeholderData: (prev) => prev,
  });
  return (
    <BreakdownSection
      title="메모리 상세"
      loading={isLoading && !data}
      expanded={expanded}
      onToggle={() => setExpanded(!expanded)}
      rows={(data ?? []).map((e) => ({ label: e.label, usedBytes: e.usedBytes, note: e.note }))}
    />
  );
}

/**
 * CPU 카드 «상세» 창 - 서버에서 도는 프로세스를 사용률 순으로 보여 준다(2026-10-12 요청).
 *
 * <p>열 때 한 번만 조회한다(서버가 0.3초 간격으로 두 번 읽어 사용률을 센다) - 자동 갱신은 하지 않고
 * «다시 읽기»로 사용자가 부른다. 프로세스는 순간순간 바뀌므로 자동으로 흔들리면 오히려 읽기 어렵다.
 */
function CpuProcessModal({ open, onClose }: { open: boolean; onClose: () => void }) {
  const query = useQuery({
    queryKey: ["host-processes"],
    queryFn: () => getHostProcesses(50),
    enabled: open,
    refetchOnWindowFocus: false,
    staleTime: 0,
  });
  const data = query.data;

  const columns: ColumnsType<HostProcess> = [
    { title: "PID", dataIndex: "pid", width: 90, sorter: (a, b) => a.pid - b.pid },
    {
      title: "이름",
      dataIndex: "name",
      width: 180,
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (name: string, row) => (
        <Tooltip title={row.command || undefined}>
          <span className="infra-process-name">{name}</span>
        </Tooltip>
      ),
    },
    {
      title: "CPU",
      dataIndex: "cpuPercent",
      width: 100,
      defaultSortOrder: "descend",
      sorter: (a, b) => a.cpuPercent - b.cpuPercent,
      render: (value: number) => `${value.toFixed(1)}%`,
    },
    {
      title: "메모리",
      dataIndex: "memoryBytes",
      width: 140,
      sorter: (a, b) => a.memoryBytes - b.memoryBytes,
      render: (value: number, row) => `${formatBytes(value)} (${row.memoryPercent.toFixed(1)}%)`,
    },
    { title: "스레드", dataIndex: "threads", width: 80, sorter: (a, b) => a.threads - b.threads },
    {
      title: "상태",
      dataIndex: "state",
      width: 80,
      render: (state: string) => PROCESS_STATE_LABEL[state] ?? state,
    },
    { title: "실행 명령", dataIndex: "command", ellipsis: true, render: (value: string) => value || "-" },
  ];

  return (
    <Modal open={open} onCancel={onClose} footer={null} width={960}
           title="CPU 상세 - 프로세스별 사용률"
           rootClassName="cerebro-modal accent-modal">
      <div className="infra-process-modal-head">
        <span>
          {data
            ? `${data.cores} 코어 · 프로세스 ${data.total}개 중 사용률 상위 ${data.items.length}개`
            : "프로세스 정보를 읽는 중입니다."}
        </span>
        <Button size="small" loading={query.isFetching} onClick={() => void query.refetch()}>다시 읽기</Button>
      </div>
      {data && !data.hostScope && (
        <Alert type="warning" showIcon className="infra-process-modal-note"
               message="서버 전체가 아니라 백엔드 컨테이너 안의 프로세스만 보입니다"
               description="호스트 /proc 을 읽기 전용으로 걸어야 서버 전체가 나옵니다(docker-compose pipeline-api)." />
      )}
      {query.isError && <Alert type="error" showIcon message="프로세스 정보를 불러오지 못했습니다." />}
      <Table<HostProcess>
        rowKey="pid"
        size="small"
        className="std-full-table"
        loading={query.isLoading}
        dataSource={data?.items ?? []}
        columns={columns}
        pagination={false}
        scroll={{ y: 420 }}
      />
    </Modal>
  );
}

/** 커널이 적는 한 글자 상태를 읽을 수 있는 말로. 없는 글자는 그대로 보여 준다. */
const PROCESS_STATE_LABEL: Record<string, string> = {
  R: "실행",
  S: "대기",
  D: "I/O 대기",
  T: "멈춤",
  Z: "좀비",
  I: "유휴",
};

/** 대시보드 오른쪽 인프라 열: 서버 리소스 3종 + CDC/NiFi/Airflow 주요 프로세스 현황. */
export function InfraRegion({ resources, resourcesLoading, processes, processesLoading, dataFlow }: InfraRegionProps) {
  const cpu = resources?.cpu ?? null;
  const memory = resources?.memory ?? null;
  const disk = resources?.disks?.[0] ?? null;

  const cpuHeadline = cpu ? `${cpu.cores} 코어` : "-";
  // CPU 는 «무엇이 쓰고 있나»를 바로 보고 싶을 때가 많아 카드에서 창을 연다(2026-10-12 요청).
  const [cpuDetailOpen, setCpuDetailOpen] = useState(false);

  return (
    <aside className="dashboard-infra-column">
      <ResourceMeter
        title="CPU 사용률"
        kind="cpu"
        percent={cpu ? cpu.usedPercent : null}
        headline={cpuHeadline}
        caption="서버(호스트) 기준"
        loading={resourcesLoading}
        action={<Button size="small" onClick={() => setCpuDetailOpen(true)}>상세</Button>}
      />
      <CpuProcessModal open={cpuDetailOpen} onClose={() => setCpuDetailOpen(false)} />
      <Card className="infra-tile infra-tile--memory" loading={resourcesLoading}>
        <ResourceMeter
          title="메모리 사용률"
          kind="memory"
          percent={memory ? memory.usedPercent : null}
          headline={memory ? `${formatBytes(memory.usedBytes)} / ${formatBytes(memory.totalBytes)}` : "-"}
          caption={memory ? `여유 ${formatBytes(memory.availableBytes)}` : "서버(호스트) 기준"}
          loading={false}
        />
        <MemoryBreakdownList />
      </Card>
      <Card className="infra-tile infra-tile--disk" loading={resourcesLoading}>
        <ResourceMeter
          title="디스크 사용률"
          kind="disk"
          percent={disk ? disk.usedPercent : null}
          headline={disk ? `${formatBytes(disk.usedBytes)} / ${formatBytes(disk.totalBytes)}` : "-"}
          caption={disk ? `${disk.mount} · 여유 ${formatBytes(disk.availableBytes)}` : "마운트 조회 불가"}
          loading={false}
        />
        <DiskBreakdownList />
      </Card>

      <ProcessHealthPanel groups={processes} loading={processesLoading} dataFlow={dataFlow} />
    </aside>
  );
}
