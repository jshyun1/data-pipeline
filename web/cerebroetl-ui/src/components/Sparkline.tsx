/**
 * 스파크라인 — 원본 문서 5-3 "#11 리소스 스파크라인: 최근 1시간 추이. 상승 중인지 하강 중인지 판단".
 *
 * <p>외부 차트 라이브러리를 쓰지 않는다. 20×60px 자리에 축·범례·툴팁이 필요 없고,
 * 인라인 SVG 가 렌더 비용도 훨씬 싸다.
 *
 * <p>결손 구간에서는 선을 끊는다. 수집이 15분 끊긴 구간을 직선으로 이어버리면
 * "그동안 완만하게 변했다"는 없는 사실을 그리게 된다.
 */

export interface SparkPoint {
  at: string;
  value: number | null;
}

interface SparklineProps {
  points: SparkPoint[];
  /** 이 간격(초)보다 크게 벌어지면 결손으로 보고 선을 끊는다. 수집 주기의 2.5배가 기본. */
  gapSeconds?: number;
  width?: number;
  height?: number;
  tone?: "ok" | "warn" | "danger";
}

export function Sparkline({
  points,
  gapSeconds = 150,
  width = 72,
  height = 20,
  tone = "ok",
}: SparklineProps) {
  const usable = points.filter((p) => p.value != null && Number.isFinite(p.value as number));
  if (usable.length < 2) {
    return <span className="sparkline sparkline--empty" title="추이를 그릴 만큼 표본이 모이지 않았습니다">—</span>;
  }

  const times = usable.map((p) => new Date(p.at).getTime());
  const values = usable.map((p) => p.value as number);
  const tMin = Math.min(...times);
  const tMax = Math.max(...times);
  const vMin = Math.min(...values);
  const vMax = Math.max(...values);
  const tSpan = tMax - tMin || 1;
  // 완전히 평평한 계열도 한가운데 선으로 보이도록 최소 폭을 준다.
  const vSpan = vMax - vMin || 1;

  const x = (t: number) => ((t - tMin) / tSpan) * (width - 2) + 1;
  const y = (v: number) => height - 1 - ((v - vMin) / vSpan) * (height - 2);

  // 결손 구간마다 path 를 끊어서 segment 여러 개로 그린다.
  const segments: string[] = [];
  let current: string[] = [];
  for (let i = 0; i < usable.length; i += 1) {
    if (i > 0 && times[i] - times[i - 1] > gapSeconds * 1000) {
      if (current.length > 1) segments.push(current.join(" "));
      current = [];
    }
    current.push(`${current.length === 0 ? "M" : "L"}${x(times[i]).toFixed(1)},${y(values[i]).toFixed(1)}`);
  }
  if (current.length > 1) segments.push(current.join(" "));

  const last = values[values.length - 1];
  return (
    <svg
      className={`sparkline sparkline--${tone}`}
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={`최근 추이 (최소 ${vMin.toFixed(1)}, 최대 ${vMax.toFixed(1)}, 현재 ${last.toFixed(1)})`}
    >
      {segments.map((d, i) => (
        <path key={i} d={d} fill="none" strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
      ))}
      <circle cx={x(times[times.length - 1])} cy={y(last)} r={1.6} />
    </svg>
  );
}
