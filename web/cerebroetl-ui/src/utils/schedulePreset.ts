import dayjs from "dayjs";

// 크론 프리셋 헬퍼. 실행 현황의 스케줄 위저드와 워크플로우 설계 화면이 같은 규칙을
// 쓰도록 공용으로 뺐다 - 두 곳이 각자 계산하면 "화면마다 다음 실행 시각이 다르다"가 된다.

export type SchedulePreset = "hourly" | "daily" | "weekly";

export const WEEKDAY_LABELS = ["일", "월", "화", "수", "목", "금", "토"];

export function presetCron(preset: SchedulePreset, hour: number, minute: number, weekday: number) {
  if (preset === "hourly") return `${minute} * * * *`;
  if (preset === "weekly") return `${minute} ${hour} * * ${weekday}`;
  return `${minute} ${hour} * * *`;
}

export function presetDescription(preset: SchedulePreset, hour: number, minute: number, weekday: number) {
  const time = `${String(hour).padStart(2, "0")}시 ${String(minute).padStart(2, "0")}분`;
  if (preset === "hourly") return `매시간 ${String(minute).padStart(2, "0")}분`;
  if (preset === "weekly") return `매주 ${WEEKDAY_LABELS[weekday]}요일 ${time}`;
  return `매일 ${time}`;
}

/**
 * 다음 실행 시각 미리보기. 저장 전에 "정말 이 시각이 맞나"를 눈으로 확인하게 한다.
 *
 * 기본 2회다. 좁은 속성창에 5줄을 깔면 패널이 늘어져 아래가 잘렸다 - 맞는지 보는 데는
 * 두 줄이면 충분하다.
 */
export function nextPresetRuns(preset: SchedulePreset, hour: number, minute: number,
                               weekday: number, count = 2) {
  const now = dayjs();
  let next = now.second(0).millisecond(0);
  if (preset === "hourly") {
    next = next.minute(minute);
    if (!next.isAfter(now)) next = next.add(1, "hour");
    return Array.from({ length: count }, (_, index) => next.add(index, "hour"));
  }
  next = next.hour(hour).minute(minute);
  if (preset === "weekly") {
    while (next.day() !== weekday || !next.isAfter(now)) {
      next = next.add(1, "day");
    }
    return Array.from({ length: count }, (_, index) => next.add(index * 7, "day"));
  }
  if (!next.isAfter(now)) next = next.add(1, "day");
  return Array.from({ length: count }, (_, index) => next.add(index, "day"));
}

function validCronField(field: string, min: number, max: number) {
  if (field === "*") return true;
  return field.split(",").every((part) => {
    const [range, step] = part.split("/");
    if (step !== undefined && (!/^\d+$/.test(step) || Number(step) === 0)) return false;
    if (range === "*") return true;
    const bounds = range.split("-");
    if (bounds.length > 2) return false;
    return bounds.every((value) => /^\d+$/.test(value) && Number(value) >= min && Number(value) <= max);
  });
}

/** 5필드 크론인지. Airflow가 받아들이는 형식과 맞춘다. */
export function validCron(cron: string) {
  const fields = cron.trim().split(/\s+/);
  if (fields.length !== 5) return false;
  const ranges: Array<[number, number]> = [[0, 59], [0, 23], [1, 31], [1, 12], [0, 7]];
  return fields.every((field, index) => validCronField(field, ranges[index][0], ranges[index][1]));
}

/** 크론 문자열을 프리셋으로 되돌린다(저장된 값을 화면에 다시 채울 때). */
export function parseCronToPreset(cron?: string | null): {
  preset: SchedulePreset; hour: number; minute: number; weekday: number; matched: boolean;
} {
  const fallback = { preset: "daily" as SchedulePreset, hour: 2, minute: 0, weekday: 1, matched: false };
  if (!cron || !validCron(cron)) return fallback;
  const [min, hr, dom, mon, dow] = cron.trim().split(/\s+/);
  const num = (v: string) => (/^\d+$/.test(v) ? Number(v) : null);
  const m = num(min);
  if (m === null || dom !== "*" || mon !== "*") return fallback;
  if (hr === "*" && dow === "*") return { preset: "hourly", hour: 0, minute: m, weekday: 1, matched: true };
  const h = num(hr);
  if (h === null) return fallback;
  if (dow === "*") return { preset: "daily", hour: h, minute: m, weekday: 1, matched: true };
  const d = num(dow);
  if (d === null) return fallback;
  return { preset: "weekly", hour: h, minute: m, weekday: d === 7 ? 0 : d, matched: true };
}

/** 크론 문자열 자체를 사람이 읽는 문장으로 바꾼다(Airflow timetable_summary 표시용). */
export function scheduleDescription(cron?: string) {
  if (!cron) return "수동 실행 전용";
  const [minute, hour, day, month, weekday, ...rest] = cron.trim().split(/\s+/);
  if (rest.length || month !== "*" || !/^\d+$/.test(minute) || !/^\d+$/.test(hour)) return cron;
  const time = `${Number(hour)}:${minute.padStart(2, "0")}`;
  if (/^\d+$/.test(day) && weekday === "*") return `매월 ${Number(day)}일 ${time}`;
  if (day === "*" && weekday === "*") return `매일 ${time}`;
  if (day === "*" && /^[0-7]$/.test(weekday)) {
    return `매주 ${["일", "월", "화", "수", "목", "금", "토", "일"][Number(weekday)]}요일 ${time}`;
  }
  return cron;
}
