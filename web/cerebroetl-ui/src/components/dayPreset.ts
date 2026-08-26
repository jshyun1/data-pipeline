import type { Dayjs } from "dayjs";
import dayjs from "dayjs";

/**
 * 조회기간의 "당일 / 전일" 프리셋.
 *
 * 화면마다 같은 버튼을 두고 있었는데 «지금 어느 쪽이 적용돼 있는지» 표시가 없어서, 기본값이
 * 당일인데도 아무 버튼도 눌린 것처럼 보이지 않았다(2026-08-26 제보).
 *
 * 선택 상태를 별도 state 로 들고 있으면 달력으로 직접 고른 기간과 어긋난다. 그래서 상태를
 * 저장하지 않고 «현재 조회기간이 그 날 하루와 같은가»로 판정한다. 사용자가 임의 기간을
 * 고르면 어느 쪽도 선택되지 않는다.
 */
export type DayPreset = 0 | 1;

/** 해당 오프셋(0=당일, 1=전일) 하루의 00:00~23:59. */
export function dayPresetRange(offsetDays: DayPreset): [Dayjs, Dayjs] {
  const day = dayjs().subtract(offsetDays, "day");
  return [day.startOf("day"), day.endOf("day")];
}

/**
 * 지금 조회기간이 당일/전일 중 어느 프리셋과 같은지. 어느 쪽도 아니면 null.
 *
 * 분 단위로 비교한다 - 초·밀리초까지 맞추면 endOf("day") 의 999ms 차이로 어긋나고,
 * 시간 단위로 느슨하게 보면 다른 기간까지 선택된 것처럼 보인다.
 */
export function activeDayPreset(from: Dayjs, to: Dayjs): DayPreset | null {
  for (const offset of [0, 1] as const) {
    const [start, end] = dayPresetRange(offset);
    if (from.isSame(start, "minute") && to.isSame(end, "minute")) {
      return offset;
    }
  }
  return null;
}

/**
 * 시간을 쓰지 않고 «날짜»만 다루는 화면용(ETL 로그의 RangePicker 는 날짜 단위다).
 * 시각까지 비교하는 {@link activeDayPreset} 로는 그런 화면에서 절대 일치하지 않는다.
 */
export function activeDayPresetByDate(from: Dayjs, to: Dayjs): DayPreset | null {
  for (const offset of [0, 1] as const) {
    const day = dayjs().subtract(offset, "day");
    if (from.isSame(day, "day") && to.isSame(day, "day")) {
      return offset;
    }
  }
  return null;
}
