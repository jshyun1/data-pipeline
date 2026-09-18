/**
 * 목록 표 정렬 규칙(2026-09-18). 화면마다 제각각 비교하면 «같은 표인데 정렬이 다르다»가 되므로
 * 모든 목록 화면이 이 세 개만 쓴다.
 *
 * <p>값이 비면 <b>항상 뒤로</b> 보낸다. 오름차순에서 빈 값이 맨 위에 몰리면 정렬한 의미가 없다.
 */
export const byText = (a?: string | null, b?: string | null) => {
  if (!a && !b) return 0;
  if (!a) return 1;
  if (!b) return -1;
  return a.localeCompare(b, "ko");
};

export const byNumber = (a?: number | null, b?: number | null) => {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  return a - b;
};

export const byDate = (a?: string | null, b?: string | null) =>
  byNumber(a ? new Date(a).getTime() : null, b ? new Date(b).getTime() : null);

export const byBool = (a?: boolean | null, b?: boolean | null) =>
  byNumber(a == null ? null : Number(a), b == null ? null : Number(b));

/** 검색어 부분일치. 비교 대상 중 하나라도 걸리면 참. 검색어가 비면 항상 참. */
export const matchesText = (needle: string, ...fields: Array<string | null | undefined>) => {
  const trimmed = needle.trim().toLowerCase();
  if (!trimmed) return true;
  return fields.some((field) => (field ?? "").toLowerCase().includes(trimmed));
};
