/**
 * 화면을 떠나기 전에 한 번 묻는 공용 가드.
 *
 * <p>워크플로우 캔버스처럼 «저장하지 않은 변경»이 남을 수 있는 화면이 여기에 자기
 * 확인 절차를 걸어 두면, 사이드 메뉴·뒤로가기 등 다른 곳에서 이동할 때도 그 절차를
 * 거친다. 예전에는 속성창의 «바로가기» 버튼 두 개에만 확인이 붙어 있어서, ← 나 메뉴로
 * 나가면 그린 내용이 말없이 사라졌다(2026-09-03).
 *
 * <p>router 가 데이터 라우터가 아니라 useBlocker 를 쓸 수 없어 모듈 수준 단일 슬롯으로
 * 둔다. 동시에 두 화면이 «저장 안 됨»일 수는 없으므로 하나면 충분하다. 등록한 화면은
 * 반드시 언마운트에서 해제할 것 - 안 그러면 이미 떠난 화면이 계속 이동을 막는다.
 */
type LeaveGuard = () => Promise<boolean>;

let guard: LeaveGuard | null = null;

export function setLeaveGuard(next: LeaveGuard | null) {
  guard = next;
}

/** 이동해도 되는지 묻는다. 가드가 없으면 항상 true. */
export async function confirmLeave(): Promise<boolean> {
  if (!guard) {
    return true;
  }
  return guard();
}
