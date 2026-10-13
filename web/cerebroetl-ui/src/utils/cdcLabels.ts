/**
 * CDC 명령 이름. CDC 로그 화면의 «구분» 칸과 대시보드 실패 상세 창이 같은 말을 쓰도록 한 곳에 둔다
 * (2026-10-13 - 창과 로그 화면이 같은 구성이어야 한다는 요청).
 */
export const CDC_COMMAND_LABEL: Record<string, string> = {
  VALIDATE: "검증",
  DEPLOY: "배포",
  PREPARE: "실행 준비",
  START: "시작",
  PAUSE: "일시정지",
  STOP: "중지",
  RESTART: "재시작",
  DELETE: "삭제",
  RUNTIME_MONITOR: "런타임 감시",
  DISMISS_DRIFT: "불일치 확인",
  SINK_CONFIG_REFRESH: "싱크 설정 재적용",
};
