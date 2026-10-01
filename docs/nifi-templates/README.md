# NiFi 템플릿 flow definition (2026-09-30)

로컬 NiFi 2.2.0 의 `Template > Initial · Incremental · truncate_initial · File` 을 완료/실패 콜백(97_완료판정 · 98_실패알림 · 99_완료알림 · 실패 모음 funnel)을 붙인 뒤
`GET /nifi-api/process-groups/{id}/download` 로 내보낸 것이다. 설명과 협의 항목은 [../2026-09-30-nifi-template-callback-handoff.md](../2026-09-30-nifi-template-callback-handoff.md).

- `X-Service-Token` 값은 `__SERVICE_TOKEN__` 으로 비워 두었다 → 가져온 뒤 그 환경의 `PIPELINE_SERVICE_TOKEN` 으로 채울 것.
- `HTTP URL` 의 `__PG_ID__` 는 자리표시자다 → 그대로 둔다(job 생성 시 백엔드가 치환).
- 컨트롤러 서비스(DBCP · Record Reader/Writer) 참조는 원래 템플릿과 같이 비어 있다(job 생성 시 백엔드가 채움).
- 가져오기: 캔버스에 Process Group 을 끌어다 놓고 «Upload» 에서 파일 선택. 같은 이름의 기존 템플릿 그룹은 먼저 정리한다.
