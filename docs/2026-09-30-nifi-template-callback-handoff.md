# NiFi 템플릿 완료/실패 콜백 — ETL 담당자 협의용 (2026-09-30)

대상 독자: ETL 템플릿(`Template > Initial · Incremental · truncate_initial · File`)을 관리하는 ETL 담당자.
로컬 NiFi(2.2.0)에서 먼저 만들어 검증한 내용이며, **MSA 반영은 이 문서로 협의한 뒤** 진행한다.
내보낸 flow definition: [`docs/nifi-templates/`](nifi-templates/) (토큰은 `__SERVICE_TOKEN__` 으로 비워 둠).

## 1. 왜 붙이나

워크플로우(Airflow)가 job 완료를 **추측**으로 판정하고 있다. 최근 45일 57건 전부 «관측(OBSERVED)» 이고,
그중 24건은 «활동을 못 봤으니 180초 기다리자»를 그대로 태웠다(적재는 이미 끝났는데 버리는 시간).
job 안에서 **마지막 적재가 끝나는 순간** NiFi 가 백엔드에 한 번 알려 주면 이 추측이 없어진다.
받는 API 는 이미 있다(`POST /api/etl/job-runs/by-pg/{그룹id}/complete` · `/fail`, 서비스 토큰 헤더 필요, 재전송 안전).

## 2. 무엇이 추가되나

템플릿 그룹 끝에 프로세서 3개와 funnel 1개가 붙는다. 기존 프로세서의 **속성은 바꾸지 않고**, 자동 종료돼 있던
관계(success · failure · retry)를 선으로 잇기만 했다. job 은 템플릿 그룹을 통째로 복제해 만들어지므로(snippet),
템플릿에 붙이면 **이후 만드는 job 에는 자동으로 따라간다**(이미 만든 job 은 소급되지 않음 — 8절).

```
(기존)  01_소스테이블 ─success→ 02_플로우파일 ─query→ 03_타깃테이블
                                                       │ success
(추가)                                                 ▼
                                             97_완료판정 (RouteOnAttribute)
                                              │ last (마지막 조각만)          unmatched → 자동 종료
                                              ▼
                                             99_완료알림 (InvokeHTTP POST …/complete)

        01 failure ─┐
        02 failure ─┼─→ (funnel) ─→ 98_실패알림 (InvokeHTTP POST …/fail?error=…)
        03 failure ─┤
        03 retry   ─┘
```

**왜 97_완료판정이 필요한가** — ExecuteSQLRecord 의 `Max Rows Per Flow File` 이 50,000(Initial · Incremental) ·
100,000(truncate_initial) 이라 큰 테이블은 **조회 1회가 FlowFile 여러 개**로 나뉜다(166만 행이면 34개).
조각마다 완료를 보내면 첫 조각에서 원장이 닫힌다. 그래서 ExecuteSQLRecord 가 붙여 주는 `fragment.index`(0부터) ·
`fragment.count` 로 **마지막 조각만** 통과시킨다. 조각이 아닌 FlowFile(`fragment.count` 없음)은 그대로 통과.

## 3. 프로세서 설정

| | 97_완료판정 | 98_실패알림 | 99_완료알림 |
|---|---|---|---|
| 타입 | RouteOnAttribute | InvokeHTTP | InvokeHTTP |
| 핵심 속성 | Routing Strategy = `Route to Property name`<br>동적 속성 `last` = `${fragment.count:isNull():or(${fragment.index:plus(1):equals(${fragment.count:toNumber()})})}` | HTTP Method = `POST`<br>HTTP URL = `http://pipeline-api:8081/api/etl/job-runs/by-pg/__PG_ID__/fail?error=${putdatabaserecord.error:replaceNull(${executesql.error.message:replaceNull('NiFi failure/retry 관계로 라우팅됨')}):replaceAll('\s+',' '):replaceAll('^(.{0,500}).*$','$1'):urlEncode()}` | HTTP Method = `POST`<br>HTTP URL = `http://pipeline-api:8081/api/etl/job-runs/by-pg/__PG_ID__/complete` |
| 공통(98 · 99) | | Connection Timeout `5 secs` · Socket Read/Write Timeout `15 secs` · Request Body Enabled `false`(본문 없음, 적재 데이터가 안 나감) · Request Content-Type `application/json` · Request Failure Penalization Enabled `false` · Response Generation Required `false`<br>동적 속성 `X-Service-Token` = 환경의 `PIPELINE_SERVICE_TOKEN` 값(요청 헤더로 나감) | 왼쪽과 같음 |
| 자동 종료 관계 | `unmatched` | `Response` `No Retry` `Retry` `Original` `Failure` 전부 | 전부 |
| Retry 탭(98 · 99) | | Retried Relationships = `Retry`, `Failure` · Number of Retry Attempts `5` · Retry Back Off Policy `Penalize` · Maximum Backoff `10 mins` — pipeline-api 재배포 중이어도 30초→60→120…초 간격으로 다시 보내 콜백을 안 버린다 | 같음 |
| 스케줄 | Timer 0 sec · 동시 태스크 1 · Penalty 30 sec · Yield 1 sec · Bulletin WARN | 같음 | 같음 |

funnel 은 «실패 모음» 용도의 빈 funnel 하나. 처리기마다 98 로 선을 긋는 대신 funnel 로 모아 98 한 개로 보낸다.

## 4. 템플릿별 연결 변경

«전» 은 전부 자동 종료(버려짐)였다.

| 템플릿 | 처리기 → 관계 | 후 |
|---|---|---|
| Initial | `03_타깃테이블` success | → 97_완료판정 → (last) → 99_완료알림 |
| | `01_소스테이블` failure · `02_플로우파일` failure · `03_타깃테이블` failure, retry | → funnel → 98_실패알림 |
| Incremental | `03_타깃테이블` success | → 97 → 99 |
| | `01` failure · `02` failure · `03` failure, retry | → funnel → 98 |
| truncate_initial | `05_INSERT` success | → 97 → 99 |
| | `02_TRUNCATE` failure · `03_TRUNCATE_DZ` failure, retry · `04_SELECT` failure · `04-1_add_columns` failure · `05_INSERT` failure, retry | → funnel → 98 |
| File | `FetchFile` failure, not.found, permission.denied · `ConvertRecord_CSV` failure · `ConvertRecord_EXCEL` failure · `QueryRecord` failure · `PutDatabaseRecord` failure, retry | → funnel → 98 (**완료 콜백은 없음**, 5절) |

## 5. 전제와 제약 (지켜져야 콜백이 맞는다)

1. ExecuteSQLRecord `Output Batch Size` 는 **0** 이어야 한다. 0 이 아니면 `fragment.count` 가 안 붙어 조각마다 완료가 나간다(지금 셋 다 0).
2. `Max Rows Per Flow File` 값은 자유(0 이면 조각 없이 한 개 — 그것도 통과). 값을 바꿔도 규칙은 그대로 맞는다.
3. 적재 처리기(PutDatabaseRecord) **동시 태스크 1** 전제. 조각이 순서대로 처리되므로 «마지막 조각 적재 완료 = 전체 완료». 2 이상으로 올리면 마지막 조각이 먼저 끝날 수 있다(그땐 MergeContent Defragment 방식으로 바꿔야 함).
4. URL 의 `__PG_ID__` 는 자리표시자다. **job 생성 시 백엔드가 그 job 그룹의 id 로 치환**한다(백엔드 후속 작업, 9절). 템플릿에는 자리표시자 그대로 둔다.
5. `X-Service-Token` 값은 **환경마다 다르다**(MSA 는 MSA 의 `PIPELINE_SERVICE_TOKEN`). 내보낸 JSON 에는 `__SERVICE_TOKEN__` 으로 비워 두었다. NiFi 화면에서 값이 보이는 일반 속성이다(민감 속성이 아님 — 협의 항목 8-3).
6. **File 템플릿은 완료 콜백을 붙이지 않았다.** 파일 하나마다 FlowFile 이 따로 오고 «이번 실행의 파일 수»를 알 수 없어 첫 파일에서 원장이 닫힌다. 실패 콜백만 붙였고 완료는 기존 관측 방식(유휴 판정)으로 남는다.
7. 코드가 이름으로 찾는 템플릿에 `HTTP` 도 있는데 **로컬에는 없다.** MSA 에 있으면 같은 규칙(적재 처리기 success → 97 → 99, 실패 관계 → funnel → 98)으로 붙이면 된다.
8. 소스 조회 자체가 실패하면(DB 접속 불가 등) ExecuteSQLRecord 에 **들어오는 FlowFile 이 없는 템플릿**(Initial · Incremental 의 `01_소스테이블`)은 failure 로 내보낼 FlowFile 이 없어 **실패 콜백이 안 나간다**(bulletin 만 남음). truncate_initial 은 `03_TRUNCATE_DZ` 에서 들어오는 FlowFile 이 있어 나간다. 이 경우는 Airflow 쪽 bulletin 검사가 계속 잡는다(그대로 유지).

## 6. MSA 반영 방법 (협의 뒤)

- **방법 A — JSON 가져오기**: 캔버스에 Process Group 을 끌어다 놓고 «Upload» 로 `docs/nifi-templates/<이름>.flow.json` 을 올린다.
  새 그룹이 생기므로 기존 템플릿 그룹과 이름이 겹친다 → 기존 것을 지우거나 이름을 바꾼 뒤 새 그룹 이름을 원래 이름으로.
  올린 뒤 98 · 99 의 `X-Service-Token` 값을 MSA 토큰으로 채운다. 컨트롤러 서비스 참조(DBCP · Reader/Writer)는 기존 템플릿과 같이 «없음» 상태(job 생성 시 백엔드가 채움).
- **방법 B — 손으로 추가**: 3절 표대로 프로세서 3개 + funnel 을 만들고 4절대로 선을 잇는다. 기존 처리기의 해당 관계 자동 종료 체크를 풀어야 선이 그어진다.
- **주의 1**: MSA `Template` 폴더에는 같은 이름이 두 벌씩 있다(`Initial` 프로세서 0개짜리와 3개짜리, `Incremental` 도). 백엔드는 이름으로 **첫 번째**를 고르므로 콜백과 무관하게 정리가 필요하다.
- **주의 2**: 이미 만들어진 job 은 소급되지 않는다. 기존 job 에는 (a) 같은 구성을 손으로 추가하거나 (b) job 재생성. 몇 개인지에 따라 백엔드가 일괄 보정 스크립트를 제공할 수도 있다(협의).
- **주의 3**: 콜백 처리기가 실패하면(WARN/ERROR bulletin) 워크플로우 로그 · 대시보드 ETL 오류에 `98_실패알림`/`99_완료알림` 이름으로 보인다. 알림 자체의 실패라 데이터 실패와 구분되게 Airflow/백엔드 쪽에서 `9x_` 접두어를 걸러 낼 예정(백엔드 후속).

## 7. 로컬에서 검증한 것 (2026-09-30)

- 4개 템플릿에 적용 후 97 · 98 · 99 모두 **VALID**(기존 처리기의 INVALID 는 템플릿 특성 — 컨트롤러 서비스 참조 없음).
- 임시 그룹에서 GenerateFlowFile 로 `fragment.index` 0·1·2(`count` 3) + 조각 없는 FlowFile 1개를 흘려 보냄 → 97 은 index 2 와 조각 없음만 `last` 로 보내고 0·1 은 unmatched(2건). 99 는 두 건 모두 HTTP **200**, 원장에 `completion_source=CALLBACK` 행 생성.
- `putdatabaserecord.error` 에 «ORA-01400: 널 삽입 불가 + 줄바꿈 + 탭» 을 넣어 98 호출 → 200, URL 에 `error=ORA-01400%3A+%EB%84%90+%EC%82%BD%EC%9E%85+…` 로 공백 정리 · UTF-8 인코딩 확인, 원장 FAILED 행 생성.
- 백엔드가 job 생성에 쓰는 **snippet 복제 API** 로 `truncate_initial` 을 복제 → 복제본에 9 프로세서 · 13 연결 · funnel 1 개가 그대로 따라옴(자리표시자 포함).
- 시험으로 생긴 임시 그룹 · 원장 행 3건은 삭제했다.

## 8. 협의가 필요한 결정

1. **`retry` 관계** — PutDatabaseRecord/PutSQL 의 retry(«다시 하면 될 수도 있는 실패»)를 지금은 failure 와 같이 실패 콜백으로 보냈다. 자기 자신으로 되돌려 재시도할지, 실패로 볼지는 ETL 정책.
2. **실패 콜백의 의미** — 조각 하나가 실패하면 그 실행은 FAILED 로 닫힌다(나머지 조각은 계속 적재됨). 부분 적재를 «실패»로 보는 것이 맞는지.
3. **토큰 노출** — `X-Service-Token` 이 일반 속성이라 NiFi 화면에서 보인다. 민감 파라미터(Parameter Context)로 옮길지.
4. **적재 행수** — 콜백에는 행수를 싣지 않았다(조각별 `executesql.row.count` 만 있어 합계를 NiFi 안에서 못 만든다). 행수는 지금처럼 NiFi 카운터 기준선 차이로 백엔드가 센다. 정확한 행수가 꼭 필요하면 백엔드가 조각 정보(`fragment.index/count`)를 받아 합산하는 방식으로.
5. **bulletin 레벨** — 콜백 처리기 bulletin 을 WARN 으로 둘지 NONE 으로 둘지(NONE 이면 콜백 실패 원인을 화면에서 못 본다).
6. **Max Rows Per Flow File** 을 0 으로 바꾸면 조각이 없어져 97 이 필요 없지만, 155MB 짜리 FlowFile 하나가 되는 것이 ETL 쪽에 괜찮은지.

## 9. 백엔드 쪽 후속 (참고, ETL 담당자 작업 아님)

- job 생성 시 `__PG_ID__` → 실제 그룹 id 치환(SQL · DBCP 를 채우는 자리에 한 줄).
- `/fail?error=` 쿼리 파라미터 읽기(지금은 본문만 읽어 기본 문구 «NiFi가 실패를 보고했습니다.»로 남는다).
- 관측기(NifiProcessorRunTracker)가 콜백으로 방금 닫힌 실행에 꼬리 활동을 붙이도록(이중 행 방지).
- Airflow await 를 콜백 우선 + 트리거러 대기로 전환, bulletin 검사에서 `9x_` 콜백 처리기 제외.
- 기존 job 일괄 보정 스크립트(필요 시).
