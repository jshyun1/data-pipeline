# 완료 콜백 기반 구조 전환 — 영향 범위와 Kafka 브로커 증설 검토 (2026-09-30)

목표 구조(9/29~30 논의)를 적용하면 지금 ETL · 워크플로우의 어디가 어떻게 달라지는지, 그리고 단일 브로커를 늘려도 되는지를
코드 · DB · 컨테이너 설정 기준으로 확인한 기록. 템플릿 쪽 실작업과 ETL 담당자 협의 항목은
[2026-09-30-nifi-template-callback-handoff.md](2026-09-30-nifi-template-callback-handoff.md).

## 0. 결론 요약

- **부하 관점**: 300개 job 이 동시에 대기해도 문제 없다. 대기는 트리거러(asyncio, capacity 1000) 안에서 돌고 워커 슬롯을 안 쓴다.
  Kafka 가 죽었을 때의 30초 원장 폴백은 «300건 ÷ 30초 = 초당 10건 × 부분 인덱스 한 줄 조회» 라 DB 커넥션 하나의 1% 수준이다.
  지금 방식(15초마다 NiFi 그룹 상태 재귀 조회)이 오히려 무겁다. **Kafka 는 지연을 줄이는 가속기지 규모를 버티기 위한 것이 아니다.**
- **영향 관점**: 바뀌는 것은 대부분 «지금 `await` 태스크 하나가 하던 일 7가지를 어디로 옮기느냐»다(2절). 화면 쪽은 §38 때 이미
  `deferred` 를 «실행 중»으로 처리해 두어 추가 작업이 거의 없다. **새로 생기는 문제 하나**는 관측기와 콜백의 이중 행(4절).
- **브로커**: 늘리는 것 자체는 가능하지만 한 호스트 compose 안에서는 얻는 것이 적고, 100개 파이프라인의 병목은 브로커가 아니라
  **Kafka Connect 워커 메모리와 Oracle LogMiner 세션 수**다(8절). 순서는 Connect → 소스 커넥터 묶기 → 브로커.

## 1. 목표 구조와 규모 검토

| 단계 | 내용 | 규모(300 job) 근거 |
|---|---|---|
| 1 | NiFi 템플릿 끝에 완료/실패 콜백 → 백엔드 원장(`etl_job_run`) | job 당 HTTP 1회 |
| 2 | Airflow `await` 를 deferrable 로: 트리거러가 원장을 몇 초마다 확인 | 트리거러 capacity 1000(실측), 대기 태스크는 DAG `max_active_tasks` · 워커 슬롯 집계에서 제외(Airflow 3.2.2 스케줄러 소스 확인, `default_pool.include_deferred=False`) |
| 3 | 백엔드가 원장 커밋 뒤 Kafka 이벤트 발행, 트리거는 «메시지 또는 30초 원장 확인» 하이브리드 | 폴백 초당 10건, 인덱스 `uq_etl_job_run_open(job_id) WHERE ended_at IS NULL`(열린 실행만 담는 부분 인덱스) |
| 4 | 백엔드가 NiFi 상태를 한 번에 모아 화면에 SSE 로 밀어줌 | 백엔드는 이미 15초/30초/60초 세 갈래로 NiFi 를 폴링 중(4절) |

## 2. 지금 `await` 가 하는 일 7가지와 새 방식

[etl_job_runtime.py `_await_completion`](../airflow/dags/etl_job_runtime.py) 기준.

| # | 지금(15초마다, 워커 슬롯 안에서) | 새 방식 |
|---|---|---|
| ① 원장 조회 `GET /job-runs/{token}` | 15초마다 | 트리거러가 몇 초마다(또는 Kafka 이벤트로 즉시) — **유일한 판정 경로** |
| ② NiFi 그룹 상태 recursive 조회(queued · activeThreads) | job 마다 15초 | **사라짐**. 콜백이 안 올 때의 저빈도 폴백(예: 5분)만 |
| ③ 프로세서별 스레드 로그 줄 | 로그에 찍힘 | 사라짐 |
| ④ bulletin 수집 → `fail_if_errors`(콜백이 SUCCESS 여도 ERROR bulletin 이면 실패) | 15초마다 누적 | **유지 필수**. 재개 단계에서 «시작~완료 구간 bulletin» 1회 조회. 단 콜백 처리기(`9x_`) 자신의 bulletin 은 제외 |
| ⑤ 유휴 추측 + 180초 유예 | 항상 이걸로 끝남 | 폴백으로만(콜백 없는 기존 job · NiFi 프로세서 멈춤 대비) |
| ⑥ 카운터 기준선 대비 적재 건수 `print_load_summary` | 완료 시 1회 | 재개 단계에서 1회(같은 문구를 찍으면 화면 무변경) |
| ⑦ `observe-complete/failed` 보고 | 항상 | 콜백이 닫으므로 폴백 때만 |

## 3. Airflow — 바뀌는 것

- **연산자 교체**: `PythonOperator` 는 defer 를 못 한다. `await` 를 커스텀 연산자(시작 → `self.defer(trigger)` → 재개 메서드)로 바꾸고
  트리거 클래스를 `airflow/dags/` 에 둔다(트리거러가 같은 폴더를 마운트). `open_run → start_pg → await → verify`, `await >> stop_pg` 체인은 그대로.
- **트리거는 async HTTP**: 트리거러는 asyncio 루프 하나라 `requests`(블로킹)를 쓰면 다른 트리거가 전부 멈춘다. 확인: `aiohttp 3.13.5` 설치됨,
  트리거러 → `pipeline-api:8081/api/health` 200, `PIPELINE_SERVICE_TOKEN` 주입됨. [_pipeline_svc_auth.py](../airflow/dags/_pipeline_svc_auth.py) 는
  `requests` 만 패치하므로 트리거가 헤더를 직접 붙여야 한다.
- **슬롯(실측)**: 스케줄러 `ConcurrencyMap.load` 는 `DEFERRED` 를 DAG `max_active_tasks` · 워커 슬롯 집계에서 빼고 태스크 단위
  `max_active_tis_per_dag` 에만 센다. `PARALLELISM=4 / MAX_ACTIVE_TASKS_PER_DAG=2` 그대로여도 대기 job 은 슬롯을 안 잡는다.
  `open_run/start_pg/verify/stop_pg` 는 여전히 짧게 슬롯을 쓰므로 [start_pg 주석](../airflow/dags/etl_job_runtime.py) 의 «3·4번 job 시작 3분 지연»은
  크게 줄지만 0 은 아니다 — `MAX_ACTIVE_TASKS_PER_DAG` 상향 검토.
- **태스크 상태**: `running` 대신 `deferred`. §38 때 [logV2Data.ts `ACTIVE_TASK_STATES`](../web/cerebroetl-ui/src/pages/workflowLogV2/logV2Data.ts) 와
  [AirflowDashboardPage.tsx](../web/cerebroetl-ui/src/pages/AirflowDashboardPage.tsx) 에 «실행 중» 처리가 있어 추가 작업 없음. 속성창 `runStateTag` 는 DAG run 상태라 무관.
- **로그 위치**: 대기 구간 로그는 태스크 로그 옆 `*.trigger.<id>.log` 로 따로 쌓이고 API 로 읽을 때 합쳐진다(`FileTaskHandler.add_triggerer_suffix`).
  «queued=… activeThreads=…» 줄이 없어져 [airflowFailureLog.ts](../web/cerebroetl-ui/src/utils/airflowFailureLog.ts) 의 그 잡음 규칙은 쓸 일이 없어진다(해롭진 않음).
- **적재 건수**: 로그 화면 두 곳(`logV2Data.loadCountOf`, `AirflowDashboardPage.outcomeDetail`)이 성공 태스크 로그의 «이번 실행 적재 건수 (총 N행)» 문구를
  정규식으로 읽는다. 재개 단계에서 같은 문구를 찍으면 화면 무변경. 제대로는 원장 `rows_processed`/`total_inserted` 를 읽도록 바꾸는 것이 맞다.
  콜백에는 행수를 싣지 않는다(조각별 `executesql.row.count` 뿐이라 NiFi 안에서 합계를 못 만든다) — 행수는 지금처럼 카운터 기준선 차이.
- **실패 판정 경로**: 지금은 Airflow 가 bulletin 을 봐서 실패. 새 방식은 ① NiFi `98_실패알림`(`/fail`) ② 재개 시 bulletin 검사 ③ `await_timeout_sec`(7200) 마감.
  콜백이 영영 안 오는 경우(연결 큐 백프레셔, 프로세서 멈춤, 들어오는 FlowFile 없이 소스 조회가 실패한 Initial/Incremental)는 ②·③·⑤ 가 잡는다.
- retries · trigger_rule · 하위 워크플로우 · CDC 노드 · 스케줄 · 게시 흐름은 안 바뀐다.

## 4. 백엔드 — 바뀌는 것과 손봐야 하는 것

- 원장 `completion_source` 가 `OBSERVED`(현재 57건 전부) → `CALLBACK`. 받는 쪽 [JobRunLedgerService.completeByPg](../web/backend/src/main/java/com/company/pipeline/workflow/JobRunLedgerService.java) 는
  이미 있다(열린 행 닫기 / 없으면 단독 행 생성 — NiFi 캔버스에서 직접 켠 실행도 이력이 남는다).
- **이중 행 문제(새로 생김)**: 관측기 [NifiProcessorRunTracker](../web/backend/src/main/java/com/company/pipeline/monitoring/NifiProcessorRunTracker.java)(15초)가
  카운터 증가분을 보고 열린 행이 없으면 `openOrAttach` 로 새 행을 만든다. 콜백이 완료 즉시 닫으면 관측기의 다음 틱에 마지막 증가분이 잡히는데
  열린 행이 없으니 두 번째 행이 생긴다 → 실행 1건이 «CALLBACK SUCCESS + 유휴정리 SUCCESS» 2행으로 갈라지고 `total_inserted` 도 나뉜다.
  지금은 둘 다 45초 유휴 기준이라 안 드러났다. **고칠 것**: 관측기가 «직전 N초 안에 콜백으로 닫힌 같은 job 의 행»이 있으면 거기에 붙이고 새로 열지 않기.
  이 행을 읽는 곳: 대시보드 추이(`DashboardTrendController` — 실패 = `nifi_execution_log FAILED` + `etl_job_run FAILED`, 소요시간 = `etl_job_run`),
  알림(`AlertEngine` — JOB 실패 10분 창 · 하루 1회는 7일 내 마지막 실행 상태 · N일간 실행 없음), LLM 컨텍스트(`EtlJobContextText`).
- `closeIdleRuns`(45초)는 콜백 누락 대비로 유지. `/fail?error=` 쿼리 파라미터는 지금 무시된다(본문만 읽음) → `@RequestParam` 한 줄.
- job 생성 시 `__PG_ID__` → 실제 그룹 id 치환(SQL · DBCP 를 채우는 자리에 한 줄). 이게 없으면 템플릿은 복제만 되고 콜백은 안 간다.
- **4단계(집계 상태)**: 백엔드는 이미 NiFi 를 세 갈래로 두드린다 — 관측기 15초(root status recursive + counters), bulletin 30초(→ `nifi_execution_log` FAILED),
  counters + root status 60초(→ `nifi_execution_log` · `nifi_counter_snapshot`). 4단계는 이 셋을 한 수집기로 합치고 SSE 로 밀어주는 백엔드 작업.
  **대시보드 ETL 오류(bulletin 기반) · 적재 건수 추이(카운터 스냅샷) · ETL 로그 화면은 이 경로라 Airflow 변경과 무관하게 그대로다.**
- 3단계: 백엔드에 «원장 커밋 후 발행» 프로듀서 추가. Airflow 이미지에 `apache-airflow-providers-apache-kafka` 가 **없다**(common-messaging 만) → 이미지에 추가.

## 5. 프론트

상태 매핑은 끝나 있다. 적재 건수 파싱 2곳(유지 또는 원장 기반 전환). 확정/추정(`completion_source`) 표기는 지금 화면 어디에도 없어 넣으면 좋다
(`OperationsOverview` 의 `OBSERVED` 는 «관측 대기» 상태로 다른 뜻).

## 6. NiFi 템플릿 (오늘 실작업, 협의 문서 참조)

- §69(9/29)의 «조회 1회 = FlowFile 1개» 는 **틀렸다.** ExecuteSQLRecord `Max Rows Per Flow File` 이 50,000/100,000 이라 큰 테이블은 조각 여러 개다.
  그래서 `97_완료판정`(RouteOnAttribute, 마지막 조각만) 을 앞에 두었다. 전제: `Output Batch Size=0`, 적재 처리기 동시 태스크 1.
- 로컬 `Template` 폴더에 `DEMO_*` (DZ_POP003L 사본) 그룹 5개가 남아 있다(이전 실험, 실제 DB 접속을 가진 사본). 백엔드는 이름으로 찾으므로 간섭은 없지만 정리 대상.
- 로컬에 `HTTP` 템플릿은 없다.

## 7. 변하지 않는 것

워크플로우 캔버스 편집 · 게시 · 스케줄 · 하위 워크플로우 · CDC 노드 · 제어 DAG, 대시보드 ETL 오류(bulletin) · 적재 건수 추이(카운터 스냅샷) ·
ETL 로그 화면(`nifi_execution_log`), 알림 규칙 자체(신호원만 정확해짐), ETL 화면(타 담당자 영역).

## 8. Kafka 브로커 증설 검토

**현재(로컬 · MSA 동일)**: `apache/kafka:3.8.0` KRaft 겸용 노드 1대(`broker,controller`, voters `1@kafka:9093`), 힙 512m / 컨테이너 640m
(사용 로컬 504MB · MSA 452MB). 토픽 전부 RF=1(`__consumer_offsets` 50파티션, `_connect-configs/offsets/status`, `oracle-cdc`, `oracle-cdc.CSB.*`,
`schema-changes.*`, `dlq.pipeline-*`), `default.replication.factor=1`, `min.insync.replicas=1`, `offsets.topic.replication.factor=1`.
Connect 내부 토픽 RF 도 compose 에 1 로 고정. 보존 14일(`KAFKA_LOG_RETENTION_HOURS=336`).

**늘리면 따라오는 것**
- 브로커 전용 노드 2대 추가(`process.roles=broker`, 같은 `CLUSTER_ID`, voters 그대로)는 쉽다. 하지만 **컨트롤러는 여전히 1대** → 그게 죽으면 리더 선출 · 메타데이터 변경 불가.
  컨트롤러도 3대로 하려면 voters 를 정적으로 바꿔 전체 재기동(3.8 에는 동적 쿼럼 KIP-853 없음, 3.9부터).
- **기존 토픽 RF 는 자동으로 안 오른다** → `kafka-reassign-partitions.sh` 로 토픽마다 재할당. 새 스키마 히스토리 토픽은 Debezium 이 브로커
  `default.replication.factor` 를 읽어 만든다(`debezium-storage-kafka-3.6.0` jar 의 `getDefaultTopicReplicationFactor` 확인) → 브로커 기본값만 3 이면 자동.
- 바꿀 설정: `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`, `default.replication.factor=3`, `min.insync.replicas=2`, `CONNECT_*_REPLICATION_FACTOR=3`,
  부트스트랩 목록 3곳 — `KAFKA_BOOTSTRAP_SERVERS`, `CONNECT_BOOTSTRAP_SERVERS`, 그리고 [DebeziumOracleTemplate.java](../web/backend/src/main/java/com/company/pipeline/connector/DebeziumOracleTemplate.java)
  의 `BOOTSTRAP_SERVERS = "kafka:9092"` **하드코딩**(MySQL 템플릿도 같은 상수).
- **한 호스트 compose 안의 3대**는 프로세스 장애 · 롤링 재시작만 막고 호스트/디스크 장애는 못 막는다. 메모리 +1.3GB.
  MSA 호스트: 31GB 중 8GB 여유(같은 호스트에 다른 Airflow 스택 `imp-dclr-*` 도 돎), 6코어, 디스크 270G 중 98G 여유 → 가능은 하다. 로컬(7GB, 2GB 여유)은 무리.

**100개 규모의 병목은 브로커가 아니다** — 파이프라인 100개 = 토픽 ~200개 · 파티션 ~200개. 브로커 1대는 수천 파티션이 보통.
1. **Kafka Connect 워커** — 파이프라인 1개 = 소스 1 + 싱크 1 커넥터(`tasks.max=1`). 100개면 커넥터 200개가 JVM 하나에.
   **MSA 는 커넥터 6개로 이미 1.1/1.25GiB.** Debezium Oracle 은 커넥터마다 LogMiner 세션 + 메모리 트랜잭션 버퍼.
   → 워커 힙 대폭 상향 + 워커 2~3대(같은 `group.id`, 태스크 분산 · 장애 시 재배치)가 브로커 증설보다 먼저.
2. **Oracle 쪽** — 소스 DB 가 같으면 커넥터 100개가 같은 redo 를 각자 100번 마이닝. Debezium 권장은 «DB(스키마)당 커넥터 1개 + `table.include.list` 여러 테이블».
   현재 «파이프라인 = 테이블 1 = 커넥터 1» 모델은 수십 개까진 가지만 100개면 소스 DB 부하가 문제 → 같은 소스 접속의 파이프라인을 소스 커넥터 하나로 묶는 설계(싱크는 테이블별 유지) 별도 검토. 화면 · 원장 · 제어 흐름 영향 큼.
3. 브로커 힙 512m → 1~2GB. 보존 14일 × 100개 → 디스크.
4. 부수 발견: 파이프라인 2 · 3 이 같은 `topic.prefix`(`oracle-cdc`, 화면 입력값)를 써서 스키마 변경 토픽 `oracle-cdc` 를 공유한다. Debezium 은 커넥터마다 고유 prefix 권장.

**권고**: 브로커 3대 + RF3 는 «운영 중 무중단(롤링 재시작 · 프로세스 장애)»이 필요해질 때, 가능하면 호스트를 나눠서. 순서 ① Connect 힙/워커 대수 → ② 소스 커넥터 묶기 설계 → ③ 브로커 증설.
워크플로우 완료 이벤트용 Kafka(3단계)는 원장 폴백이 있어 RF=1 이어도 되지만, CDC 데이터 경로는 RF=1 이면 브로커 재시작 = CDC 전체 정지(볼륨에 남아 유실은 아님).

## 9. 다음 작업 순서(제안)

1. 템플릿 협의(ETL 담당자) → MSA 반영 · 기존 job 보정 방식 결정
2. 백엔드: `__PG_ID__` 치환, `/fail?error=` 읽기, 관측기 이중 행 방지
3. Airflow: deferrable await(트리거러, aiohttp), bulletin 검사에서 `9x_` 제외, 로그 문구 유지 → 로그 화면 무변경 확인
4. (선택) Kafka 이벤트 가속 — provider 추가 · 백엔드 프로듀서
5. (선택) 백엔드 NiFi 폴링 통합 + SSE
6. CDC 규모 대비: Connect 힙/워커, 소스 커넥터 묶기 설계, 그다음 브로커
