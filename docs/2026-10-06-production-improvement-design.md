# 서비스 수준 개선 설계 - 1~3단계 항목별 상용 제품 비교 · 실측 계획 (2026-10-06)

대상: 이 개선을 승인하는 사람과 실제로 구현 · 측정할 개발자.
근거 문서: `docs/2026-10-06-production-readiness-audit.md`(점검 결과). 이 문서는 그 결과의 각 항목에 대해 다음을 정한다.
- **상용 제품은 어떻게 하나**
- **우리는 무엇을 채택하나**
- **어떻게 재서 «됐다»고 판단하나**

기준 버전: NiFi 2.2.0 · Airflow 3.2.2(LocalExecutor) · Kafka 3.8(KRaft) · Kafka Connect(Confluent 7.7.1 = Kafka 3.7) · Debezium 3.6 ·
PostgreSQL 16 · Spring Boot 3.3 · React(React Query).

## 0. 읽는 법 · 공통 원칙

### 0-1. 항목 틀

각 항목은 같은 순서로 쓴다.
1. **지금**: 현재 방식과 근거(파일:행).
2. **상용 제품**: 같은 문제를 상용 · 대표 오픈소스 제품이 푸는 방식. 공개 문서에 나온 일반적인 방식만 적는다.
3. **채택안**: 우리가 할 설계.
4. **실측**: 지표 · 방법 · **합격 기준**. 바꾸기 전에 같은 방법으로 기준값(baseline)을 먼저 잰다.
5. **영향 · 작업량**: 기존 기능 · 데이터 · MSA/폐쇄망 영향, 되돌리기, 작업량(추정, 1명 기준).

### 0-2. 실측 환경과 도구 (모든 항목 공통)

| 구분 | 내용 |
|---|---|
| 환경 A - 로컬 | WSL 8코어 · 7.8GB. 기능 · 정확성 시험과 **축소 규모**(그룹 50~100 · 파이프라인 20~50) 부하 시험. 그 이상은 측정값을 바탕으로 늘려 잡는다(외삽이라고 표시) |
| 환경 B - 부하 시험 서버(필요) | 16~32GB · 8코어 이상 1대(또는 MSA 와 분리된 VM). 목표 규모(그룹 500 · job 2,000 · 파이프라인 500 · 동시 사용자 50) 실측과 3단계 HA 장애 시험. **로컬만으로는 3단계를 «실측했다»고 할 수 없다** |
| 계측 - 백엔드 | 1단계 첫 작업으로 Spring Boot Actuator + Micrometer + Prometheus 엔드포인트 추가. 외부 호출 횟수 · 지연(NiFi/Airflow/Connect 클라이언트별), 주기 작업 소요 시간, HTTP p50/p95/p99 |
| 계측 - 외부 | NiFi `nifi-request.log`(요청 수), nginx 접근 로그(화면 → 서버 요청 수), PostgreSQL `pg_stat_statements` · `EXPLAIN (ANALYZE, BUFFERS)`, `docker stats`(CPU · 메모리), Kafka/Connect JMX |
| 부하 도구 | k6(HTTP 부하 · 동시 사용자), Playwright(실제 화면 여러 탭 · 프레임 시간), NiFi/Connect/Airflow REST 로 시험 대상 대량 생성 스크립트(이미 쓴 `lagtest.py` 방식), pgbench · 대량 INSERT 스크립트 |
| 정합성 | 원천 · 대상 행 수 + 열 단위 해시(예: `md5(string_agg(…))`), Debezium 이벤트 수 대조 |
| 기록 | 항목마다 «기준값 → 변경 후 값 → 합격 여부»를 WORK_LOG 와 이 문서 끝 표에 남긴다 |

### 0-3. 우선순위 · 의존 관계

- 1단계 **1-0(계측 추가)** 을 가장 먼저 한다. 이게 있어야 나머지를 잴 수 있다.
- 2단계 2-1(실행 판정)은 2-2(Airflow 대기) · 2-3(동시 실행) · 2-4(관측기 역할)와 한 묶음으로 설계 · 시험한다.
- 3단계 3-3(CDC 구조 이전)은 3-1(HA) 없이도 할 수 있지만, 고객 원천 DB 에 주는 영향이 가장 크므로 원천 DB 쪽 측정(세션 · WAL)을 반드시 포함한다.

---

## 1단계 - 보안 · 데이터 손실 · 작은 수정 (1~2주)

### 1-0. 계측 추가 (다른 항목의 전제)

- **지금**: 백엔드에 Actuator · Micrometer 없음(빌드 파일 확인). 외부 호출 수 · 주기 작업 시간을 잴 방법이 로그뿐.
- **상용 제품**: Confluent Control Center · Cloudera Manager · Airbyte 등은 구성요소 지표를 Prometheus/JMX 형식으로 내보내고, 운영 화면과 경보가 그 지표를 읽는다.
  Spring 기반 제품은 Actuator + Micrometer 가 사실상 표준이다.
- **채택안**: `spring-boot-starter-actuator` + `micrometer-registry-prometheus`. 외부 클라이언트(NifiClient · AirflowDagRunClient · KafkaConnectClient)에
  `Timer`(호출 수 · 지연 · 결과) 태그, `@Scheduled` 작업마다 소요 시간 타이머. `/actuator/prometheus` 는 내부망에서만(인증 또는 관리 포트 분리).
- **실측**: 지표가 실제 호출 수와 맞는지 - NiFi `nifi-request.log` 요청 수와 백엔드 NiFi 호출 지표를 10분간 비교, 차이 ±2% 이내.
  계측 자체의 부하: 백엔드 CPU 증가 < 2%(docker stats 10분 평균 비교).
- **영향 · 작업량**: 기능 영향 없음. 0.5~1일.

### 1-1. 비밀번호 노출 차단

- **지금**
  - 커넥터 설정 JSON 에 원천 · 대상 DB 비밀번호가 평문으로 들어 있다(`DebeziumOracleTemplate`, `JdbcSinkTemplate`).
    이 설정이 `GET /api/pipelines` 응답에 그대로 나간다(`PipelineConnectorSummary.java:14,26`).
  - 업무 API 는 `permitAll` 이다(`SecurityConfig.java:57`).
  - nginx `/kafka-connect-api/` 는 인증 없이 열려 있다(`nginx.conf:142-151`).
  - Kafka 내부 토픽 `_connect-configs` 에도 평문 비밀번호가 있다.
- **상용 제품**
  - Confluent Platform: Connect 의 **ConfigProvider**(`${file:…}`, `${directory:…}`, Vault 제공자)로 비밀을 설정 밖에 둔다.
    Connect REST 응답과 `_connect-configs` 에는 자리표시자만 남는다. 관리 화면 접근은 RBAC 로 통제한다.
  - Airbyte: 연결 비밀을 별도 비밀 저장소(Vault · 클라우드 비밀 관리자)에 두고, API · 화면은 가린 값(`****`)만 돌려준다.
  - Informatica IICS · Azure Data Factory: 연결(Linked Service) 비밀은 저장 뒤 다시 읽을 수 없다(ADF 는 Key Vault 참조 권장). API 내보내기에도 비밀이 빠진다.
- **채택안**
  1. Connect 워커에 `config.providers=file`(1단계) → 비밀은 Connect 컨테이너의 권한 제한 파일(Docker secret 마운트)에 둔다.
     커넥터 설정에는 `${file:/run/secrets/conn-<id>.properties:password}` 같은 참조만 넣는다. 3단계에서 Vault 제공자로 바꿀 수 있게 한다.
  2. 백엔드는 커넥터 설정을 저장 · 반환할 때 비밀 키(`*.password`, `*.secret`, `*.key` 등)를 가린다. 화면이 실제 값을 받을 경로가 없다.
  3. nginx `/kafka-connect-api/` 삭제. 화면에 필요한 상태는 pipeline-api 가 권한 확인 후 좁힌 DTO 로 준다.
  4. 업무 API 인증을 기본값으로(`anyRequest().authenticated()`).
     - Airflow · NAR 은 서비스 토큰으로 호출한다(2단계에서 호출자별 토큰으로 나눔).
     - MSA 포털 연동은 메모의 방안(서비스 토큰 또는 SSO 교환)대로 한다 - **MSA 포털 경로 영향 확인 필수**.
  5. Kafka 호스트 포트 19092 공개 중단(운영 compose 에서는 내부망만).
- **실측**
  - 노출 0 확인: 로그인 없이 `/api/**`(로그인 · health 제외) 전 경로 호출 → 모두 401.
    로그인 사용자로 `/api/pipelines`, 커넥터 상세 응답 전부에서 비밀번호 문자열 검색 → 0건.
    `/kafka-connect-api/` → 404.
  - `_connect-configs` 토픽을 처음부터 읽어 비밀번호 문자열 검색 → 0건(기존 커넥터 재배포 후).
  - 기능 회귀: 기존 CDC 파이프라인 전부 재배포 → RUNNING, 변경 1,000건 반영 지연이 기준값 대비 +10% 이내.
- **영향 · 작업량**
  - 기존 커넥터는 비밀 파일을 만든 뒤 재배포해야 한다(오프셋 유지 - 이름이 같으면 이어서 돈다).
  - MSA 포털 연동 경로와 Airflow 호출에 인증이 붙는다.
  - 되돌리기: 인증 강제 설정 끄기 · 비밀 파일 대신 평문으로 재배포.
  - 3~4일.

### 1-2. 커밋된 비밀 제거 · 기본 비밀값 거부

- **지금**
  - git 에 `nifi-conf-backup/jsh_260811/conf/{nifi.properties,keystore.p12,truststore.p12}` 와 `web/cerebroetl-ui/certs/localhost.key` 가 있다.
  - Airflow 사용자 비밀번호 HMAC 키 기본값이 소스에 있다(`AirflowUserSyncService.java:70`).
  - `.env.example` 예시 비밀값을 그대로 넣어도 기동된다.
  - Airflow DB 비밀번호가 SQL 에 고정돼 있다.
- **상용 제품**
  - 출시 파이프라인에 비밀 검사(gitleaks · trufflehog · GitHub secret scanning)를 넣는다.
  - 설치 때 관리자 · 키 값을 반드시 새로 만들게 한다(예: NiFi 2 는 single-user 비밀번호를 기동 때 생성 · 출력).
  - 예시 · 기본 비밀로는 기동을 거부하는 «안전하지 않은 기본값 거부»가 일반적이다.
- **채택안**
  1. 파일 삭제 + **해당 키 · 비밀번호 전부 교체**(NiFi sensitive props key 교체 시 흐름 암호화 값 재암호화 절차 포함).
  2. git 이력 정리는 별도 결정(원격 강제 푸시가 필요 - 협업자 영향).
  3. `.gitignore` · pre-commit 에 gitleaks.
  4. 기동 검사: 비밀이 비었거나 예시값 · 알려진 기본값이면 기동 실패(백엔드 · 래퍼 스크립트). Airflow DB 비밀번호는 환경변수로.
- **실측**
  - gitleaks 를 현재 트리와 이력에 실행 → 현재 트리 0건(이력은 정리 여부에 따라).
  - 예시값으로 기동 → 실패 메시지 확인.
  - 교체 후 NiFi 흐름의 민감 속성(DB 비밀번호 등) 복호화 정상 - 전체 job 1회 실행 성공.
- **영향 · 작업량**
  - 키 교체 순서를 틀리면 NiFi 흐름의 민감 값이 깨진다 → 백업 후 진행.
  - MSA · 폐쇄망 설치 절차에 «비밀 생성» 단계가 추가된다.
  - 2~3일(이력 정리 제외).

### 1-3. CDC 데이터 정합 설정

- **지금**
  - `decimal.handling.mode=double`(Oracle · MySQL)
  - `event.processing.failure.handling.mode=warn`
  - `schema.history.internal.skip.unparseable.ddl=true`
- **상용 제품**
  - Debezium 기본값이 `precise`(정확한 소수) · `fail`(실패 시 멈춤) · DDL 건너뛰기 `false` 다.
  - Fivetran · Airbyte 같은 상용 CDC 도 원천 정밀도를 보존하는 쪽(문자열 · 고정 소수점)을 택하고, 처리 실패를 «동기화 실패»로 드러낸다.
  - Confluent 도 금액 열은 `precise` 와 Decimal 논리형 유지를 권장한다.
- **채택안**
  - `decimal.handling.mode=precise`(대상이 NUMERIC 이면 그대로, 문자열이 필요한 대상만 `string`).
  - `event.processing.failure.handling.mode=fail`.
  - DDL 건너뛰기는 기본 false. 꼭 켜야 하는 원천만 파이프라인 설정으로 켜고, 켜면 경보(알 수 없는 DDL 발생)를 함께 건다.
  - 대상 열 타입이 이미 double 로 만들어진 경우 이전 절차(대상 열 NUMERIC 변경 → 재적재)를 둔다.
- **실측(정합성 시험)**
  - 원천에 NUMBER(38,10) · NUMBER(18,2) 경계값(최대 · 최소 · 0.1 + 0.2 류 · 큰 정수 15자리 이상) 1만 행을 넣는다.
  - 초기 적재 + 변경 1만 건 뒤 원천 · 대상 열 해시 비교 → **불일치 0**(현재 설정으로 같은 시험을 해서 불일치 건수를 기준값으로 남김).
  - 처리 실패 주입(대상이 거부할 값이 아니라 원천 쪽 파싱 실패 - 예: 알 수 없는 DDL) → 커넥터가 FAILED 로 멈추고 경보 1건.
- **영향 · 작업량**
  - 기존 파이프라인은 설정을 바꿔 재배포해야 한다. 대상 테이블 타입이 double 이면 재적재가 필요하다(테이블별 판단).
  - 2일 + 이전 작업(테이블 수에 비례).

### 1-4. 보관 정리 안전화

- **지금**: 파티션 테이블도 `DELETE … WHERE ctid IN (SELECT ctid … LIMIT n)`(`RetentionService.java:94-119`). 파티션 삭제(DROP) 구현 없음.
- **상용 제품**
  - 시계열 · 로그 테이블은 시간 파티션 + **오래된 파티션 DETACH/DROP**(pg_partman 이 사실상 표준).
  - TimescaleDB 의 `drop_chunks`, Airflow 의 `airflow db clean`(기준 시각 이전 행을 기본키 범위로 배치 삭제)도 같은 원리다.
- **채택안**
  1. 즉시: 파티션 테이블은 «보관 기한이 지난 파티션만 DETACH → DROP», 일반 테이블은 기본키(또는 시간 열 + 기본키) 범위 배치 삭제로 바꾼다. ctid 사용 금지.
  2. 파티션 생성 · 삭제는 pg_partman(폐쇄망 반입 가능한 확장) 또는 백엔드의 같은 로직으로 - 매일 미래 파티션 미리 생성.
- **실측**
  - 최근 행 안전성: 시험 테이블에 30일치 파티션(파티션마다 같은 ctid 가 생기게 행 수를 맞춤)을 만든다.
    보관 7일로 정리 → **보관 기간 안 행 수 변화 0**, 기한 지난 행 0(현재 코드로 같은 시험을 해서 잘못 지워지는지 기준값 기록).
  - 성능: 1,000만 행 · 30파티션에서 정리 소요 시간과 WAL 생성량(현재 DELETE 방식 대비 - DROP 은 수 초 · WAL 미미 예상).
- **영향 · 작업량**: 데이터 정리 로직만. 2~3일.

### 1-5. 원장 조회 인덱스화

- **지금**
  - `byToken` 이 `runRepository.findAll().stream()` 으로 원장 전체를 읽는다(`JobRunLedgerService.java:84-90`).
  - `resolveJobId` 는 `jobRepository.findAll()` 을 쓴다.
  - 둘 다 Airflow(실행 중 job 마다 3초) · NAR 콜백마다 불린다.
- **상용 제품**: 실행 상태 조회는 실행 id 로 인덱스 조회(Airflow 의 `task_instance` 기본키 조회, ADF pipeline run id 조회) - 언급할 필요도 없는 기본.
- **채택안**: `findByRunToken`(유일 인덱스 `uq_etl_job_run_token` 이미 있음), `findByNifiPgIdAndDeletedAtIsNull`(인덱스 확인 · 없으면 추가).
- **실측**: 원장 100만 행 생성 후 `GET /api/etl/job-runs/{token}` p95 · `EXPLAIN` 확인 → Index Scan, p95 < 10ms(현재 방식 기준값: 1만 · 10만 행에서 측정).
  Airflow 실행 중 job 100개 흉내(k6 로 3초마다 100건) 동안 백엔드 메모리 · GC 안정.
- **영향 · 작업량**: 없음. 0.5일.

### 1-6. NiFi 토큰 재사용

- **지금**: `NifiClient` 의 28곳이 매 호출 전 `/access/token` 로그인(`NifiClient.java:1597-1619`).
- **상용 제품**
  - NiFi 를 감싸는 운영 도구(Cloudera Manager · DataFlow)는 NiFi 를 클라이언트 인증서(mTLS)로 호출한다 - 로그인 단계 자체가 없다.
  - 토큰을 쓰는 경우에도 만료까지 재사용 · 401 시 갱신이 표준이다(우리 Airflow 클라이언트도 이미 그렇게 함).
- **채택안**
  - 1단계: 토큰 캐시(만료 시각 = JWT `exp` - 여유 1분), 401 응답 시 1회 재발급 후 재시도, 동시 갱신은 한 스레드만.
  - 3단계(3-5): 클라이언트 인증서로 전환.
- **실측**: 10분간 NiFi `nifi-request.log` 의 `/access/token` 요청 수 → 기준값(현재) 대비 **분당 1건 이하**. 전체 NiFi 요청 수 약 절반으로.
  NiFi 재기동(토큰 무효) 후 첫 호출이 자동 재발급으로 성공.
- **영향 · 작업량**: 없음. 1일.

### 1-7. 워크플로우 관리(Airflow) 화면의 NiFi 덧칠 제거

- **지금**: `/airflow/manage` 에도 NiFi 전용 덧칠(CSS 주입 · 1초 전체 훑기 번역 · 감시 4개)이 걸린다(`ConsoleFramePage.tsx` handleFrameLoad, 2026-10-05 화면에서 확인).
- **상용 제품**
  - 다른 제품 화면을 붙일 때는 그 제품의 공식 맞춤 수단을 쓴다. Airflow 3 은 UI 플러그인 · 테마 · 다국어 지원이 있고, Astronomer 등도 Airflow 화면을 직접 고치지 않는다.
  - 그렇지 않으면 iframe 없이 링크로 연다.
- **채택안**
  - 덧칠을 NiFi iframe(`showProcessGroupTree` 또는 `product="nifi"`)에만 건다. Airflow 에는 로고 숨김 1줄짜리 CSS 만(필요하면 그것도 Airflow 테마로).
  - 한글화는 Airflow 다국어 설정으로 가능한지 확인한다. 안 되면 영어 유지.
- **실측**: Playwright 로 `/airflow/manage` 를 열고 10초 유휴 · 그리드 스크롤 시 스크립트 시간 · 긴 작업(50ms+) 수 → 기준값 대비. **덧칠 설치 표식 0**.
  화면의 한/영 혼용(«이름/Name») 사라짐 확인.
- **영향 · 작업량**: Airflow 화면 일부 글자가 영어로 돌아갈 수 있다(덧칠이 바꾸던 것). 0.5일.

### 1-8. NiFi 정지 실패 노출 · «활동 없음 = 성공» 제거

- **지금**
  - `stop_pg` 가 예외를 삼킨다(`etl_job_runtime.py:80-85`).
  - «시작 뒤 180초간 활동 없음 = 성공»이다(`etl_nifi_ops.py:38`, `etl_job_runtime.py:192-205`).
  - 실패 · 강제 종료 때 정리 훅(`on_kill`, 리스너)이 없다.
- **상용 제품**
  - Airflow 공식 provider 연산자(Databricks · dbt Cloud · Glue 등)는 외부 작업 결과를 «외부 시스템이 알려 준 최종 상태»로만 성공 처리한다. 모르면 실패 · 재시도한다.
  - 취소 · 실패 시 `on_kill` 로 외부 작업을 취소한다(예: DatabricksRunNowOperator 의 run cancel).
- **채택안**
  - `stop_pg` 실패는 태스크 실패(재시도 3회 그대로).
  - 무활동 = 성공 규칙을 삭제한다. 콜백이 없으면 «시간 초과(실패)»다. 2단계 2-1 전까지는 «활동이 있었고 콜백이 왔을 때만 성공».
  - `on_kill` · DAG 실패 콜백에서 그룹 정지.
- **실측**
  - 고의 실패 시나리오 4개: 정지 API 실패 주입, NiFi 일시 중단, 태스크 강제 종료(UI «실패로 표시»), 시작 무시(이미 RUNNING).
  - 각 시나리오에서 «거짓 성공 0» · 그룹이 RUNNING 으로 남는 경우 0.
  - 정상 job 100회 반복에서 성공률 기준값과 동일.
- **영향 · 작업량**: 지금 «성공»으로 넘어가던 애매한 실행이 실패로 드러난다(의도). 1~2일.

### 1-9. 대시보드 숫자 · 오류 표시 바로잡기(임시)

- **지금**
  - Airflow DAG 을 100개까지만 받는다(`platform.ts:345-350`).
  - 실행은 5,000건에서 자른다.
  - 조회 실패를 `.catch(() => [])` 로 «0건» 처리한다(`DashboardPage.tsx:112-117`).
- **상용 제품**
  - 운영 대시보드(Airflow 자체 · ADF Monitor · Informatica Monitor)는 서버에서 집계한 수치를 보여준다.
  - 원천 조회 실패는 «데이터 없음 / 마지막 갱신 시각»으로 표시하지, 0 으로 보이지 않는다.
- **채택안**
  - 1단계: 페이지를 끝까지 넘겨 받기(또는 서버 집계 쓰기 전까지 상한 경고), 조회 실패 · 오래된 값은 배너로 표시(`isError` · `dataUpdatedAt`).
  - 2단계 2-7 에서 서버 집계 API 로 대체한다.
- **실측**
  - DAG 150개 · 실행 7,000건(시험 DAG 생성)에서 실패 건수가 Airflow DB 직접 집계와 일치.
  - Airflow 를 멈췄을 때 «0건»이 아니라 오류 배너가 보임.
- **영향 · 작업량**: 1일.

---

## 2단계 - 실행 판정 · 수집 구조 (3~6주)

### 2-1. NiFi job 실행 판정 - «조용하면 끝» → «배치 경계 + 실행 번호 보고»

- **지금**
  - NAR 이 «활동 뒤 3초 조용하면 완료»로 판정한다(`CompletionDetector.java:106-118`).
  - 실행은 그룹 단위로 닫는다(`completeByPg`). 실행 번호와 연결되지 않는다.
  - 보고 대기열이 메모리에 있다(NiFi 재기동 시 유실).
- **상용 제품**
  - **StreamSets**: 배치형 파이프라인은 원천이 «더 이상 데이터 없음(no-more-data)» 이벤트를 내고, Pipeline Finisher 가 그 이벤트로 파이프라인을 끝낸다 - 시간 추측이 아니다.
  - **Cloudera DataFlow Functions / Stateless NiFi**: 흐름을 «한 번 실행 단위»로 돌린다. 들어간 데이터가 모두 나가면 그 실행이 끝나고, 성공 · 실패가 실행 결과로 나온다.
  - **NiFi 프로세스 그룹 배치 설정**: `FlowFile Concurrency = Single Batch Per Node` + `Outbound Policy = Batch Output`.
    그룹 안으로 들어간 한 묶음이 모두 처리돼 나올 때까지 다음 묶음을 받지 않고, 출력도 묶음 단위로 내보낸다 - 묶음의 시작과 끝이 명확하다.
  - Informatica · ADF: 실행 엔진이 실행 id 별 최종 상태를 직접 보고한다(관측으로 추정하지 않음).
- **채택안**(NiFi 소스는 고치지 않고 흐름 설정 + NAR)
  1. **실행 번호 전달**: Airflow 가 원장을 연 뒤 받은 `run_token` 을 그룹의 파라미터(또는 시작 FlowFile 속성)로 넣고 시작한다.
     - 원천(GenerateFlowFile/ExecuteSQL 시작점)이 만든 FlowFile 에 `cerebro.run.token` 속성이 실린다.
  2. **배치 경계**: 템플릿의 job 그룹을 `Single Batch Per Node` + `Batch Output` 으로 둔다.
     - 대안 A: 그룹 실행 엔진을 Stateless 로(트랜잭션 단위 실행 · 실패 시 전체 롤백). 소스형 처리기 · 상태 저장 처리기 호환성 확인이 필요하다.
     - 대안 B: 성공 · 실패 출구에 «보고 처리기(NAR 의 새 Processor)»를 두어, FlowFile 의 `run_token` 으로 «성공 n건 / 실패 사유»를 보고한다.
     - 어느 쪽이든 «마지막 FlowFile 이 출구에 닿았다»가 완료 신호다.
  3. **보고 경로를 지속형으로**: NAR 의 보고를 NiFi 상태(State Manager, 재기동 후 유지)에 먼저 쓰고 보낸다. 또는 Kafka 토픽(`cerebro.job-events`)에 쓰고 백엔드가 소비한다.
     - 보낸 뒤 확인된 것만 지운다(최소 1회 + 원장 쪽 멱등 처리 - 이미 있음).
  - **전제(중요)**: `Single Batch Per Node` 의 묶음 경계는 **입력 포트로 들어오는 FlowFile** 에만 적용된다.
    지금 job 그룹은 그룹 안의 원천 처리기(GenerateFlowFile · ExecuteSQL 등)가 스스로 시작하므로 이 설정만으로는 경계가 생기지 않는다.
    그래서 job 그룹을 «입력 포트로 들어온 시작 FlowFile(실행 번호 포함) 하나가 원천 처리기를 깨우는» 구조로 바꿔야 한다.
    ExecuteSQL 류는 들어오는 연결을 받을 수 있다. 시작 FlowFile 은 상위 «실행기» 그룹이나 NiFi REST 로 넣는다.
    템플릿 4종(Initial · Incremental · truncate_initial · File)마다 이 구조가 되는지 먼저 PoC 로 확인한다.
    ListFile 처럼 들어오는 연결을 받지 않는 원천은 대안 B 나 Stateless 로 간다.
  4. **원장**: 콜백은 `run_token` 으로 그 실행을 닫는다(그룹 단위 아님). 콜백 없이 시간이 지나면 TIMED_OUT(성공 아님).
  5. 기존 «3초 조용» 감시는 **배치 설정이 없는 옛 job 의 예비용**으로만 남기고, 그때 결과는 «추정»으로 표시한다(화면에서 구분).
- **실측**(정확성 + 지연)
  - 0단계 PoC: 템플릿 4종을 각각 «입력 포트 시작 + Batch Output»(또는 Stateless)으로 바꾼 시험 그룹에서 정상 적재가 지금과 같은 결과(행 수 · 해시)를 내는지 확인한다.
    안 되는 템플릿은 대안 B 로 정하고, 이 결정을 문서에 남긴 뒤 아래 시험으로 간다.

  | 시험 흐름(로컬 시험 그룹) | 기대 결과 | 합격 기준 |
  |---|---|---|
  | 정상 적재 1만 행 | 1회 완료, 행 수 일치 | 오판 0, 완료 감지 지연 p95 < 3초(마지막 FlowFile 출구 시각 기준) |
  | **간격 실행**(타이머 10초 간격으로 5묶음) | 5묶음 끝난 뒤 1회 완료 | 중간 완료 오판 0(현재 NAR 은 오판할 것으로 예상 - 기준값으로 기록) |
  | Wait/Notify · ControlRate 포함 | 끝난 뒤 완료 | 오판 0 |
  | 중간 실패(DB 거부) | 실패 + 사유 | 거짓 성공 0 |
  | NiFi 재기동(완료 직후 보고 전) | 재기동 뒤 보고 도착 | 유실 0 |
  | 백엔드 5분 중단 | 복구 뒤 보고 반영 | 유실 0 · 이중 행 0 |
  | 같은 job 연속 2회 | 실행마다 별도 결과 | 섞임 0 |
  - 규모: job 100개를 동시에(로컬은 20개) 시작 → 모든 실행이 정확히 1회 완료, 백엔드 콜백 처리 p95 < 100ms.
- **영향 · 작업량**
  - **템플릿(ETL 담당자 영역) 설정 변경과 기존 job 이전**이 필요하다. 이전 전 job 은 예비 감시(추정 표시)로 계속 돈다.
  - Stateless 를 고르면 처리기 호환 검토가 필요하다.
  - 2~3주(템플릿 이전 포함).

### 2-2. Airflow 대기 구조 - 작업 칸을 잡지 않는 연산자

- **지금**
  - `await` 태스크가 `time.sleep` 루프로 최대 7,200초 칸을 점유한다(`etl_job_runtime.py:126-224`).
  - 실행기는 LocalExecutor · `PARALLELISM=4` 다.
- **상용 제품**
  - Airflow 공식 권장은 외부 작업을 기다릴 때 **deferrable 연산자 + triggerer**다. Databricks · dbt Cloud · Glue · EMR provider 연산자가 `deferrable=True` 를 지원한다.
  - 대규모 운영(Astronomer · MWAA)은 Celery/Kubernetes 실행기 + 풀(pool)로 동시성을 관리한다.
- **채택안**
  - `NifiJobOperator` 하나로 합친다(시작 → `defer(NifiRunTrigger(run_token))` → `execute_complete` 에서 결과 확인 · 정지).
  - 트리거는 원장 상태를 비동기로 확인한다. 1차는 비동기 HTTP 로 5초 간격, 2차는 PostgreSQL LISTEN/NOTIFY 또는 SSE 로 즉시 통지.
  - 실행기는 운영 기준으로 Celery(또는 K8s) 검토(3-1 과 함께), 풀로 NiFi 동시 job 수 제한.
- **실측**
  - 동시 job 태스크 100개(로컬 20)를 띄운 상태 기준.
  - **대기 중 점유 칸 0**: `running` 태스크 수 vs `deferred` 수.
  - triggerer CPU · 메모리(트리거 100개에서 CPU < 10% · 메모리 < 300MB 목표).
  - 전체 처리 시간이 «job 실행 시간 합 / 동시 실행 가능 수»에 맞는지. 현재 방식으로 같은 시험을 해서 처리량을 기준값으로 남긴다(현재는 4개씩만 진행 예상).
  - 완료 → 다음 태스크 시작 지연 p95 < 5초(1차) / < 1초(2차).
- **영향 · 작업량**: DAG 생성 코드 변경, 기존 워크플로우 재게시. triggerer 는 이미 있음. 1~1.5주.

### 2-3. 같은 job 동시 실행 제어

- **지금**: 같은 job 을 여러 노드 · 워크플로우가 써도 경고만 한다(`WorkflowValidator.java:436-461`, «한쪽이 대기» 안내는 사실과 다름).
- **상용 제품**
  - ADF 는 파이프라인 단위 `concurrency` 설정(넘치면 대기열)이 있다.
  - AWS Glue 는 job 별 `MaxConcurrentRuns` 를 넘으면 거절한다.
  - Informatica 는 같은 작업 동시 실행을 허용 · 대기 중에서 고른다.
  - Airflow 는 pool(슬롯 1) · `max_active_tis_per_dag` 로 한다.
- **채택안**
  - job 별 **임대(lease)**: 원장 열 때 `job_run_lease(job_id)` 를 원자적으로 잡는다(유일 인덱스 + 만료 시각).
  - 이미 잡혀 있으면 정책에 따라 «대기(트리거가 다시 시도)» 또는 «거절(실패)» - job 설정값.
  - Airflow 쪽은 job 별 pool 대신 이 임대를 기준으로 한다(pool 수가 job 수만큼 늘지 않게). 안내 문구를 실제 동작과 맞춘다.
- **실측**
  - 같은 job 을 두 워크플로우에서 동시에 10회 시작 → 동시 실행 0, 대기 정책이면 모두 순차 완료, 거절 정책이면 거절 사유가 남음.
  - 임대 만료(태스크 강제 종료) 뒤 다음 실행이 막히지 않음.
- **영향 · 작업량**: 3~4일.

### 2-4. 관측기 역할 축소 · 실패 · 건수 보고 경로

- **지금**
  - 관측기(15초 · 45초 무활동)가 원장을 열고 닫는다(`NifiProcessorRunTracker`, `EtlJobRunService.closeIdleRuns`).
  - 실패는 알림판(bulletin) 글자를 30초마다 긁는다(`NifiPipelineMetricScheduler.java:121-172`).
  - 적재 건수는 화면이 로그 글자에서 정규식으로 뽑는다.
- **상용 제품**
  - NiFi 생태계는 실패 · 계보를 **Reporting Task**(SiteToSiteBulletinReportingTask · SiteToSiteProvenanceReportingTask · Prometheus 보고)로 밀어낸다.
  - 실행 결과의 건수는 실행 결과 레코드(구조화된 값)에서 읽는다(ADF activity output, Informatica 세션 통계).
- **채택안**
  - 관측기는 지표(처리량 · 대기열)만 남기고 실행을 열고 닫지 않는다.
  - 실패 사유 · 성공/실패 건수는 2-1 의 보고에 싣는다.
  - 화면 · 알림은 원장의 구조화된 값(`totalInserted`, 실패 사유)만 쓴다. 로그 정규식 제거.
- **실측**
  - 2-1 시험과 같은 흐름에서 원장 건수 = 대상 테이블 실제 증가 행 수(**차이 0**).
  - 알림판 폭주(1분에 오류 100건) 시험에서 실패 누락 0(현재 방식 기준값: 5분 고리 버퍼 · 30초 수집으로 누락 건수 측정).
- **영향 · 작업량**: 1주(2-1 과 함께).

### 2-5. 수집 구조 - NiFi 트리 · 카탈로그 · Kafka Connect 상태

- **지금**
  - NiFi 그룹 트리: 30초마다 그룹별 `GET /flow/process-groups/{id}`(`NifiProcessGroupTreeService.java:76,114`).
  - job 카탈로그: 5분마다 그룹 · 파라미터 컨텍스트 · 처리기별 조회(`NifiJobMirrorService`).
  - Kafka Connect: 파이프라인마다 10초 · 20초 주기로 상태 조회(`KafkaPipelineStateSynchronizer`, `KafkaPipelineMetricScheduler`).
- **상용 제품**
  - NiFi 운영 도구는 **재귀 상태 1회**(`/flow/process-groups/root/status?recursive=true`) 또는 Prometheus 지표(NiFi 2 `/nifi-api/flow/metrics/prometheus`)로 전체를 한 번에 읽는다.
  - 구조 변경은 NiFi Registry 버전 · 흐름 변경 이력(`/flow/history`)으로 «바뀐 것만» 본다.
  - Kafka Connect: Confluent Control Center 등은 `GET /connectors?expand=status&expand=info` 한 번이나 `connect-status` 토픽 구독으로 전체 상태를 얻는다.
    지연은 JMX(`kafka-lag-exporter` · Burrow)로 본다.
- **채택안**
  - NiFi 트리: 재귀 상태 응답 1회로 트리 구성(계층 정보 포함). 그룹 상세는 «이름 · 위치가 바뀐 그룹»만 - 변경 감지는 `/flow/history` 마지막 action id 비교.
  - 카탈로그: 변경 이력 기반 증분 + 하루 1회 전체 대조.
  - Connect: `expand=status` 1회/10초(파이프라인 수와 무관). 3단계에서 `connect-status` 토픽 구독.
  - 오프셋 · 지연은 AdminClient 묶음 조회(파이프라인별 반복 제거).
- **실측**
  - 시험 그룹 100/300개(로컬 100) · 커넥터 50개(로컬 20)를 생성한다.
  - 1-0 지표로 **분당 외부 호출 수**와 수집 1회 소요 시간을 잰다.
  - 목표: NiFi 분당 호출이 그룹 수와 무관하게 상수(트리 · 상태 합쳐 분당 ≤ 10), Connect 분당 ≤ 12, 수집 1회 < 2초(그룹 300).
  - 기준값(현재): 같은 규모에서 측정. 그룹 300 이면 30초당 약 600건(토큰 제외)으로 예상.
- **영향 · 작업량**: 1~1.5주.

### 2-6. 여러 대 운영 대비 - 분산 잠금 · 상태 DB화

- **지금**
  - `@Scheduled` 25개, 분산 잠금이 없다.
  - 파이프라인 잠금 · 실패 횟수 · 마지막 카운터 · 감사 읽기 위치가 메모리에 있다.
  - NAR 감시 등록이 «확인 후 생성»이다.
- **상용 제품**
  - Spring 기반 제품은 ShedLock(JDBC) 또는 리더 선출(Spring Integration LockRegistry · Kubernetes Lease)을 쓴다.
  - Airflow 스케줄러 HA 는 DB 행 잠금(`SELECT … FOR UPDATE SKIP LOCKED`)으로 여러 대가 일을 나눈다.
- **채택안**
  - ShedLock(JDBC, PostgreSQL)으로 모든 수집 · 평가 작업을 «한 대만».
  - 상태는 DB(조건부 갱신 · `version` 열)로 옮긴다.
  - 파이프라인 명령은 DB 상태 전이 검사(`UPDATE … WHERE status = 기대값`)로 직렬화한다.
  - 재알림 키는 경보 id + 재알림 회차(DB 증가)로 유일하게 만든다.
- **실측**
  - 백엔드 2대 · 3대로 2시간 운영한다.
  - 같은 수집 작업 동시 실행 0(ShedLock 표 · 로그).
  - 적재 건수 합계가 1대 운영과 동일(이중 계산 0).
  - 경보 타이머가 설정 시간과 동일하다(예: «10분 지속» 경보가 10분 ±20초에 발생).
  - 같은 재알림 중복 발송 0. 한 대를 죽이면 다른 대가 1주기 안에 이어받는다.
- **영향 · 작업량**: 1주.

### 2-7. 화면 상태 - 서버 집계 · 스냅샷 API + 푸시

- **지금**
  - 대시보드 1탭이 분당 약 68건을 요청한다(50명이면 약 2,700건 · 추정).
  - NiFi · Connect · Airflow 를 화면 요청마다 직접 조회한다.
  - 창에 다시 초점이 오면 약 15개 조회가 한꺼번에 나간다.
- **상용 제품**
  - Confluent Control Center · ADF Monitor 는 서버가 주기적으로 모은 «스냅샷»을 여러 사용자에게 나눠 준다(사용자 수와 원천 호출이 무관).
  - 실시간 변화는 WebSocket/SSE 로 민다.
- **채택안**
  - `GET /api/dashboard/snapshot` 하나(서버가 10~15초마다 갱신 · 캐시) + SSE `/api/events`(상태 변화 · 경보).
  - 화면은 스냅샷 1회 + SSE 구독. 숨은 탭은 구독을 끊는다.
- **실측**
  - k6/Playwright 로 동시 탭 10 · 50개(로컬 10)를 띄운다.
  - **원천(NiFi/Connect/Airflow) 호출 수가 탭 수와 무관한지** - 1탭과 50탭의 분당 NiFi 호출 차이 ≤ 5%.
  - 백엔드 HTTP 분당 요청: 50탭에서 기준값 대비 90% 이상 감소.
  - 상태 변화가 화면에 보이기까지 p95 < 3초.
- **영향 · 작업량**: 대시보드 · 실시간 모니터링 화면 데이터 계층 교체. 1.5~2주.

### 2-8. 목록 · N+1 - 서버 페이지네이션 · 역색인 API

- **지금**
  - 그룹 클릭마다 워크플로우 전체를 하나씩 조회한다(`ConsoleFramePage.tsx:3983-3984`).
  - 로그 · 감사 목록은 기간 전체를 받아 브라우저에서 쪽을 나눈다.
  - CDC 로그 서비스가 31일치 원시 행을 메모리에 올린다(`CdcLogService.java:55-70`).
- **상용 제품**: 운영 화면(Airflow 목록 API · ADF 모니터 · Informatica 모니터)은 서버 쪽 필터 · 정렬 · 페이지(커서)가 기본이다. 집계는 집계 테이블에서 한다.
- **채택안**
  - 역색인 API `GET /api/etl/jobs/{id}/workflows`.
  - 목록 API 전부 `page/size/sort/filter` 와 키셋(커서) 페이지네이션. 알림 이력 탭 방식을 공통으로 쓴다.
  - CDC 로그는 시간 단위 집계 테이블에서 읽는다.
- **실측**
  - 워크플로우 500 · 로그 100만 행 · 감사 100만 행.
  - 그룹 클릭 요청 수 1건.
  - 목록 첫 화면 p95 < 300ms, 응답 크기 < 200KB, 서버 힙 증가 < 50MB.
- **영향 · 작업량**: 1~1.5주.

### 2-9. 오래 걸리는 명령의 비동기화 (CDC 배포 · 시작 · 일괄 생성)

- **지금**
  - 요청 처리 중에 커넥터 상태를 최대 60초 이상 기다린다(`PipelineDeployService.java:58-62,717-757`).
  - 일괄 생성 50건을 한 요청에서 처리한다.
  - Airflow 의 120초 읽기 제한을 넘을 수 있다.
- **상용 제품**: 클라우드 · 데이터 제품의 장시간 작업은 **비동기 작업(operation id) + 상태 조회/푸시**가 표준이다(Azure 의 202 Accepted + Location, Airbyte 의 job/attempt).
- **채택안**
  - 명령 API 는 즉시 `202 + operationId` 를 돌려준다. 작업은 DB 명령 대기열(아웃박스와 같은 집기 방식)에서 처리한다.
  - 상태는 `GET /operations/{id}` · SSE 로 준다.
  - Airflow CDC 노드는 deferrable 트리거로 결과를 기다린다(2-2 와 같은 구조).
- **실측**
  - 커넥터 50개 일괄 생성 → 요청 응답 p95 < 300ms. 전체 완료 시간과 실패 건 사유 기록.
  - 처리 중 백엔드를 재기동해도 명령이 이어서 완료된다(유실 · 이중 실행 0).
- **영향 · 작업량**: 화면 · Airflow CDC 노드 수정 포함 1.5주.

### 2-10. 워크플로우 DAG 생성 · 게시 구조

- **지금**
  - 해석할 때마다 Airflow Variable 을 1+N 번 읽는다(`etl_workflows_dynamic.py:346-379`). 모든 태스크 시작 때 다시 해석한다.
  - 게시가 색인을 통째로 덮어쓴다(`WorkflowPublishService.java:86-88,148-171`).
- **상용 제품**
  - Airflow 3 은 **DAG 번들**(Git/로컬 버전 관리)과 해석 문맥(`get_parsing_context()`)으로 «필요한 DAG 만» 만든다.
  - 대규모 사용자(Astronomer)는 DAG 를 «사양 파일 하나 = DAG 하나»로 둔다. 동적 생성기는 해석 문맥을 쓴다.
- **채택안**
  - 백엔드가 게시 시 워크플로우별 사양 파일(JSON)을 DAG 번들 폴더(버전 관리)에 원자적으로 쓴다(임시 파일 → 이름 바꾸기).
  - 생성기는 해석 문맥으로 해당 DAG 만 만든다.
  - 색인 Variable 은 폐지(파일 목록이 곧 색인).
  - 게시는 워크플로우 단위 잠금, 커밋 뒤 파일 반영(아웃박스).
- **실측**
  - 워크플로우 300개(로컬 100).
  - dag-processor 해석 1회 시간 · CPU.
  - 태스크 1개 시작 시 해석 시간(현재 기준값 대비 90% 감소 목표).
  - 동시 게시 20건 → 누락 0.
  - 게시 → Airflow 에 반영 p95 < 60초.
- **영향 · 작업량**: 1~1.5주.

### 2-11. 워크플로우 경보 규칙의 데이터 원천

- **지금**
  - 경보 규칙이 «전체 DAG 의 최근 200건»만 본다(`AlertEngine.java:1617-1633`).
  - Airflow 조회가 실패하면 빈 목록이 되고, 그러면 경보가 해제된다.
- **상용 제품**
  - Airflow 는 실행 실패 · SLA(Deadline) 이벤트를 콜백 · 리스너로 밖에 알린다.
  - Astronomer 경보 · Datadog 연동도 «실행 이벤트 수신» 방식이다.
- **채택안**
  - Airflow listener 플러그인이 DAG run 상태 변화를 pipeline-api 로 푸시하고, 백엔드는 DAG 별 최신 상태 표를 유지한다.
  - 경보는 그 표로 판단한다. 푸시가 끊기면 «신호 없음» 상태로 둔다(해제하지 않음).
  - 보조 대조: DAG 별 최근 실행 조회(하루 1회).
- **실측**
  - DAG 100개(시간 · 일 주기 섞음)로 24시간 운영.
  - «미완료» 오탐 0, 실패 누락 0(Airflow DB 대조).
  - Airflow 를 30분 중단 → 경보가 해제되지 않고 «신호 없음» 표시.
- **영향 · 작업량**: 1주.

### 2-12. NiFi 선택 동기화 · 편집 잠금

- **지금**
  - 선택은 NiFi 화면 글자에서 UUID · 이름을 긁어 낸다(`ConsoleFramePage.tsx:1907-1975, 2518-2564`).
  - 편집 잠금은 화면 글자로 버튼을 끄는 «권고형»이다(NiFi REST 로는 수정 가능).
- **상용 제품**
  - 화면을 붙일 때는 문서 이벤트 · 메시지(postMessage)로 선택 · 동작을 주고받는다.
  - 잠금은 서버에서 강제한다. NiFi 자체도 리비전(낙관적 잠금)으로 동시 수정을 막는다.
- **채택안**
  - ②안 화면 소스(수정분)에서 선택 · 진입 · 편집 시작/끝 이벤트를 `postMessage` 로 Cerebro 에 보낸다. ConsoleFramePage 는 그 이벤트만 듣는다.
  - 잠금은 편집 대화상자가 열릴 때 잡고, 다른 사용자의 편집 화면에서 «적용»을 화면 소스에서 막는다.
  - NiFi 리비전 충돌은 그대로 안전망으로 둔다.
- **실측**
  - 같은 이름 · 접두어 그룹 10쌍에서 선택 오인 0.
  - 두 사용자 동시 편집 시나리오 10회 → 늦게 연 쪽 적용 불가.
  - 숨은 탭 30분 뒤에도 잠금 유지(심장박동 대신 대화상자 열림 상태 기반).
- **영향 · 작업량**: 1주.

---

## 3단계 - 판매 구성 (1분기 이상, 환경 B 필요)

### 3-1. 고가용성(HA) 기준 구성

- **지금**: Kafka 1대(브로커 겸 컨트롤러, 복제 1), Connect 1대, NiFi 1대, metadata-db 1대(Airflow DB 공유), Airflow LocalExecutor 1대.
- **상용 제품**
  - **Kafka**: Confluent · Kafka 문서의 운영 권장은 브로커 3대 이상, 컨트롤러 3대(KRaft), RF=3, `min.insync.replicas=2`, 생산자 `acks=all`.
  - **Connect**: 분산 모드 2대 이상(작업 재분배).
  - **NiFi**: Cloudera Flow Management 는 NiFi 클러스터(3노드) + NiFi Registry.
  - **PostgreSQL**: Patroni(또는 Crunchy/CloudNativePG) 복제 + 자동 전환.
  - **Airflow**: 스케줄러 2대 이상(HA 지원) + Celery/K8s 실행기.
- **채택안**: 고객 규모별 두 구성을 제품 문서로 정한다.

  | 구성 | 용도 | 내용 |
  |---|---|---|
  | 최소(단일 서버) | 소규모 · 시연 | 지금과 비슷하되 백업 · 감시 필수, RPO/RTO 를 명시(예: RPO 24h · RTO 4h) |
  | 권장(3노드) | 운영 | Kafka 3+3 · RF3, Connect 2, NiFi 3노드 + Registry, PostgreSQL 주/복제 + 자동 전환, Airflow 스케줄러 2 · Celery 작업자 2+, 백엔드 2(2-6 전제) |

- **실측**(환경 B, 장애 주입)

  | 장애 | 지표 | 합격 기준(권장 구성) |
  |---|---|---|
  | Kafka 브로커 1대 강제 종료 | CDC 이벤트 유실 · 중복, 생산 중단 시간 | 유실 0(RPO 0), 중단 < 30초 |
  | Connect 작업자 1대 종료 | 커넥터 재배치 시간 | < 2분, 유실 0 |
  | NiFi 노드 1대 종료 | 실행 중 job 결과, 재분배 | 실패로 드러나거나 재시도로 성공(거짓 성공 0) |
  | PostgreSQL 주 서버 종료 | 전환 시간, 커밋 유실 | RTO < 60초, 동기 복제 시 유실 0 |
  | 백엔드 1대 종료 | 수집 · 경보 공백 | 1주기 이내 인계 |
  | Airflow 스케줄러 1대 종료 | 일정 실행 지연 | < 1분 |

- **영향 · 작업량**: 설치 · 운영 문서, compose(또는 K8s 차트) 재구성. 3~5주(+ 장애 시험).

### 3-2. 백업 · 복구

- **지금**: 백업 도구 없음(수동 덤프뿐). Kafka 볼륨을 잃으면 위치 · 설정 · 스키마 이력이 한꺼번에 사라진다.
- **상용 제품**
  - PostgreSQL 은 pgBackRest/WAL-G 시점 복구(PITR)가 표준이다.
  - NiFi 흐름은 Registry(Git 저장소 백엔드)로 버전 · 복구한다.
  - Kafka 는 복제로 가용성을 확보한다. 재해 복구는 MirrorMaker 2/Cluster Linking 으로 한다.
  - Connect 오프셋은 Kafka 토픽에 있으므로 Kafka 복구와 함께 간다.
- **채택안**
  - metadata-db(+Airflow DB) PITR(일 전체 + WAL 연속).
  - NiFi 흐름 Registry 버전 관리(Git 저장소).
  - 설정 · 비밀 백업 절차.
  - Kafka 는 3-1 복제. 재해 복구가 필요한 고객만 MirrorMaker 2.
  - **복구 훈련 절차서**(분기 1회).
- **실측**
  - 복구 훈련: 어제 시점으로 metadata-db 복구 → 화면 · 워크플로우 · 원장 정합 확인. RTO 측정(목표 < 1시간).
  - NiFi 흐름 삭제 → Registry 에서 복구 → job 실행 성공.
  - Kafka 볼륨 손실 시나리오에서 CDC 재개 절차(재스냅샷 포함) 소요 시간을 문서화.
- **영향 · 작업량**: 1.5~2주.

### 3-3. CDC 구조 이전 - 테이블당 커넥터 → 원천 DB(또는 스키마 묶음)당 커넥터

- **지금**
  - 테이블마다 커넥터를 하나씩 만든다(`ConnectorNaming.java:16-22`).
  - Oracle 은 테이블 수만큼 LogMiner 세션을 연다. PostgreSQL 은 테이블마다 슬롯을 만들고, heartbeat 가 없다.
  - Oracle `topic.prefix` 기본값이 모든 파이프라인에서 같다.
- **상용 제품**
  - **Debezium 문서 · Confluent**: 커넥터 하나가 데이터베이스 하나를 맡고, `table.include.list` 로 여러 테이블을 캡처한다.
    테이블 추가는 **signal 테이블 + 증분 스냅샷**(커넥터를 멈추지 않음)으로 한다.
    PostgreSQL 은 조용한 테이블 대비로 `heartbeat.interval.ms` + `heartbeat.action.query` 를 권장한다.
  - **Fivetran · Airbyte**: «연결 = 원천 DB 하나», 그 안에서 테이블(스트림)을 고르고 테이블별 재동기화를 제공한다.
- **채택안**
  - «CDC 원천 연결» 단위 커넥터(원천 DB 하나, 필요하면 스키마 묶음 · 부하 분리용으로 2~3개).
  - 화면의 «파이프라인»은 그 안의 테이블 묶음(싱크는 대상 단위).
  - 테이블 추가 · 재스냅샷은 signal 테이블 증분 스냅샷으로 한다. 오프셋 초기화는 Connect offsets API(`DELETE/PATCH /connectors/{n}/offsets`, Kafka 3.6+)로 한다.
  - PostgreSQL heartbeat 기본으로 켠다. 커넥터별 고유 `topic.prefix` 를 강제한다.
  - **기존 파이프라인 이전 도구**: 새 커넥터를 «현재 SCN/LSN 부터(스냅샷 없음)»로 만든 뒤 옛 커넥터를 내리는 무중단 전환을 만든다. 겹치는 구간은 대상 upsert 로 흡수한다.
- **실측**(원천 DB 영향이 핵심)

  | 지표 | 방법 | 합격 기준 |
  |---|---|---|
  | Oracle 원천 세션 · CPU | 테이블 20개를 현재(커넥터 20) vs 이전 후(커넥터 1)로 1시간 변경 부하 | LogMiner 세션 수 = 커넥터 수, 원천 CPU 사용 감소(기준값 대비 수치 기록) |
  | PostgreSQL WAL 보존 | 변경 없는 테이블 1개 + 바쁜 테이블 1개, 24시간 | heartbeat 켠 쪽 슬롯 보존 WAL < 1GB(기준값: 계속 증가 예상) |
  | 처리량 · 지연 | 초당 1,000 · 5,000 변경 | 끝단 지연 p95 < 10초, 유실 0(행 해시 일치) |
  | 테이블 추가 | 운영 중 증분 스냅샷으로 100만 행 테이블 추가 | 기존 테이블 지연 영향 < 20%, 추가 테이블 정합 100% |
  | 이전 무중단 | 변경을 계속 넣으며 옛 커넥터 → 새 커넥터 전환 | 유실 0, 중복은 upsert 로 0 |

- **영향 · 작업량**
  - 데이터 모델(파이프라인 · 커넥터 관계) · 화면 · 이전 도구를 바꿔야 해서 가장 크다. 4~6주.
  - 이전은 고객 원천 DB 작업 시간대에 맞춰야 한다.

### 3-4. 관측 - 지표 · 외부 경보

- **지금**
  - 상태를 REST 폴링으로만 본다. JMX · Prometheus 가 없다.
  - Debezium 원천 지연(`MilliSecondsBehindSource`)을 재지 않는다.
  - 감시 · 경보가 감시 대상(백엔드 · DB) 안에 있다.
- **상용 제품**
  - Confluent · Cloudera · Airbyte 모두 Prometheus/JMX 지표 + 외부 경보(Alertmanager · 클라우드 모니터)를 제공한다.
  - «감시 대상과 감시자를 분리»하는 것이 기본이다(Prometheus `up` · `absent()` 경보).
- **채택안**
  - Kafka/Connect/Debezium JMX exporter, NiFi Prometheus 지표, 백엔드 Micrometer(1-0), PostgreSQL exporter, Airflow StatsD/OTel.
  - Prometheus + Alertmanager(별도 컨테이너 · 가능하면 별도 노드)를 둔다.
  - 기존 화면 경보는 «업무 경보», Alertmanager 는 «구성요소 생존 · 자원 경보»로 역할을 나눈다. 폐쇄망 반입 이미지에 포함한다.
- **실측**
  - 경보 시험 표(구성요소별 중단 · 지연 · 디스크 80%)에서 경보 도달 시간 < 2분, 누락 0.
  - 백엔드 · metadata-db 를 멈췄을 때도 경보가 도달한다(지금은 0).
  - 지표 수집 부하가 각 구성요소 CPU +3% 이하.
- **영향 · 작업량**: 2주.

### 3-5. 보안 체계 - 인증서 · 신원 · 비밀 저장소

- **지금**
  - 인증서 검증을 끄고 있다(NiFi 클라이언트 전부 신뢰, `verify=False`, `proxy_ssl_verify off`). NiFi 자체 서명 인증서가 825일 뒤 만료되고 갱신 절차가 없다.
  - 관리자 계정을 모든 구성요소가 공유한다. 서비스 토큰 하나가 모든 권한을 가진다.
  - 로그인 JWT 가 30일 · 철회 불가다. Kafka 가 평문(PLAINTEXT)이다.
- **상용 제품**
  - Cloudera(Auto-TLS: 내부 CA 로 인증서 발급 · 갱신, 구성요소 간 mTLS)와 Confluent(RBAC · mTLS/SASL · 비밀 보호)가 기준이다.
  - 사용자 인증은 OIDC/SAML SSO, 짧은 토큰 + 갱신 토큰을 쓴다.
- **채택안**
  - 내부 CA(설치 때 생성) → 구성요소 인증서 발급 · 만료 전 자동 갱신 스크립트, 모든 클라이언트 검증 켬.
  - 구성요소별 신원: 백엔드 → NiFi 클라이언트 인증서, NAR · Airflow → 범위가 다른 서비스 토큰(job 결과 보고만 / 워크플로우 실행만).
  - Kafka SASL_SSL + ACL.
  - 사용자 JWT 15~60분 + 갱신 토큰(철회 가능).
  - 비밀 저장소(Docker secrets → 고객 선택 시 Vault).
- **실측**
  - testssl.sh/nmap 으로 평문 포트 · 약한 암호 0.
  - 인증서 만료 30일 전 갱신 시험(시계 조정).
  - 권한 범위 시험: NAR 토큰으로 파이프라인 삭제 시도 → 403.
  - 토큰 철회 뒤 즉시 거부.
  - TLS 켠 뒤 처리량 감소 < 10%(Kafka · NiFi).
- **영향 · 작업량**: 설치 절차 · 모든 연결 설정 변경. 3~4주.

### 3-6. 데이터 증가 관리

- **지금**
  - 보관 정책이 없는 테이블: `etl_job_run`, `pipeline_flow_state_event`, `airflow_dag_run`, `pipeline_consistency_check`, `llm_usage_log`, `airflow_dag_alert`.
  - `pipeline_metric_snapshot` 이 파이프라인당 하루 4,320행씩 쌓인다.
  - CDC 로그 화면이 원시 행을 읽는다.
- **상용 제품**: 운영 이력은 «원시는 짧게(일 단위 파티션 · 수일~수주), 집계는 길게(시간 · 일 집계)»가 표준이다(Prometheus 다운샘플링, Airflow `db clean`, ADF 45일 보관).
- **채택안**
  - 성장 테이블 전부를 보관 정책 표에 등록(원시 보관 · 집계 보관 · 파티션 여부).
  - 시간 단위 집계 테이블(rollup 패키지 확장).
  - 화면은 기간에 따라 원시/집계를 자동으로 고른다.
- **실측**
  - 파이프라인 500 · job 2,000 규모를 30일치 생성(환경 B)한다.
  - DB 크기 증가율이 정책대로인지(하루 증가 = 보관 기간 지나면 0 순증).
  - 31일 범위 CDC 로그 화면 p95 < 1초, 서버 메모리 증가 < 100MB.
- **영향 · 작업량**: 1.5주.

### 3-7. 릴리스 · 업그레이드 안전성

- **지금**
  - 적용된 마이그레이션을 수정한 이력이 있고(V63 · V65→V66 · V70), 기동 때 자동 `flyway.repair()` 를 한다.
  - 업그레이드 전 백업 단계가 없다.
  - 폐쇄망 이미지 목록이 오래됐고 digest 가 없다. 기본 이미지 태그가 고정돼 있지 않다. jar 체크섬 검증이 없다.
- **상용 제품**
  - 출시물은 이미지 digest 고정 · SBOM(CycloneDX/SPDX) · 서명(cosign) · 체크섬을 갖춘다.
  - DB 마이그레이션은 «적용된 것은 절대 수정하지 않음 · 되돌리기 스크립트 또는 백업 복구 절차»가 원칙이다(Flyway/Liquibase 운영 지침).
  - 업그레이드 도구가 백업 · 사전 검사 · 롤백을 단계로 갖는다.
- **채택안**
  - 마이그레이션 규칙(적용본 수정 금지 · 자동 repair 제거 · CI 에서 빈 DB 재생 + 운영 스냅샷 재생 검사).
  - 릴리스 스크립트에 digest 고정 · SBOM · 체크섬 · 이미지 목록 자동 생성을 넣는다.
  - `upgrade.sh`(사전 검사 → 백업 → 이미지 교체 → 마이그레이션 → 상태 점검 → 실패 시 복구).
- **실측**
  - 이전 버전 설치 → 업그레이드 → 상태 점검 통과, 고의 실패 주입(마이그레이션 오류) → 자동 복구로 이전 버전 정상.
  - 오프라인 패키지 무결성 검사(체크섬 불일치 시 설치 거부).
  - 폐쇄망 전체 예행연습(save → load → install → healthcheck)을 릴리스마다 자동으로 돌린다.
- **영향 · 작업량**: 2주.

---

## 4. 일정 · 결정 필요 사항

| 단계 | 항목 | 작업량(추정, 1명) | 환경 | 결정 필요 |
|---|---|---|---|---|
| 1 | 1-0 ~ 1-9 | 약 2~3주 | A(로컬) | 1-1 MSA 포털 연동 방식, 1-2 git 이력 정리 여부, 1-3 기존 대상 테이블 재적재 범위 |
| 2 | 2-1 ~ 2-12 | 약 10~14주(2명이면 6~8주) | A + 일부 B | 2-1 배치 방식(Batch Output vs Stateless) · 템플릿 이전 담당(ETL 담당자), 2-2 실행기(Celery/K8s) |
| 3 | 3-1 ~ 3-7 | 약 16~22주 | **B 필수** | 3-1 표준 배포 형태(compose vs K8s), 3-3 기존 파이프라인 이전 일정(고객 원천 DB), 3-5 비밀 저장소(Vault 도입 여부) |

- 작업량은 구현 + 실측 + 문서를 합친 추정이다. 각 항목 착수 전에 상세 설계(인터페이스 · 이전 절차)를 한 쪽으로 다시 확인받는다.
- 모든 항목은 «기준값 측정 → 변경 → 같은 방법으로 재측정 → 합격 기준 판정» 순서를 지킨다. 결과는 아래 표에 쌓는다.

## 5. 실측 결과 기록표 (진행하며 채움)

| 항목 | 지표 | 기준값(변경 전) | 변경 후 | 합격 |
|---|---|---|---|---|
| (예) 1-6 | NiFi `/access/token` 분당 요청 | | | |
