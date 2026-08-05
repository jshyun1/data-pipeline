# 운영 치트시트 — 자주 쓰는 명령·경로·테이블

> 정리일: 2026-07-31 / 기준 브랜치: `msa-integration`
> 근거: 로컬 스택 실측(컨테이너 12개 기동 상태에서 NiFi API·Airflow 메타DB·타깃 DB 직접 조회)
> 포트·계정은 모두 [`.env`](../.env)에서 오며, 값이 바뀌면 이 문서보다 `.env`가 우선이다.

---

## 1. 스택 제어

```bash
./docker-control.sh up dev      # --profile poc 포함 (로컬 Oracle/target-db까지 기동)
./docker-control.sh up prod     # 사내 실 Oracle/타란툴라DB 연결 전제, POC 컨테이너 제외
./docker-control.sh down
./docker-control.sh logs
./docker-control.sh ps

docker ps --format '{{.Names}}\t{{.Status}}'      # 컨테이너 상태 일괄 확인
docker compose up -d --build pipeline-api          # 특정 서비스만 재빌드
docker logs -f --tail 200 pipeline-api             # 로그 추적
docker stats --no-stream                           # 메모리 압박 확인 (WSL2 할당 11GB)
```

컨테이너 11개: `cerebroetl-ui` `pipeline-api` `nifi` `kafka` `kafka-connect`
`filebeat` `airflow-apiserver` `airflow-scheduler` `airflow-dag-processor` `metadata-db` `target-db`

---

## 2. 주소 / 포트

| 용도 | 주소 |
|---|---|
| 통합 포털 | `http://localhost:13001` |
| pipeline-api 직접 | `http://localhost:18086` |
| NiFi 캔버스 | `https://localhost:18443/nifi` (admin / `NIFI_SINGLE_USER_PASSWORD`) |
| Airflow | `http://localhost:8090` |
| http-ingest 수집 | `POST http://localhost:18442/contentListener` |
| 비정형 수집 | `POST http://localhost:18444/unstructured` |
| Kafka 브로커(호스트) | `localhost:19092` |
| 메타DB(호스트) | `localhost:15434` |
| target-db 컨테이너(호스트) | `localhost:15432` |
| 타란툴라DB(외부 실 타깃) | `192.168.50.12:7432` |
| Oracle 원천(외부 VM) | `192.168.204.128:1521 / XEPDB1` (계정 CSB) |

---

## 3. DB 접속 — 가장 자주 쓰는 4가지

```bash
# ① 메타데이터 DB : 파이프라인 정의·커넥터·명령이력·NiFi 지표
docker exec metadata-db psql -U pipeline_app -d pipeline_meta \
  -c "select id,name,pipeline_type,status from pipeline_definition order by id;"

# ② Airflow 메타DB : 같은 컨테이너, 다른 DB
docker exec metadata-db psql -U airflow_app -d airflow \
  -c "select dag_id,count(*),max(start_date) from dag_run group by 1 order by 3 desc nulls last;"

# ③ 타란툴라DB : 실제 타깃 (외부 서버)
docker exec -e PGPASSWORD=<타란툴라DB 비밀번호> target-db psql -h 192.168.50.12 -p 7432 -U tarandb -d postgres \
  -c "select count(*) from dz_com003m;"

# ④ target-db 컨테이너 : POC 랜딩용
docker exec target-db psql -U tarantula_app -d tarantula -c "\dt unstructured_landing.*"
```

psql 안에서 자주 쓴 것: `\dn`(스키마) · `\dt 스키마.*`(테이블) · `\d 테이블`(컬럼/인덱스) · `-x`(세로 출력)

접속이 `password authentication failed`로 끊기면 네트워크가 아니라 **비밀번호 문제**다.
`PGPASSWORD=` 로 넘기거나 `~/.pgpass`(권한 600)에 `192.168.50.12:7432:*:tarandb:<비밀번호>`를 넣는다.

---

## 4. 주요 테이블

### 메타DB `pipeline_meta` — 화면·API가 읽는 원장

| 테이블 | 내용 |
|---|---|
| `pipeline_definition` | 파이프라인 원장(유형·소스/타깃·토픽·상태). 이 ID가 `kafka_pipeline_{id}_control` DAG명이 된다 |
| `pipeline_connection` | 소스/타깃 DB 접속정보 (비밀번호는 암호화 저장) |
| `pipeline_connector` | 파이프라인당 커넥터 쌍(SOURCE/SINK)과 배포된 설정 JSON |
| `pipeline_command_history` | 시작/중지/배포 명령 이력 — 대시보드 "명령 이력" |
| `log_pipeline_source` | LOG_FILE 파이프라인의 Filebeat 입력 설정 |
| `pipeline_daily_load_metric`, `pipeline_metric_snapshot`, `nifi_counter_snapshot` | 대시보드 적재량 차트 원본 |
| `nifi_execution_log`, `nifi_processor_run` | `/etl/logs` 실행 이력 |
| `app_user` | 로그인 계정(Keycloak 제거 후 ST_USER 대체 테이블) |

### 타란툴라DB(192.168.50.12) `postgres` — 운영 타깃

| 그룹 | 테이블 |
|---|---|
| DZ 착지 11개 | `dz_com001m` `dz_com002l` `dz_com003m` `dz_com004m` `dz_com111m` `dz_pop001l`~`dz_pop006l` |
| DW 가공 10개 | `dw_com001m`~`dw_com004m`, `dw_pop001l`~`dw_pop006l` |
| CDC 타깃 | `"LW_IMP018M"` — 대문자라 조회 시 큰따옴표 필수 |
| 비정형 5종 | `unstructured.image_files` `video_files` `log_records` `xml_records` `csv_records` |
| 원천 사본 | `tb_com001m` `tb_com003m` 등 `tb_*` |
| 성능시험 | `perf_k2_src` `perf_k4_src` `perf_n2_src` `perf_n3_src` |

### target-db 컨테이너 `tarantula` — POC 랜딩

`unstructured_landing.file_objects`(http-ingest 타깃) · `log_file_lines` · `access_log` ·
`log_landing.app_log`(app-log-ingest 타깃) · `cdc_landing.customers` · `batch_landing.employees`

---

## 5. Kafka Connect

```bash
docker exec kafka-connect curl -s localhost:8083/connectors                       # 커넥터 목록
docker exec kafka-connect curl -s localhost:8083/connectors/{이름}/status | python3 -m json.tool
docker exec kafka-connect curl -s -X PUT localhost:8083/connectors/{이름}/resume   # 시작
docker exec kafka-connect curl -s -X PUT localhost:8083/connectors/{이름}/stop     # 중지

# Oracle 원천 도달 확인 (실패하면 VM부터 기동)
docker exec kafka-connect bash -c 'timeout 5 bash -c "</dev/tcp/192.168.204.128/1521" && echo OK'
```

---

## 6. NiFi REST — 토큰부터 받는다

```bash
TOKEN=$(curl -sk -X POST https://localhost:18443/nifi-api/access/token \
  -d 'username=admin' --data-urlencode 'password=<NIFI_SINGLE_USER_PASSWORD>')

N() { curl -sk -H "Authorization: Bearer $TOKEN" "https://localhost:18443/nifi-api$1"; }

N /flow/process-groups/root                              # 최상위 그룹 목록
N /process-groups/{그룹ID}/processors                    # 프로세서 + 설정값 전부
N /process-groups/{그룹ID}/connections                   # 연결 구조
N "/flow/process-groups/{그룹ID}/status?recursive=true"  # 큐/활성 스레드 = 실제 실행 여부
N /flow/bulletin-board                                   # 오류 알림(ERROR bulletin)
N /flow/process-groups/{그룹ID}/controller-services      # DBCP/Reader 등
```

프로세서 속성을 API로 바꿀 때는 **먼저 `component.config.descriptors`로 실제 키를 확인**한다.
UI 라벨과 API 키가 다른 경우가 많다(예: DBCP 드라이버 경로 = `database-driver-locations`).

---

## 7. Airflow

```bash
docker exec airflow-scheduler airflow dags list
docker exec airflow-scheduler airflow variables list
docker exec airflow-scheduler airflow variables get nifi_pipeline_9e2da75d_control__schedule
docker exec airflow-scheduler airflow variables set {dag_id}__auto_stop_after_run true

# DAG 태스크 구성은 CLI가 못 읽는 경우가 있어 메타DB에서 직접 확인
docker exec metadata-db psql -U airflow_app -d airflow -t -c \
  "select t->'__var'->>'task_id' from serialized_dag s,
          jsonb_array_elements(s.data->'dag'->'tasks') t where s.dag_id='{dag_id}';"
```

**Variable 키 규칙**

| 키 | 용도 |
|---|---|
| `{dag_id}__schedule` | 크론 표현식 / `@daily` 같은 프리셋. 없으면 수동 트리거 전용 |
| `{dag_id}__auto_stop_after_run` | `true`면 실행 후 NiFi 그룹 자동 정지(배치용) |
| `{dag_id}__target_schema` / `__target_table` / `__target_timestamp_column` | 적재 검증 3종 세트. 셋 다 있어야 검증을 수행한다 |
| `nifi_process_groups_cache` / `nifi_root_connections_cache` | NiFi 조회 실패 시 DAG가 사라지지 않게 하는 캐시 |

변수·DAG 변경은 **파싱 주기(최대 5분)가 지나야** 화면에 반영된다.

---

## 8. 코드 경로 지도

| 무엇을 고칠 때 | 경로 |
|---|---|
| Airflow 동적 DAG(제어·완료대기·검증) | [`airflow/dags/nifi_pipelines_dynamic.py`](../airflow/dags/nifi_pipelines_dynamic.py), [`kafka_pipelines_dynamic.py`](../airflow/dags/kafka_pipelines_dynamic.py) |
| 파이프라인 생성/배포 로직 | [`.../pipeline/`](../web/backend/src/main/java/com/company/pipeline/pipeline/) — `PipelineService`, `PipelineDeployService` |
| 커넥터 설정 렌더링 | [`.../connector/`](../web/backend/src/main/java/com/company/pipeline/connector/) — `DebeziumOracleTemplate`, `JdbcSinkTemplate` |
| 로그 파이프라인 · Filebeat | [`.../logpipeline/`](../web/backend/src/main/java/com/company/pipeline/logpipeline/), [`filebeat/filebeat.yml`](../filebeat/filebeat.yml) |
| 대시보드 자원·프로세스 상태 | [`infra/ProcessHealthService.java`](../web/backend/src/main/java/com/company/pipeline/infra/ProcessHealthService.java), [`HostResourceService.java`](../web/backend/src/main/java/com/company/pipeline/infra/HostResourceService.java) |
| NiFi 지표 수집(60초 주기) | [`monitoring/NifiPipelineMetricScheduler.java`](../web/backend/src/main/java/com/company/pipeline/monitoring/NifiPipelineMetricScheduler.java) |
| 대시보드 화면 | [`DashboardPage.tsx`](../web/cerebroetl-ui/src/pages/DashboardPage.tsx), [`InfraRegion.tsx`](../web/cerebroetl-ui/src/components/InfraRegion.tsx) |
| 파이프라인 생성 폼 | [`PipelinesPage.tsx`](../web/cerebroetl-ui/src/pages/PipelinesPage.tsx) |
| DB 초기 스크립트 | [`db/tarantula-init/`](../db/tarantula-init/), [`db/target-init/`](../db/target-init/) |
| NiFi 플로우 문서·백업 | [`nifi/FLOW_RUNBOOK.md`](../nifi/FLOW_RUNBOOK.md), [`nifi/flow-exports/`](../nifi/flow-exports/), `nifi/backup/` |
| 설정·비밀값 | [`.env`](../.env), [`docker-compose.yml`](../docker-compose.yml) |

---

## 9. 리소스 ID (2026-07-31 실측)

| NiFi 그룹 | ID 앞 8자 | Airflow DAG |
|---|---|---|
| DZ (초기적재) | `9e2da75d` | `nifi_pipeline_9e2da75d_control` (스케줄 `56 12 * * *`) |
| DW (DZ→DW 가공) | `9e2dc2c9` | `nifi_pipeline_9e2dc2c9_control` |
| DZ_UPSERT (변경적재) | `c68c6a27` | `nifi_pipeline_c68c6a27_control` |
| http-ingest | `6894340e` | `nifi_pipeline_6894340e_control` |
| logfile | `59d6f6f7` | `nifi_pipeline_59d6f6f7_control` |
| 비정형 | `a7c2d807` | `nifi_pipeline_a7c2d807_control` |
| perf-test-scratch | `9f8083bf` | `nifi_pipeline_9f8083bf_control` |

컨트롤러 서비스: `cp-oracle-192-168-204-128`(원천) · `cp-tarantula-192-168-50-12`(타깃) ·
`cp-perf-oracle-50-91` · `cp-perf-pg-target` · `avro-reader`

Kafka 파이프라인: `LW_IMP018M` = ID 50 (TABLE_CDC) · `app-log-ingest` = ID 8 (LOG_FILE)

---

## 10. 반복해서 걸렸던 함정

- **NiFi 속성 키가 UI 라벨과 다름** — 설정 전 `component.descriptors` 확인 (`database-driver-locations` 등)
- **NiFi 그룹 임포트는 `/process-groups/upload` 멀티파트 전용** — 본문에 `versionedFlowSnapshot`을 보내면 HTTP 201인데 빈 그룹이 생긴다
- **ListenHTTP** — 하위 경로를 받지 않음(405), `mime.type` 속성을 만들지 않음, 헤더는 정규식 속성으로 받아야 함, multipart 버퍼 1MB
- **캔버스 작업 유실** — 메모리 압박 상황에서 컨테이너 재시작 시 사라진 적 있음. 작업 후 API로 실재 확인 + `nifi/backup/`에 export
- **Airflow 실행기 오류의 실제 원인은 호스트 OOM**(swap 고갈)이었다 — DAG 로직 검증은 파이썬 직접 실행으로
- **시간대별 차트는 원본 관측 테이블 기반** — 2026-07-26 이전 NiFi 구간이 비는 것은 정상
- **ST_USER 테이블은 읽기 전용** — 절대 쓰지 않는다
- **브랜치 승격 순서는 dev → test → main**, 각 단계에서 확인 후 진행
