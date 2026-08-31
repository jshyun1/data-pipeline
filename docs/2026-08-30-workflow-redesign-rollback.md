# ETL 워크플로우 재설계 — 백업 및 복구 런북 (2026-08-30)

> **목적**: `docs/2026-08-26-etl-workflow-design.md`의 ETL 워크플로우 캔버스 재설계에 착수하기 전,
> 언제든 착수 시점으로 되돌릴 수 있도록 만든 복귀 지점과 백업 자산, 그리고 복구 절차를 정리한다.
> **작성 시점 기준 커밋**: `52cbd61` (브랜치 `msa-integration`)

---

## 1. 복귀 지점

| 항목 | 값 | 비고 |
|---|---|---|
| **복귀 태그** | `pre-workflow-redesign` → 커밋 `52cbd61` | annotated tag. **로컬 전용(push 안 함)** |
| **작업 브랜치** | `feature/etl-workflow-canvas` | `msa-integration`(=`52cbd61`)에서 분기 |
| **보존 브랜치** | `msa-integration` | 재설계 작업이 닿지 않음 |

> 태그 객체 SHA(`d4b6299`)와 커밋 SHA(`52cbd61`)는 다르다. 커밋을 보려면 `git rev-parse pre-workflow-redesign^{commit}`.

**주의**: 작성 시점에 `msa-integration`에 **미푸시 커밋 4개**가 있었다(`52cbd61`, `af7c758`, `0e69edf`, `004a828`).
원격(`gitlab/msa-integration`)은 `6295dd6`이므로, 로컬 저장소가 유실되면 이 4개는 복구할 수 없다. **별도 push 권장.**

---

## 2. 백업 자산

모두 `backup/` 디렉터리. **`.git/info/exclude`에 `/backup/`을 추가**해 커밋 대상에서 제외했다(덤프에 운영 데이터가 들어 있으므로 리포지토리에 올리지 않는다).

| 파일 | 크기 | 내용 | 생성 방법 |
|---|---|---|---|
| `pipeline_meta-20260830-1635.sql` | 1.7M | **애플리케이션 메타DB** (72테이블, COPY 70블록 = 스키마+데이터). `etl_job`, `pipeline_definition`, `alert_rule`, `app_user`, `flyway_schema_history` 등 | `docker exec metadata-db pg_dump -U pipeline_app -d pipeline_meta` |
| `airflow_db-20260830-1638.sql` | 937K | **Airflow 메타DB** (68테이블). `dag_run`, `task_instance`, `variable` 등 **DAG 실행 이력** | `docker exec metadata-db pg_dump -U pipeline_app -d airflow` |
| `airflow-vars-20260830-1634.json` | 2.6K | Airflow Variable 2개 (CLI export 형식 — import로 바로 복원 가능) | `airflow variables export` |
| `nifi-conf-20260826-143701.tgz` | 4.7M | NiFi 설정 + **flow 정의 아카이브**(`archive/*_flow.json.gz`) — 이번 세션 생성분 아님(기존 자산) | (이전 세션) |
| `nifi-conf-20260820-185502.tgz` | 2.0M | 위와 동일, 더 이전 시점 | (이전 세션) |

### 중요: DB가 2개다
`metadata-db` **컨테이너 하나 안에 데이터베이스가 두 개**다.

```
metadata-db (컨테이너)
├── pipeline_meta   ← 애플리케이션 메타 (etl_job, alert_rule, ...)
└── airflow         ← Airflow 메타 (dag_run, task_instance, variable, ...)
```

`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = postgresql+psycopg2://***@metadata-db:5432/airflow`

`pipeline_meta`만 덤프하면 **DAG 실행 이력이 백업되지 않는다.** 위 표처럼 **두 개 모두** 떠야 한다.

---

## 3. 복구 절차

### S1. 코드만 되돌리기 (가장 흔한 경우)
```bash
cd /home/user/data-pipeline
git checkout msa-integration                 # 착수 시점 브랜치로
git branch -D feature/etl-workflow-canvas    # 작업 브랜치 폐기(선택)
```
태그 시점으로 직접 가려면: `git checkout pre-workflow-redesign`

### S2. 애플리케이션 메타DB 되돌리기
```bash
docker exec -i metadata-db psql -U pipeline_app -d pipeline_meta \
  < backup/pipeline_meta-20260830-1635.sql
```
> 이 덤프는 `--clean` 없이 생성됐다. 기존 객체와 충돌하면 대상 DB를 비우거나
> `pg_restore --clean` 상당의 처리가 필요하다. 가장 안전한 방법은
> `DROP DATABASE pipeline_meta; CREATE DATABASE pipeline_meta;` 후 복원(서비스 중단 필요).

### S3. Airflow 상태 되돌리기
```bash
# (a) Variable만 복원 — 가볍고 안전
docker cp backup/airflow-vars-20260830-1634.json airflow-apiserver:/tmp/v.json
docker exec airflow-apiserver airflow variables import /tmp/v.json

# (b) 실행 이력까지 통째로 복원 — Airflow 컨테이너 정지 후 수행
docker compose stop airflow-apiserver airflow-scheduler airflow-dag-processor
docker exec -i metadata-db psql -U pipeline_app -d airflow < backup/airflow_db-20260830-1638.sql
docker compose start airflow-apiserver airflow-scheduler airflow-dag-processor
```

### S4. NiFi flow 되돌리기 (범위 B에서 job 체인을 수정한 경우에만)
```bash
tar -xzf backup/nifi-conf-20260826-143701.tgz -C /tmp/nifi-restore
# archive/*_flow.json.gz 를 NiFi conf 디렉터리에 복사 후 NiFi 재기동
```
> flow 복원은 NiFi 정지 상태에서 해야 하며, 복원 시점 이후의 캔버스 변경은 모두 사라진다.

### S5. 전체 원복 (순서 중요)
```
1) 컨테이너 정지         docker compose stop pipeline-api airflow-* 
2) 코드 복귀             git checkout msa-integration
3) DB 복원               pipeline_meta → airflow 순서
4) 이미지 재빌드         docker compose build pipeline-api cerebroetl-ui
5) 기동                  docker compose up -d
6) 검증                  /api/health, Flyway 버전, DAG 목록
```

---

## 4. 백업이 **커버하지 않는** 것

정직하게 남긴다. 아래는 이번 백업으로 복구되지 않는다.

| 대상 | 왜 안 되나 | 필요 시 대응 |
|---|---|---|
| **Kafka Connect 커넥터 설정** | Kafka 내부 토픽 `_connect-configs`에 저장(RF=1). 덤프 대상 아님 | `curl localhost:8083/connectors/{name}/config` 로 개별 export, 또는 `pipeline_connector` 테이블(=`pipeline_meta` 덤프에 포함)에서 재배포 |
| **Kafka 토픽 데이터·오프셋** | `kafka-data` 볼륨. 덤프 대상 아님 | CDC는 소스에서 재스냅샷 가능하나 시간 소요 |
| **컨테이너 이미지** | 코드 복원 후 재빌드 필요 | `docker compose build` |
| **`.env` 시크릿** | 의도적으로 백업/커밋 제외 | 사용자 보관본 사용 |
| **NiFi flow 최신 상태** | `backup/`의 tgz는 **2026-08-26 시점**. 그 이후 캔버스 변경은 미포함 | 범위 B 착수 직전 새로 백업할 것 |

---

## 5. 착수 전 확인된 사실 (재설계에 영향)

백업 과정에서 드러난 현행 상태 — 전환 계획에 반영해야 한다.

1. **Airflow Variable이 2개뿐이며, 둘 다 캐시다.**
   - `nifi_process_groups_cache`, `nifi_root_connections_cache`
   - **`{dag_id}__schedule` / `__target_schema` / `__target_table` / `__target_timestamp_column` / `__auto_stop_after_run` 가 하나도 없다.**

2. 그 결과:
   - 모든 NiFi DAG가 **수동 트리거 전용**으로 동작 중(`schedule=None`).
   - `verify_target_db_landing`이 **매 실행 조용히 skip** 중일 가능성이 높다(target 3키 부재 → `AirflowSkipException`).
     → `docs/2026-08-20-integration-verification.md`의 **M2-5** 결함이 실제로 발현 중인 상태.

3. **전환에 유리한 점**: 이관하거나 잃을 스케줄 설정이 **존재하지 않는다.**
   기존 `nifi_pipelines_dynamic.py`를 제거해도 날아갈 운영 스케줄이 없고,
   남은 2개 캐시 Variable은 새 팩토리(spec Variable 기반)에서 불필요해진다.

---

## 6. 체크리스트

착수 전:
- [x] 복귀 태그 `pre-workflow-redesign` 생성
- [x] 작업 브랜치 `feature/etl-workflow-canvas` 분기
- [x] `pipeline_meta` 덤프
- [x] `airflow` DB 덤프
- [x] Airflow Variable export
- [x] `backup/` 을 git 추적에서 제외
- [ ] 미푸시 커밋 4개 push (**미완 — 유실 위험**)
- [ ] 범위 B 착수 직전 NiFi flow 재백업

복구 후 검증:
- [ ] `curl localhost:18086/api/health` → `{"status":"UP"}`
- [ ] Flyway 최고 버전이 복원 시점과 일치
- [ ] `airflow dags list` 로 DAG 목록 확인
- [ ] 화면 로그인 및 파이프라인/ETL 목록 조회
