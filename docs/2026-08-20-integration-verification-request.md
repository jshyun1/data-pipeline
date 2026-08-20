# 전체 통합 검증 요청서 (2026-08-20)

> 대상 브랜치: `msa-integration` (미커밋 authz WIP 포함)
> 산출물: `docs/2026-08-20-integration-verification.md`

---

## 0. 요청 취지

기능 단위로는 각각 동작하지만, **컴포넌트 사이의 접점**(NiFi / Kafka / Airflow / pipeline-api / metadata-db)이
서로 어긋나거나, 한쪽만 알고 있거나, 문서화되지 않은 암묵 합의에 기대고 있는 곳을 전수로 찾고 싶다.
ETL 솔루션이므로 **기능 정합성(A축)** 과 **안정성(B축)** 을 같은 비중으로 본다.

---

## 1. 범위와 단계

| 단계 | 내용 | 이번 요청 |
|---|---|---|
| 1단계 | **정적 대조** — 코드·DDL·compose·DAG·설정을 읽고 불일치를 찾는다. 스택 기동 불필요 | ✅ 이번 범위 |
| 2단계 | **실기동 E2E** — 1단계에서 나온 의심 지점만 실제로 돌려서 확인 | ⏸ 후보 목록만 산출 |

2단계를 나누는 이유: WSL2 VM 메모리가 7.6GiB라 13개 컨테이너 동시 기동이 불안정하다 (`WORK_LOG.md` §9).

**2단계 회차 분할 제안**
- 회차 A (경량, 5개 서비스: `kafka` + `kafka-connect` + `target-db` + `metadata-db` + `pipeline-api`)
  → B축 시나리오 1·2·3·4·5 검증 가능
- 회차 B (NiFi·Airflow·Oracle 추가) → B축 시나리오 8·9 및 A축 접점 1·2·3 검증

---

## 2. 기지(旣知) 사실 — 다시 보고하지 말 것

### 2.1 기존 문서로 이미 정리된 것
- `docs/2026-08-04-production-readiness-review.md`
  - NiFi↔Airflow 구조적 한계 3건 (완료 판정이 추측 / 건수가 카운터 증가분 추정 / 스케줄이 Airflow Variable에 산재)
  - DLQ, 데이터 정합성 검증, 시크릿 외부화, 감사로그, 백필, 계보
  - ⚠️ 단, **"이 지적사항들이 그 이후 코드에 실제로 반영됐는지"** 는 대조 대상에 포함한다.
- `WORK_LOG.md` §9 알려진 환경 이슈 (Testcontainers 미동작, WSL2 메모리, credsStore, Filebeat strict.perms 등)

### 2.2 이미 구현되어 있는 안정성 자산 — 없다고 지적하지 말 것
| 자산 | 위치 |
|---|---|
| DLQ + 승인 워크플로우 | `JdbcSinkTemplate.java:83-89` (`errors.tolerance=all`, `dlq.pipeline-{id}`), `V44__create_dlq_replay_request.sql` (risk_level / approved_by) |
| 전 서비스 자원 한도 | `docker-compose.yml` 각 서비스 `deploy.resources.limits` (memory + cpus) |
| 프로세스 감시 | `heartbeat/WatchdogService`, `heartbeat/HeartbeatService`, `infra/ProcessHealthService` |
| 보존/정리 | `retention/RetentionService`, `retention/PartitionMaintenanceService`, `PostgresReplicationCleanupService` |
| 정합성 검증 테이블 | `V43__create_pipeline_consistency_check.sql` (source_count vs target_count) |
| 스케줄러 격리 | `common/config/PlatformSchedulerConfig` (controlPlaneScheduler / batchScheduler) |

---

## 3. A축 — 기능 정합성: 검토 대상 접점 12개

| # | 접점 | 대조해야 할 것 |
|---|---|---|
| 1 | Airflow DAG → pipeline-api REST | `airflow/dags/kafka_pipelines_dynamic.py:50` 하드코딩 base URL. 호출하는 4개 엔드포인트(`/api/pipelines/{id}`, `/{id}/{action}`, `/api/connect/connectors/{name}/status`, `/{id}/metrics/snapshot`)가 실제 컨트롤러에 존재하는지 + 응답 스키마 일치 |
| 2 | Airflow DAG → NiFi REST | `airflow/dags/nifi_pipelines_dynamic.py` — `https://nifi:8443` 토큰 발급, `/nifi-api/counters`, `/flow/bulletin-board`, `/flow/process-groups/{id}` |
| 3 | Airflow Variable 산재 설정 | `{dag_id}__schedule`, `__target_schema`, `__target_table`, `__target_timestamp_column` — 코드 밖 설정. 누락 시 동작, 오타 시 무증상 실패 여부 |
| 4 | pipeline-api → Kafka Connect REST | 커넥터 이름 규칙, 상태 폴링 주기, 등록/삭제 실패 시 처리 |
| 5 | Debezium → 토픽명 → JdbcSink | 토픽 명명 규칙과 sink의 `table.name.format` 이 양쪽에서 동일 규칙을 쓰는지 |
| 6 | Filebeat 경로 | pipeline-api가 `filebeat-inputs` 볼륨에 YAML 기록 → 10s reload → Kafka → JdbcSink. **schema envelope는 문서화되지 않은 암묵 계약** |
| 7 | metadata-db 마이그레이션 48개 | FK 없는 설계에서 고아 레코드가 실제로 생기는 지점. V24 중복 이력, V25/V34/V35/V37~39 결번 사유 |
| 8 | 미러 테이블 ↔ 실체 | `airflow_dag_catalog`, `etl_job_catalog`, `nifi_*` 가 실제 Airflow/NiFi와 어긋날 때의 동작 |
| 9 | **authz 3중 정합 (미커밋 WIP)** | `V47/V48` 권한 데이터 ↔ 백엔드 컨트롤러 권한 검사 ↔ 프론트 `RequirePermissionRoute` 세 곳이 같은 권한 집합을 보는지 |
| 10 | **MSA 포털 ↔ Cerebro** | `192.168.50.30` 포털 `ST_USER` ↔ `app_user` 정합. 인가를 켜면 포털의 `/etls` 프록시가 401이 되는 미결 사안 |
| 11 | alert / notification 체인 | signal → rule → instance → notification 발송. 연쇄 억제, 재발송 정책, 심각도 분기 |
| 12 | 프론트 타입 ↔ 백엔드 DTO | `web/cerebroetl-ui/src/api/*` 의 타입 정의 ↔ 컨트롤러 실제 응답 |

### 찾아야 할 결함 유형 (A축)
- **불일치(mismatch)** — 같은 값이 두 곳에 하드코딩됐는데 다름 (토픽명, 테이블명, 포트, DAG id 규칙)
- **고아(orphan)** — 한쪽만 알고 다른 쪽은 모름 (미러 테이블에 있는데 실체 없음, DDL은 있는데 쓰는 코드 없음)
- **암묵 계약(implicit contract)** — 문서·코드 어디에도 안 적힌 채 양쪽이 합의한 형식
- **깨지는 조건(fragile)** — 정상 경로는 되는데 재시작·삭제·동시성·부분실패에서 깨짐
- **판정 근거 부재** — 성공/실패를 추측으로 결정하는 곳

---

## 4. B축 — 안정성

### 4.1 이미 눈에 띈 취약점 6건 — "그래서 실제로 언제 터지나"를 파고들 것

| 관측 | 근거 | 우려 |
|---|---|---|
| Kafka·Connect 내부 토픽 전부 `RF=1` | `docker-compose.yml:92,144-146` | 단일 브로커 전제. 디스크 손상 시 오프셋/커넥터 설정 전손 |
| `errors.tolerance=all` | `JdbcSinkTemplate.java:83` | 불량 레코드가 **조용히** DLQ로 빠짐. DLQ 적재가 알림으로 이어지지 않으면 "성공했는데 데이터가 빈" 상태 |
| `@Scheduled` 21개, 분산락 없음 | 20개 파일 | pipeline-api를 2대 띄우는 순간 전부 중복 실행. 단일 인스턴스가 암묵 전제 |
| 스케줄러 미지정 8개 | `PlatformSchedulerConfig.java:15` 주석 | 이름 폴백으로 controlPlane에 몰림. 느린 작업 하나가 알림 발송(5초 주기)을 밀어냄 |
| Hikari `connection-timeout: 3000` | `application.yml:11` | 스케줄러 다수 동시 기동 시 풀 고갈 → 관제 API가 3초 만에 실패 |
| `min.insync.replicas` / `unclean.leader.election` / retention 미지정 | compose에 없음 | 기본값 의존. 의도한 값인지 확인 필요 |

### 4.2 검증할 시나리오 9개

1. **재시작 내성** — 컴포넌트를 하나씩 죽였다 살렸을 때 재개 지점.
   상태 저장소가 **5곳에 분산**: Debezium SCN/LSN, Filebeat registry, Connect offset, NiFi flowfile repo, Airflow task state.
   → 동시에 죽었을 때 서로 어긋나는가?
2. **부분 실패 롤백** — `PipelineDeployService`가 source 커넥터 등록 성공 후 sink 등록 실패 시.
   고아 커넥터가 남는가, 보상 트랜잭션이 있는가.
3. **DLQ 가시성** — DLQ 토픽 적재를 `AlertEngine`이 감지하는가. 감지 못 하면 H1급 결함.
4. **삭제 전파** — 파이프라인 삭제 시 정리 대상 6곳(커넥터 / 토픽 / replication slot / publication / Filebeat YAML / DAG·미러 테이블)이
   모두 정리되는가. 중간에 하나 실패하면 나머지는?
5. **동시 조작** — 같은 파이프라인에 start와 delete가 동시에 도착할 때. 낙관적 락 / 상태머신 가드 유무.
6. **자원 고갈** — WSL2 7.6GiB에서 13개 컨테이너. `-Xmx384m` / limit 768m 인 pipeline-api가
   스케줄러 21개 + 메트릭 수집을 돌릴 때 OOM 여유. Kafka 로그 / NiFi content repo 디스크 증가 상한.
7. **역압(backpressure)** — 소스 폭주로 sink DB가 못 따라갈 때 어디가 먼저 터지는가.
   consumer lag → Kafka 디스크 → 브로커 다운 경로가 열려 있는지.
8. **스키마 변경** — 소스 테이블 컬럼 추가/삭제 시. `V45` column policy / `V46` masking policy와의 상호작용 포함
   (마스킹 대상 컬럼이 이름 변경되면 마스킹이 조용히 해제되는가?).
9. **판정 근거의 신뢰도** — `nifi_pipelines_dynamic.py`가 카운터 증가분으로 완료를 **추측**한다.
   이 추측이 틀렸을 때 downstream(알림 / 정합성 체크 / 대시보드 / `etl_job_run`)이 어떻게 오염되는가.

### 4.3 심각도 기준 — 데이터에 무슨 일이 일어나는가로 판정

```
H1 유실   소스에 있는데 타겟에 영영 없음 (복구 수단 없음)
H2 오염   타겟에 틀린 값이 들어감 (부분 커밋, 스키마 불일치, 마스킹 누락)
M1 중복   재처리로 중복 발생 (idempotent 보장되면 L로 강등)
M2 정지   멈췄는데 아무도 모름 (관측 부재)
L  지연   늦지만 결국 도착
```

코드 스멜·스타일·리팩터링 제안은 이번 범위에서 제외한다.

---

## 5. 추가로 특히 봐줄 것

- **시간대** — DB 세션 TZ(KST) vs UTC 혼용. 시간창 쿼리에서 과거에 물린 적 있음.
- **미커밋 authz WIP 포함** — 현재 `msa-integration` 브랜치의 수정 11개 + 신규 7개 파일 포함해서 검토.
- **pipeline-api 단일 인스턴스 전제가 깨지는 지점 전수** — 스케일아웃 시 무엇이 먼저 깨지는지 목록화.
- **관측 사각지대** — 장애가 났을 때 화면/알림 어디에도 안 나타나는 실패 유형이 있는가.

---

## 6. 산출물 형식

`docs/2026-08-20-integration-verification.md` 에 아래 형식으로:

| # | 항목 | 축 | 근거 (file:line) | 실패 시나리오 | 심각도 | 수정 제안 |
|---|---|---|---|---|---|---|

- 심각도 높은 순 정렬
- 근거는 반드시 `file:line` 로 명시 (추측이면 "추정"이라고 표기)
- 말미에 **2단계 실기동 E2E 후보 시나리오 목록** (회차 A / 회차 B 로 분류)
