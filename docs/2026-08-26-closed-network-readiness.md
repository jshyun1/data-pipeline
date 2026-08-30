# 폐쇄망 서버 이관 — ETL/CDC 성능·안정성 분석 (2026-08-26)

다른 폐쇄망에 서버로 반입할 때의 성능·안정성 평가. 추정이 아니라 **현재 리포의 설정값과
로컬 스택 런타임 실측**을 근거로 한다.

**한 줄 결론** — 기능은 그대로 동작한다. 다만 현재 구성은 *단일 노드 개발 스택*이라
운영 서버에서는 ①메모리 산정 ②데이터 유실 지점 ③2대 배포 시 중복 실행이 먼저 문제가 된다.

---

## 1. 리소스 — 서버 사양 산정

`docker-compose.yml`의 `deploy.resources.limits` 합계 = **9.0 GB**.

| 컨테이너 | 메모리 상한 | 실측(유휴 수준) | 점유율 |
|---|---|---|---|
| nifi | 3,072 MB | 1,001 MB | 33% |
| kafka-connect | 1,280 MB | 603 MB | 47% |
| airflow-scheduler | 1,024 MB | 230 MB | 22% |
| pipeline-api | 768 MB | 481 MB | 63% |
| kafka | 640 MB | 402 MB | 63% |
| airflow-apiserver | 512 MB | 310 MB | ⚠️ 61% |
| **airflow-dag-processor** | **448 MB** | **371 MB** | 🔴 **83%** |
| metadata-db | 384 MB | 94 MB | 25% |
| airflow-init | 384 MB | (1회성) | - |
| filebeat | 192 MB | 65 MB | 34% |
| cerebroetl-ui | 96 MB | 14 MB | 15% |

> 실측은 파이프라인이 거의 없는 상태의 값이다. 실부하에서는 위로 움직인다.

**권장 물리 메모리: 16 GB.** 9 GB는 컨테이너 상한 합계일 뿐이고 OS·페이지캐시·docker
데몬이 별도다. 12 GB는 빠듯하고, 같은 호스트에 다른 솔루션이 올라가면 부족하다.

### 🔴 즉시 조정 대상

**(a) `airflow-dag-processor` 448 MB — 유휴에도 83% 상주**
DAG 파일 수에 비례해 늘어나는 프로세스다. DAG는 파이프라인 수만큼 생기는 구조
(`nifi_pipeline_*_control`)라 고객 규모에서 확실히 증가한다. → **768 MB**

**(b) NiFi CPU 상한 2.0 cores — 유휴에 이미 106% 상주**
파이프라인이 거의 없는데도 1코어를 상시 소진 중이다. 실부하가 붙으면 상한에 걸린다.
→ **4 cores**. 별도로 유휴 CPU 소진 원인 규명 필요
(`nifi.bored.yield.duration=10 millis`가 의심 지점).

**(c) 힙 ↔ 컨테이너 상한 불일치**

| | JVM 힙 | 컨테이너 상한 |
|---|---|---|
| NiFi | 1 GB (`NIFI_JVM_HEAP_MAX`) | 3 GB |
| pipeline-api | 384 MB (`-Xmx384m`) | 768 MB |
| kafka-connect | 768 MB (`CONNECT_HEAP_OPTS`) | 1.25 GB |
| kafka | 512 MB (`KAFKA_HEAP_OPTS`) | 640 MB |

NiFi가 3 GB를 받는데 힙은 1 GB뿐이라 대용량 처리 시 힙이 먼저 터진다.
→ 서버에서는 `NIFI_JVM_HEAP_MAX=2g`.

---

## 2. 안정성 — 데이터 유실 지점 (가장 심각)

### (a) 🔴 Kafka 단일 브로커 + 복제 없음

```
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
```

브로커 1대 + RF=1 → **디스크 손상 시 복구 불가**. 특히 `_connect-offsets`가 날아가면
CDC가 어디까지 읽었는지를 잃어 **전체 재적재 또는 데이터 구멍**이 된다.

폐쇄망 단일 서버에서 브로커 3대는 현실적이지 않으므로 완화책:
- Kafka 데이터 볼륨을 **RAID 또는 별도 물리 디스크**에 배치
- `_connect-offsets` 정기 백업 절차 수립

### (b) 🔴 DLQ 무통보 유실 *(백로그 #1)*

```java
// JdbcSinkTemplate.java:83-86
errors.tolerance = all
errors.deadletterqueue.topic.name = dlq.pipeline-{id}
```

적재 실패 레코드를 조용히 DLQ로 보내고 **파이프라인은 계속 성공으로 표시**된다.

**조회·재처리 수단은 있다**(2026-08-30 정정) — `CdcLogController` 에 `/api/cdc/logs/dlq`,
`/dlq/detail`, `/dlq/replay-requests`(승인 절차 포함)가 구현돼 있다.
**없는 것은 «자동 감시»다** — `alert` 패키지에 DLQ 참조가 0건이라 DLQ 에 쌓여도
알림이 발생하지 않는다. 즉 **사람이 화면을 열어보지 않으면 모른 채로 지나가고**,
Kafka 기본 보존 7일이 지나면 **영구 유실**된다.

폐쇄망은 고객사 데이터라 재수집이 불가능할 수 있어 위험도가 더 높다.

### (c) 🔴 replication slot 고아 *(백로그 #5)*

`PipelineService.java:195-223`. 파이프라인 삭제가 실패하면 원천 DB의 replication slot이
남는다. Postgres는 slot이 살아 있는 동안 **WAL을 무한 보유**한다.
→ **고객사 원천 DB 디스크 포화**. 우리 솔루션이 고객 운영 DB를 죽이는 시나리오.

### (d) 🔴 알림이 실제로 나가지 않는다 *(백로그 #2)*

```yaml
notification.email.enabled: ${NOTIFICATION_EMAIL_ENABLED:false}
notification.sms.enabled:   ${NOTIFICATION_SMS_ENABLED:false}
```

화면에서 채널을 켜도 실제 발송은 이 환경변수만 본다. **폐쇄망 기본값은 false**라
위 (a)(b)(c)가 터져도 아무도 모른다. **안전망 3개가 전부 무력화된 상태로 나가는 셈**이라
이 항목이 사실상 1순위다.

---

## 3. 확장성 — 2대 이상 배포 시 즉시 깨짐

**ShedLock(분산락)이 없다**(코드 확인). 그런데 `@Scheduled`가 20개 이상 돈다:

| 주기 | 용도 |
|---|---|
| 5초 | 제어 플레인 |
| 15 / 20 / 30초 | 상태 수집 |
| 1분 | NiFi 캔버스 감사, 워치독, 메트릭 수집 |
| 1시간 | 보존 정리 |
| 6시간 | 파티션 유지보수 |
| 일 1회 | 감사 로그 보존 정리 |

2대로 띄우면 전부 2배로 돈다 → **알림 이중 발송 · 파이프라인 이중 제어 · 적재 건수 이중 집계**.
MSA 포털이 2대를 프록시하면 바로 현실화된다.

**결론: 현재 구조는 단일 인스턴스 전용.** 이중화가 요구사항이면 ShedLock 도입이 선행돼야 한다.

---

## 4. 디스크

| 항목 | 상한 | 평가 |
|---|---|---|
| 컨테이너 로그 | 50MB × 5 × 13서비스 ≈ 3.2 GB | ✅ 상한 있음 |
| NiFi provenance | 10 GB / 30일 | ✅ |
| NiFi flow archive | 500 MB / 30일 | ✅ |
| **NiFi content archive** | **디스크의 90%** | 🔴 위험 |
| metadata-db | 보존정책 11종 정상 동작 | ✅ |

### 🔴 `nifi.content.repository.archive.max.usage.percentage=90%`

NiFi가 **디스크를 90%까지 채운 뒤에야** 정리를 시작한다. 다른 솔루션과 디스크를 공유하는
서버라면 **그 솔루션들이 먼저 죽는다.** → 50~60%로 낮추거나 NiFi 전용 볼륨 분리.

### ✅ 보존 정리는 정상 동작 확인

`retention_policy` 11종 전부 2026-08-26 02:40 실행, `last_error` 없음.

| 테이블 | 보존 | 방식 |
|---|---|---|
| infra_resource_sample | 3일 | PARTITION_DROP |
| alert_signal_sample | 7일 | DELETE_BATCH |
| pipeline_metric_snapshot | 30일 | PARTITION_DROP |
| nifi_execution_log / nifi_processor_run / notification_delivery | 90일 | DELETE_BATCH |
| alert_instance / collector_outage | 180일 | DELETE_BATCH |
| pipeline_command_history | 365일 | DELETE_BATCH |
| infra_resource_rollup | 730일 | DELETE_BATCH |
| pipeline_load_rollup | 1095일 | DELETE_BATCH |

파티션도 `p202609 / p202610 / p202611 + pdefault`로 3개월치 선생성돼 있다.
현재 DB 총량 18 MB(최대 테이블 `infra_resource_sample_pdefault` 2.2 MB).

---

## 5. 폐쇄망 특유의 이슈

**✅ 잘 되어 있는 것**
- 모든 의존성 다운로드가 **빌드 시점에만** 발생. 런타임은 `docker load` + `docker compose up`만
- `offline/` 반입 도구 일체 (`save-images.sh` / `load-images.sh` / `image-manifest.json`)
- front-proxy도 기존 `cerebroetl-ui` 이미지를 재사용 → **반입 이미지 목록 불변**

**⚠️ 사전 확인 필요**
- **SMTP 릴레이** — 폐쇄망에 메일 서버가 있어야 알림이 나간다. 없으면 SMS 게이트웨이 필요
  (`NOTIFICATION_SMS_GATEWAY_URL`)
- **P5b mTLS 인증서** — 신규 환경마다 발급 + NiFi truststore 등록 필요(자동화 안 됨).
  `docs/p5b-personal-accounts-runbook.md`
- **NiFi 서버 인증서 2028-11-19 만료** — 자동 갱신 안 됨. 갱신 시 truststore가 초기화되며
  `cerebro-proxy-ca`가 함께 사라지는 함정 있음(재등록 필수)

---

## 6. 우선순위

| | 항목 | 이유 | 규모 |
|---|---|---|---|
| 🔴 1 | **알림 발송 활성화** (#2) | 나머지 전부의 전제. 없으면 장애를 아무도 모름 | S |
| 🔴 2 | **DLQ 자동 감시·알림** (#1) | 조회 화면은 있으나 알림이 없어, 안 보면 7일 뒤 유실 | S~M |
| 🔴 3 | **dag-processor 448→768MB, NiFi CPU 2→4** | 유휴에 83%/106%. 실부하에서 확실히 부족 | XS |
| 🟠 4 | **NiFi content archive 90%→60%** | 다른 솔루션 동거 시 동반 장애 방지 | XS |
| 🟠 5 | **replication slot 고아 정리** (#5) | 고객 원천 DB 보호 | M |
| 🟡 6 | **ShedLock 도입** (#13) | 이중화 요구가 있을 때만 | M |
| 🟡 7 | **NiFi 힙 1→2GB** | 대용량 처리 대비 | XS |

**3·4·7은 `.env` / 설정값 변경뿐이라 즉시 반영 가능하다.**
1·2가 실제 개발이 필요한 부분이고, 폐쇄망 반출 전에 끝내는 것이 맞다.

관련: 42건 백로그 원본 `docs/2026-08-20-integration-verification.md`,
남은 작업 전반 `docs/2026-08-24-remaining-work-handoff.md`.
