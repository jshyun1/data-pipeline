# CDC 실패 처리 정책 전환 — «버리고 계속» → «멈추고 사람 부르기»

싱크 커넥터가 레코드 한 건을 거부했을 때의 기본 동작을 바꾸는 건에 대한 검토와 실행 계획.
**2026-09-17 전환했다.** 실측으로 전제 일부가 바뀌었다 - 결과 · 운영 절차 · 운영(MSA) 반영 순서는 [9절](#9-전환-결과-2026-09-17).

---

## 1. 지금 어떻게 되어 있나

[`JdbcSinkTemplate.addDlqSettings`](../web/backend/src/main/java/com/company/pipeline/connector/JdbcSinkTemplate.java)

```java
config.put("errors.tolerance", "all");                                  // 나쁜 건 버리고 계속
config.put("errors.deadletterqueue.topic.name", "dlq.pipeline-" + id);  // 버린 건 여기에
config.put("errors.deadletterqueue.context.headers.enable", "true");    // 원래 토픽·사유를 헤더에
config.put("errors.log.include.messages", "false");                     // 로그엔 원문 안 남김
```

## 2. 두 가지 실패는 서로 다르다

**① 커넥터가 멈추는 실패** — DB 접속 끊김, 커넥터 죽음, Kafka 장애.
오프셋이 그 자리에 멈추므로 **고치고 재시작하면 이어서 간다.** 여기는 문제가 없다.

**② 레코드 한 건이 거부되는 실패** — 외래키 위반, 타입 불일치, 컬럼 길이 초과.
`errors.tolerance = all` 이라 커넥터는 멈추지 않고 **그 건을 버리고 오프셋은 지나간다.**
다시 읽지 않으므로 **재시작해도 그 건은 영원히 안 들어온다** — 소스에는 있는데 타깃에는 없는 상태.

DLQ 토픽은 ②에서 버려진 원문을 담는 자리다. 오프셋으로는 복구가 불가능하기 때문에 존재한다.

## 3. 상용 CDC 는 어떻게 하나

구조(단계별 큐 + 체크포인트)는 우리와 같은 계보다. **기본 정책이 다르다.**

GoldenGate `REPERROR`:

```
ABEND     (기본)  Replicat 를 세운다. 운영자가 고치고 재시작
DISCARD           그 건만 버리고 계속 → discard 파일에 원문+사유
EXCEPTION         그 건을 «예외 테이블»에 INSERT 하고 계속
```

SharePlex 도 Post 적용 오류 시 기본적으로 Post 를 멈추고 오류 로그에 남긴다.

즉 **우리의 `tolerance = all` 은 상용 기준으로 `DISCARD` 를 기본으로 켜 둔 셈**이다.
우리 DLQ 토픽은 GoldenGate 의 discard 파일 / 예외 테이블과 같은 자리다.

| | 상용(SharePlex·GoldenGate) | 지금 우리 |
|---|---|---|
| 중단 후 재개 | 단계별 체크포인트 | Connect 오프셋 — **동일** |
| 한 건 거부 시 기본 | **멈춤**(ABEND) | **버리고 계속**(tolerance=all) |
| 버린 건 보관 | discard 파일 / 예외 테이블 | **DLQ 토픽 — 동일한 자리** |
| 재적용 | 운영자가 SQL 로 직접 | 백엔드 완성, **화면만 미연결** |
| 불일치 탐지 | compare / Veridata | 정합성 점검(부분적) |
| 실패 알림 | 오류 즉시 통보 | **규칙은 있으나 꺼져 있음** |

## 4. 결정 — `errors.tolerance = none` 으로 전환한다

**DLQ 는 필요 없어진다.** DLQ 설정은 `tolerance = all` 일 때만 쓰이므로 토픽에 아무것도
안 들어가고, 거부된 건은 오프셋이 지나가지 않아 **고치고 재시작하면 그 건부터 다시 들어온다.**

재처리 UI·승인 정책·멱등성 같은 복잡한 것이 통째로 필요 없어지는 것도 이점이다.
조용한 데이터 누락보다 시끄러운 정지가 낫다.

## 5. 전환 전에 반드시 갖춰야 할 세 가지

### ① 실패 알림을 먼저 켠다 (전제 조건)

```
CONNECTOR_FAILED     커넥터 FAILED     enabled = f    CRITICAL
SERVICE_UNREACHABLE  서비스 응답 없음   enabled = f    CRITICAL
```

**둘 다 꺼져 있다.** 이 상태로 전환하면 «조용히 버리기»가 «조용히 멈추기»로 바뀔 뿐이다.
며칠 뒤 "데이터가 안 들어와요"로 발견하게 된다. **이걸 켜지 않고는 전환하면 안 된다.**
수신자가 실제로 지정돼 있는지도 함께 확인한다.

### ② Kafka 보존 기간 안에 고쳐야 한다

멈추는 것은 **싱크 커넥터뿐**이다. 소스(Debezium)는 계속 돌며 Kafka 에 쌓는다.

```
소스 커넥터   계속 동작 → 토픽에 계속 적재
싱크 커넥터   FAILED 로 정지
                ↓
        보존 기간이 지나면 밀린 데이터가 삭제 → 진짜 유실
```

`docker-compose.yml` 에 보존 설정이 없다 → **Kafka 기본값 7일로 추정된다(실측 필요).**
7일 안에 고치면 무손실, 넘기면 잃는다. 보존을 14~30일로 늘리거나 최소한
«며칠째 멈춰 있음» 알림이 필요하다.

### ③ 고칠 수 없는 건의 탈출구

거부된 건이 **고칠 수 없는 데이터**일 수 있다(원본이 깨졌거나 타깃 제약이 그 값을 영원히
안 받거나). `all` 이면 알아서 건너뛰지만 `none` 이면 **그 자리에서 영구히 멈춘다.**

이때는 싱크 컨슈머 오프셋을 밀어 그 한 건을 건너뛰어야 하는데 **지금 그 기능이 없다.**
재시작은 있다 — [`KafkaConnectClient.restartTask`](../web/backend/src/main/java/com/company/pipeline/connector/KafkaConnectClient.java).

## 6. 진행 순서

```
1. CONNECTOR_FAILED 알림 켜기 + 수신자 확인          ← 이것부터. 없으면 전환 금지
2. Kafka 보존 기간 실측 → 14~30일로 상향
3. JdbcSinkTemplate 의 errors.tolerance 를 none 으로
   + DLQ 설정 제거, 기존 파이프라인 재배포(설정은 배포 시점에 굳는다)
4. CDC 로그의 «실패 데이터(DLQ)» 탭 정리
5. (나중에) 오프셋 건너뛰기 탈출구
```

3번은 **기존 파이프라인을 재배포해야 적용된다.** 커넥터 설정은 배포 시점에 확정되므로
템플릿만 고치면 이미 떠 있는 커넥터는 그대로다.

## 7. 남은 결정 — 재처리 백엔드를 지울까 남길까

DLQ 재처리 기능이 **백엔드에 이미 완성돼 있고 화면에만 안 붙어 있다.**

```
GET  /api/cdc/logs/dlq/replay-requests
POST /api/cdc/logs/dlq/replay-requests               재처리 요청
POST /api/cdc/logs/dlq/replay-requests/{id}/approve  승인
```

승인 정책도 들어 있다 — DELETE 이벤트는 HIGH 위험으로 **다른 관리자 승인 필수**(본인 승인 불가),
그 외는 관리자 즉시 실행·일반 사용자는 승인 대기, 같은 건 재요청은 차단(멱등성).
프론트 API 함수(`api/cdcLogs.ts`)까지 있으나 호출하는 화면이 없다.

`tolerance = none` 이면 안 쓰인다. 다만 나중에 특정 파이프라인만 «버리고 계속»으로
돌리고 싶어질 수 있어 되살리는 비용을 아끼려면 남기는 편이 싸다.
**화면에 안 붙은 코드가 계속 남는 부담과 저울질해서 정한다.**

## 8. 별건 — 불일치 탐지

상용은 `compare`/`repair`(SharePlex), Veridata(GoldenGate)로 **소스와 타깃을 직접 대조**한다.
DLQ 에도 안 남는 불일치가 있기 때문이다 — 초기 적재와 CDC 가 겹친 구간, 사람이 타깃을 직접
건드린 경우 등.

우리에겐 `pipeline_consistency_check` 와 정합성 점검이 부분적으로 있다.
이 방향을 키우는 것이 상용의 compare/repair 에 대응하며, 이번 전환과는 별개 과제다.

## 9. 전환 결과 (2026-09-17)

### 9-1. 바꾼 것

| 항목 | 내용 |
|---|---|
| 싱크 실패 정책 | CDC 테이블 싱크(일반 · 델타) `errors.tolerance = none` + DLQ 설정 제거 (`JdbcSinkTemplate.addStopOnErrorPolicy`) |
| 로그 파이프라인 싱크 | `all`(건너뛰고 계속) 유지, **DLQ 만 제거**. 줄 하나(긴 줄 등)로 모든 로그 적재가 멈추는 편이 더 해롭다고 판단 — 바꾸려면 `addSkipOnErrorPolicy` 대신 `addStopOnErrorPolicy` 한 줄 |
| 실패 알림 | `CONNECTOR_FAILED` 가 **태스크 FAILED 도** 잡게 고침(아래 9-2 ②). 로컬에서 `CONNECTOR_FAILED` · `SERVICE_UNREACHABLE` 규칙을 켬(관리 API) |
| Kafka 보존 | 실측 168시간(7일, 브로커 기본값) → `KAFKA_LOG_RETENTION_HOURS: 336`(14일) |
| 기존 커넥터 적용 | `POST /api/pipelines/{id}/sink-config/refresh` + CDC 관리 «상세 › Connector» 탭 «싱크 설정 재적용». 소스는 그대로, 실행 중이면 싱크 태스크만 새 설정으로 재시작돼 커밋된 위치부터 이어 읽는다. 탭에 현재 정책을 보여 주고 옛 정책이면 경고 |
| DLQ 백엔드 | `/api/cdc/logs/dlq*` 5개 API · `DlqReadService` · `DlqReplayService` · 엔티티 · DTO 삭제, **V75 로 `dlq_replay_request` 삭제**(로컬 0건 · 운영 0건 확인), 화면 API 함수 삭제 |
| 옛 DLQ 토픽 | 파이프라인 삭제 때 `dlq.pipeline-{id}` 도 함께 지우게 함(`KafkaTopicCleanupService`) |

### 9-2. 실측으로 바뀐 전제

로컬 `target-db` 에 실제 CDC 파이프라인(PostgreSQL 소스 → Kafka → JDBC 싱크, 타깃 컬럼 `varchar(5)`)을 앱 API 로 만들어 확인했다.

**① `errors.tolerance = all` 이어도 DB 가 거부한 건은 버려지지 않았다.** 옛 설정(all + DLQ)에서 긴 값을 넣자 싱크 **태스크가 FAILED 로 멈췄고 DLQ 는 비어 있었다**
(`Failed to flush records … value too long for type character varying(5)`). Debezium JDBC 싱크는 DB 쓰기(flush) 실패를 오류 처리기에 넘기지 않고
그대로 던진다. 2절 ②의 «외래키 위반 · 타입 불일치 · 길이 초과 → 조용히 버려짐» 은 이 커넥터에서는 사실이 아니었다.

`all` 이 실제로 버린 것은 **변환 단계 오류**다 — 역직렬화가 안 되는 메시지를 토픽에 넣자 옛 설정은 DLQ 로 빼고 알림 없이 계속 갔고(조용한 누락 재현),
새 설정은 `Tolerance exceeded in error handler` 로 멈췄다. 그러니 이번 전환의 실제 효과는:

- 변환 오류(깨진 메시지 · 스키마 불일치 · SMT 실패)의 조용한 누락이 없어졌다
- **DB 거부로 멈추는 것은 전환 전에도 이미 일어나고 있었다** → 그래서 ②가 더 중요했다

**② 그 멈춤을 알림이 못 잡고 있었다.** 싱크가 멈추면 커넥터는 RUNNING, **태스크만** FAILED 가 된다. `CONNECTOR_FAILED` 는 커넥터 상태만 봐서
울리지 않았다. 런타임 감시가 10초마다 적는 `pipeline_connector.last_status_json` 의 태스크 상태를 함께 보게 고쳤고, 시험에서
«zz-정책전환-시험 — Sink 태스크 FAILED»(CRITICAL)가 약 20초 안에 떴다가, 고친 뒤 300초 정상 유지 후 자동 해소됐다.

### 9-3. 운영 절차 — 싱크가 멈췄을 때

알림 «… Sink 태스크 FAILED» → CDC 관리 «상세» 의 운영 상태 · Connector 실행상태(trace), CDC 로그의 «런타임 감시 실패» 줄(GPT 분석 가능)에서 원인을 본다.
멈춘 뒤 런타임 감시가 파이프라인을 **FAILED** 로 바꾼다(그 상태에서 시작 · 중지는 거절되고 **재시작 · 배포**가 된다).

**화면 절차와 자세한 설명: [2026-09-17 안내 문서 5절](2026-09-17-cdc-failure-and-subworkflow-guide.md#5-복구--화면에서-하는-방법)**

| 상황 | 방법 |
|---|---|
| 고칠 수 있는 원인(컬럼 길이 · 제약 · 접속) | 원인을 고치고 **워크플로우 › 로그 → 실행 설정 → «재시작 (Restart)»** — 멈춘 건부터 다시 들어온다 |
| 커넥터 설정 자체가 잘못됨 | **실행 설정 → «배포» → «시작»** — 설정을 다시 넣고 세운다 |
| 고칠 수 없는 한 건(깨진 메시지 등) | **CDC 관리 → 상세 → Connector → «이 한 건 건너뛰기»** — 내용 · 원인을 보고 사유를 적으면 그 건을 넘기고 자동으로 다시 띄운다. **넘긴 건은 타깃에 안 들어간다** |

API 로 직접 부르려면 `POST /api/pipelines/{id}/restart`, `GET /api/pipelines/{id}/sink-stuck`,
`POST /api/pipelines/{id}/sink-stuck/skip`(topic · partition · offset · reason). 409 «rebalance was underway» 면 잠시 뒤 다시 부른다.

> 2026-09-17 저녁 추가. 그 전에는 화면 복구 수단이 없어 명령어로만 했다 - 그때 쓰던 `kafka-consumer-groups --reset-offsets --shift-by 1` 절차는
> 이제 «한 건 건너뛰기» 버튼이 대신한다(같은 일을 한다).

### 9-4. 운영(MSA) 반영 순서

운영은 2026-09-17 기준 DEPLOYED 파이프라인 3개(14 · 15 · 22), `dlq.*` 토픽 6개, `CONNECTOR_FAILED` 켜짐 · `SERVICE_UNREACHABLE` 꺼짐 ·
활성 수신자 2명(읽기 전용 확인).

1. **알림 먼저** — `SERVICE_UNREACHABLE` 을 켤지 결정하고 수신자를 확인한다(`CONNECTOR_FAILED` 는 이미 켜짐). 사람이 일부러 끈 설정일 수 있어 마이그레이션으로 강제로 켜지 않았다
2. 백엔드 · 화면 배포(V75 가 `dlq_replay_request` 를 지운다)
3. `docker-compose.yml` 반영 후 `kafka` 재생성(보존 14일, 잠깐 끊겼다 Connect 가 다시 붙는다) — 디스크 여유 확인
4. 파이프라인 14 · 15 · 22 각각 «상세 › Connector › 싱크 설정 재적용» → 탭 경고가 «멈춤» 으로 바뀌는지 확인
5. 옛 `dlq.pipeline-*` 토픽은 비었는지 확인 후 수동 삭제(파이프라인을 지울 때는 자동으로 지워진다)

### 9-5. 남은 것

- 5절 ③ 탈출구의 화면 기능(오프셋 건너뛰기) — **2026-09-17 저녁에 만들었다**(9-3). 건너뛴 레코드 원문 보관과 «다른 관리자 승인» 여부는 정하지 않았다
- «며칠째 멈춰 있음» 단계 알림 — 멈춰 있는 동안 `CONNECTOR_FAILED` 가 30분(renotify 1800초)마다 다시 알리지만, «보존 14일이 곧 끝난다» 같은 단계 경고는 없다