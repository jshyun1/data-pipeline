# CDC 실패 처리 정책 전환 — «버리고 계속» → «멈추고 사람 부르기»

싱크 커넥터가 레코드 한 건을 거부했을 때의 기본 동작을 바꾸는 건에 대한 검토와 실행 계획.
**검토만 끝났고 아직 아무것도 바꾸지 않았다.** 다음 작업 때 이 문서대로 진행한다.

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
