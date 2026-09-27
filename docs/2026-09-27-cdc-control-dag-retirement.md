# CDC 를 워크플로우로만 관리한다 — 제어 DAG 정리 검토 (2026-09-27)

**왜**: CDC 를 워크플로우(CDC 노드)로 관리하기로 하면서, **파이프라인마다 생기던 제어 DAG**
(`kafka_pipeline_{id}_control`)가 이제 필요 없지 않느냐는 물음이 나왔다. 함께, 워크플로우 로그 화면 상단의
«CDC / ETL» 두 갈래 현황도 워크플로우 단위 하나로 합치면 되지 않느냐는 물음도 같이 나왔다.

이 문서는 그 두 가지를 코드로 확인한 결과와, 확인 중 드러난 **실제 문제 두 건**을 적는다.

---

## 0. 먼저 — 2026-09-10 설계서에서 이 질문은 다뤄지지 않았다

[2026-09-10-cdc-workflow-node-design.md](2026-09-10-cdc-workflow-node-design.md) 를 다시 읽으면,
제어 DAG 는 **검토 대상이 아니라 출발점**이었다.

- §2 «그러나 필요한 단위 제어는 이미 한 층 아래에 있다» — 있는 것을 **활용하자**는 논지
- §5 실시간 모니터링 — «파이프라인 개별 제어(실행 설정)는 **그대로**»
- §6 정리 — ««지시»만 맡기면(B) **지금 구조를 그대로 살릴 수 있다**»

그때 답한 질문은 «워크플로우 태스크가 제어 DAG 를 부르고 **기다릴지 / 끝낼지**»였고,
«제어 DAG 를 **걷어낼 수 있는지**»는 물음 자체가 없었다. 사용자의 애초 의도(파이프라인별 DAG 를 정리하고
워크플로우로만 관리)와 설계 결론(기존 구조 유지 + 워크플로우를 얹기)이 어긋난 채로 구현이 진행됐다.

---

## 1. 지금 구조 (코드 확인)

```
[화면] CDC 관리                     [워크플로우] CDC 노드
   │  deploy 만 직접 API              │  TriggerDagRunOperator(wait_for_completion=False)
   │                                  ↓
   └────────────→ kafka_pipeline_{id}_control ──→ POST /api/pipelines/{id}/{action} ──→ Kafka Connect
                     apply_action
                     verify_action
                     verify_target_db_landing
                     monitor_cdc_runtime (센서)
```

- 화면이 백엔드를 직접 부르는 것은 **deploy 하나뿐**이다(`api/pipelines.ts:71` — «start/pause/stop/restart 는
  Airflow 가 전담»). 나머지 제어는 전부 제어 DAG 를 거친다.
- 워크플로우 CDC 노드도 **같은 제어 DAG 를 트리거**한다(`WorkflowCompiler:103`).
- 제어 DAG 는 60초 주기 Variable 동기화(`CdcDagSpecPublisher`)로 만들어진다 - **파이프라인 «생성 시점»과
  결합돼 있지 않다.** 무엇을 놓쳐도 다음 주기에 회복된다.

### 감시(monitor)의 실체는 이미 백엔드에 있다

`monitor_cdc_runtime` 의 docstring 이 직접 말한다.

> 실제 Kafka Connect 상태의 연속 실패 판정과 metadata-db 갱신은 백엔드의
> `KafkaPipelineStateSynchronizer` 가 담당한다.

센서가 하는 일은 셋뿐이다. ① start Run 을 CDC 수명만큼 살려 둬 **화면에 «실행 중»으로 보이게** 한다
② 백엔드가 FAILED 로 찍으면 DAG Run 을 빨갛게 만든다 ③ 감시 Run 중복 방지.
**제어가 아니라 표시(representation)** 기능이다.

---

## 2. 제어 DAG 를 걷어낼 수 있는가 — 가능하다

| 제어 DAG 가 하는 일 | 옮길 곳 |
|---|---|
| `POST /api/pipelines/{id}/{action}` 호출 | 워크플로우 CDC 노드가 직접(엔드포인트 이미 있음: start·pause·stop·deploy) |
| skip 판정(이미 DEPLOYED 면 건너뜀) | 백엔드 - 판정 근거가 백엔드 상태값이다 |
| verify_action · verify_target_db_landing · 지표 스냅샷 | 백엔드 스케줄러 또는 노드 |
| **monitor_cdc_runtime** | **없앤다** - 감시는 이미 백엔드가 한다 |

### 잃는 것

- **Airflow 에 CDC 실행 이력이 남지 않는다.** «누가 언제 start 를 눌렀나»는 `pipeline_command_history`
  (CDC 상세 → 제어 이력 탭)만 남는다. 그 탭은 2026-09-27 에 쓸 만하게 고쳤다(요청자 · 필터 · 페이징).
- **재시도 · 타임아웃을 Airflow 에 맡길 수 없다.** 백엔드에서 처리해야 한다.
- 기존 `kafka_pipeline_*_control` DAG 13개를 Airflow 에서 지워야 한다(실행 이력도 함께 사라진다).

### 규모

백엔드 1.5일(노드 실행 로직 + skip/verify 이관) · DAG 팩토리 반나절 · 화면 1일 · 검증 반나절 → **3~3.5일**.
지금 게시된 CDC 워크플로우는 `cdc_imp`(파이프라인 2개) 하나뿐이라 이것으로 전환 검증이 된다.

### 먼저 정해야 할 것

**CDC 를 워크플로우 없이 단독 운용하는 경우를 허용할 것인가.**
허용하면 CDC 관리 화면의 직접 제어가 남아야 하고, 워크플로우로만 관리하면 그 버튼들을 걷어내야 한다.

---

## 3. 현황 카드를 워크플로우 단위로 합치는 문제

지금 두 카드가 세는 것이 다르다.

| 카드 | 세는 대상 | 뜻 |
|---|---|---|
| CDC | `kafka_pipeline_*_control` | **파이프라인이 도는가** |
| ETL | `etl_wf_*` | **워크플로우 실행 결과** |

그런데 CDC 워크플로우(`etl_wf_cdc_imp`)는 `etl_wf_` 로 시작하니 **ETL 카드에 잡힌다**
(`utils/dagHistory.ts:12`). CDC 를 워크플로우로 관리하기 시작한 순간부터 두 갈래 구분은 이미 무너졌다.

**제어 DAG 를 걷으면 CDC 카드는 자연히 사라진다.** 그 카드가 세는 것이 제어 DAG 이고, «실행 중»의 정체가
monitor 센서 Run 이기 때문이다. 두 물음은 사실 하나였다.

다만 **CDC 파이프라인의 «지금 가동 중인가»는 워크플로우 실행 상태로 알 수 없다** - CDC 워크플로우는 설계상
«지시를 보내고 끝»이라 **항상 즉시 성공**으로 끝난다. 그 정보를 어디에 남길지 정해야 한다.

- (a) 현황 줄에 별도 표기: `실행 중 0 · 성공 0 · 실패 0 · 대기 0 │ CDC 파이프라인 2/2 가동`
- (b) CDC 관리 · CDC 로그 화면에만 두고, 워크플로우 로그 화면은 워크플로우만 보여 준다

---

## 4. 확인 중 드러난 문제 두 건

### 4-1. 워크플로우는 «성공»인데 CDC 는 시작되지 않았다 (2026-09-27 23:37 실측)

사용자가 `cdc_imp` 워크플로우를 실행했다. 화면은 태스크 2개 모두 성공.
**실제로는 두 파이프라인 다 시작되지 않았다.**

```
etl_wf_cdc_imp (워크플로우)          성공   ← "제어 DAG 를 불렀다"까지만 책임진다
  └ cdc_aaa_tb_imp018m.send         성공
       └ kafka_pipeline_2_control    실패   ← apply_action 에서 502
            └ POST /api/pipelines/2/start → 502 KAFKA_CONNECT_ERROR
```

- **왜 502 였나**: 소스 커넥터 태스크가 FAILED 라 백엔드가 시작을 거부하고 이전 상태로 되돌렸다
  (`pipeline_command_history` 에 «(이전 상태로 복구됨)» 으로 남아 있다).
- **태스크가 왜 FAILED 였나**(커넥터를 잠깐 깨워 trace 확보):

  ```
  io.debezium.DebeziumException: The connector is trying to read change stream starting at
  OracleOffsetContext [scn=31626389, …], but this is no longer available on the server.
  Reconfigure the connector to use a snapshot mode when needed.
  ```

  마지막 처리가 2026-08-24 인데 그 뒤 한 달 넘게 멈춰 있는 사이 **원천 Oracle 의 redo/archive 가 그 SCN 을
  지나가 버렸다.** 이어서 읽을 수 없으니 스냅샷을 다시 뜨거나 오프셋을 리셋해야 한다.
  파이프라인 3(`AAA-tb_imp060l`)도 같은 시각 같은 방식으로 실패했다. (확인 뒤 커넥터 · 파이프라인 상태는
  원래대로 STOPPED · READY 로 되돌려 두었다.)

- **구조 문제는 따로 있다**: 위 실패가 화면에서 «워크플로우 성공»으로 보였다는 점이다. 방안 B 의 대가다
  (설계서 §4 «작은 틈»). 제어 DAG 를 걷어내고 **노드가 직접 호출·판정**하게 되면 이 틈은 함께 사라진다 -
  이번 전환의 근거 중 가장 실질적인 것.

### 4-2. 남의 워크플로우 로그가 섞여 나온다

CDC 태스크 로그에 이런 줄이 찍힌다.

```
[dz_com_daily] 상위 워크플로우가 있어 자체 스케줄(10 14 * * *)은 끕니다 - 상위가 지시할 때만 돕니다
```

`dz_com_daily` 는 이 실행과 아무 관계가 없다. 원인은 이 문구가 `etl_workflows_dynamic.py` 의
**모듈 수준 print**(:365)라는 것이다. 워커가 태스크를 돌리려고 DagBag 을 채우면 그 파일 전체가 파싱되고,
**모든 워크플로우의 파싱 메시지가 그 태스크 로그에 섞인다**(로그 앞머리의 «Filling up the DagBag from
/opt/airflow/dags/etl_workflows_dynamic.py» 가 그 순간이다).

기능 문제는 아니고 **로그 잡음**이다. 고치려면 print 를 `logging.getLogger(...).debug(...)` 로 낮추면 된다
(파싱 진단이 필요할 때는 dag-processor 로그 수준을 올려서 본다).

---

## 5. 권하는 순서

1. **4-2 로그 잡음** - print → debug. 10분.
2. **전환 결정 두 가지** - ① CDC 단독 운용 허용 여부 ② CDC 가동 상태를 (a) 현황 줄 (b) CDC 화면 중 어디에.
3. **전환 구현** - 노드가 직접 호출 · skip/verify 백엔드 이관 · 제어 DAG 팩토리 제거 · 화면 정리(3~3.5일).
   4-1 의 «성공인데 실패» 틈이 이때 함께 닫힌다.
4. **기존 파이프라인 오프셋 복구**(4-1) - 전환과 별개다. 스냅샷 재수행 또는 오프셋 리셋을 정해야 한다.

## 6. 근거로 읽은 것

- `airflow/dags/kafka_pipelines_dynamic.py`(제어 DAG 팩토리 · `monitor_cdc_runtime`),
  `airflow/dags/etl_workflows_dynamic.py:345-372`(모듈 수준 파싱 메시지)
- `web/backend/.../workflow/WorkflowCompiler.java:103`, `pipeline/CdcDagSpecPublisher.java`,
  `pipeline/PipelineController.java`(start·pause·stop·deploy), `monitoring/KafkaPipelineStateSynchronizer.java`
- `web/cerebroetl-ui/src/api/pipelines.ts:71`, `utils/dagHistory.ts:9-22`
- 실측: `dag_run` · `task_instance`(2026-09-27 23:37), `pipeline_command_history`,
  Kafka Connect `/connectors/source-2-oracle-csb-tb_imp018m/status`
