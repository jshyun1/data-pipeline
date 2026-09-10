# CDC 노드를 워크플로우 캔버스에 얹는 방식 — 구체 설계

[2026-09-08 방안 분석](2026-09-08-cdc-workflow-design-options.md)에서 «캔버스로 간다»가
정해진 뒤, 실제로 어떻게 엮을지를 검토한 문서다. **검토만 끝났고 아직 구현하지 않았다.**

출발점이 된 질문 — 워크플로우에 CDC 노드 3개를 병렬로 두고 실행한 뒤, **그중 1개만 멈추고
조치하고 다시 시작**해야 한다. 돌고 있는 워크플로우 Run 안에서 태스크 하나에만 stop/deploy 를
주고, 조치 후 start 를 다시 줄 수 있는가?

---

## 1. 직답 — 태스크 단위 트리거는 불가능하다

Airflow 에서 지시(`params`/`conf`)의 단위는 **DAG Run 전체**다. 돌고 있는 Run 안의 태스크 하나에
새 파라미터를 줄 방법이 없다. 태스크에 할 수 있는 것은 셋뿐이다.

```
Clear           같은 conf 로 다시 돈다 (action 을 바꿀 수 없다)
성공/실패 표시   상태만 바꾼다
대기            그대로 둔다
```

`action=start` 로 뜬 Run 안에서 태스크 2 에만 `action=stop` 을 주는 것은 Airflow 모델상
성립하지 않는다. 코드를 더 짜서 만들 수 있는 것도 아니다.

## 2. 그러나 필요한 단위 제어는 이미 한 층 아래에 있다

파이프라인마다 제어 DAG 가 따로 있다(`airflow/dags/kafka_pipelines_dynamic.py`).

```
kafka_pipeline_1_control     파이프라인 1 전용
kafka_pipeline_2_control     파이프라인 2 전용   ← 여기에 stop 을 주면 2번만 멈춘다
kafka_pipeline_3_control
```

«3개 중 2번만 멈추고 → 조치 → 다시 시작»은 **워크플로우와 무관하게 지금도 된다.**
실시간 모니터링에서 파이프라인 2 우클릭 → 실행 설정 → stop → 조치 → start. 1·3 은 손대지 않는다.

그래서 진짜 설계 질문은 이것이다: **워크플로우의 CDC 태스크가 제어 DAG 를 «부르고 기다릴지»,
«부르고 끝낼지».**

## 3. 제어 DAG 의 액션별 동작 (실측)

```
apply_action >> verify_action >> [verify_target_db_landing, monitor_cdc_runtime]
```

`monitor_cdc_runtime` 센서(reschedule, 30초)가 마지막 leaf 다. 액션에 따라 끝나는 시점이 다르다.

| action | 센서 동작 | Run 이 끝나는 시점 |
|---|---|---|
| `deploy` | 즉시 True | 몇 초~분 |
| `stop` | 즉시 True | 몇 초~분 |
| `start` | 상태가 STOPPED 가 될 때까지 폴링 | **누군가 stop 을 줄 때까지 RUNNING** |
| `monitor` | start 와 같음 | 같음 |

`start` Run 은 CDC 수명만큼 산다. 이것이 아래 A 방식이 깨지는 원인이다.

## 4. 두 방식 비교

### A. 부르고 기다린다 — `wait_for_completion=True` (지금 SUBWF 방식)

워크플로우 Run 이 CDC 가 도는 동안 RUNNING 으로 남는다. 모니터링에서 자연스러워 보이지만
**지금 구성에서는 성립하지 않는다.** `start` 를 기다리는 태스크가 영원히 대기하기 때문이다.

| 문제 | 왜 |
|---|---|
| 워커 슬롯 영구 점유 | SUBWF 는 `deferrable=False` 라 대기 태스크가 슬롯을 쥔다. `AIRFLOW__CORE__PARALLELISM=4` 에서 2칸이 영구히 사라져 ETL 배치가 굶는다 |
| 3번째 노드가 영원히 대기 | `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2` — 3개 중 2개만 돌고 1개는 큐에서 못 나온다 |
| stop 지시가 교착 | start Run 이 RUNNING 인데 워크플로우 `max_active_runs` 기본 1 이라 stop Run 이 큐에 갇힌다. stop 이 돌아야 start 가 끝나는데, start 가 끝나야 stop 이 돈다 |
| triggerer 없음 | `deferrable=True` 로 슬롯 문제를 피하려면 triggerer 서비스가 필요한데 `docker-compose.yml` 에 없다 (apiserver·scheduler·dag-processor 뿐) |

triggerer 를 추가해도 아래 두 줄은 남는다. **A 는 권하지 않는다.**

### B. 부르고 끝낸다 — `wait_for_completion=False` (권장)

워크플로우 = **«N개 파이프라인에 같은 지시를 한 번에»** 쏘는 발사대.

```
워크플로우 실행 (action=start)
  ├─ CDC 노드 1 → kafka_pipeline_1_control 에 start 쏘고 즉시 완료
  ├─ CDC 노드 2 → kafka_pipeline_2_control 에 start 쏘고 즉시 완료
  └─ CDC 노드 3 → kafka_pipeline_3_control 에 start 쏘고 즉시 완료
워크플로우 Run 은 몇 초 만에 success
CDC 는 각자의 제어 DAG 가 계속 감시
```

출발점의 시나리오는 **B 에서 아무 추가 작업 없이 된다.** 2번만 멈추고 다시 켜는 건 제어 DAG
직접 조작이고, 워크플로우는 처음 «일괄 시작»만 했다. 워크플로우 Run 이 이미 끝나 있어서
«돌고 있는 Run 안의 태스크»라는 문제 자체가 없다.

「CDC 노드는 연결 불가」 전제와도 맞는다. 순서 없이 병렬로 쏘는 것이 전부다.

**B 의 대가** — 워크플로우 Run 의 success 는 «쏘기 성공»이지 «CDC 실행 중»이 아니다.
화면이 이를 오해하게 두면 안 된다. **CDC 워크플로우의 상태는 DAG Run 이 아니라 소속
파이프라인들의 런타임 상태를 모아서** 보여준다.

```
전부 DEPLOYED     → 실행 중
하나라도 FAILED   → 실패
전부 STOPPED      → 중지
섞임              → 일부 실행
```

**작은 틈** — 이미 도는 파이프라인에 워크플로우로 `start` 를 또 쏘면 백엔드가
«CDC 파이프라인이 이미 실행 중입니다. 중복 start는 허용되지 않습니다.» 로 거부하고
(`PipelineDeployService`), 그 제어 Run 은 FAILED 가 되는데 워크플로우는 이미 success 라 모른다.
파이프라인은 멀쩡히 도니 실해는 없고 위 집계 상태로 보면 된다.
`deploy`/`stop` 은 금방 끝나므로 **이 둘만 기다리게 해서 결과를 받아오는 정교화**가 가능하다
(`wait_for_completion` 은 생성자 인자라 액션별로 연산자를 갈라야 한다).

## 5. B 로 갈 때 필요한 작업

### 백엔드
- `etl_workflow_node` 에 `node_type = 'CDC'`, `pipeline_id` 컬럼 (마이그레이션 V70)
- 검증 규칙: CDC 노드는 연결선 금지, CDC 워크플로우에는 CDC 노드만 (ETL job·SUBWF 와 섞지 않음)
- 컴파일러(`WorkflowCompiler`): CDC 노드 → `{type: "CDC", pipeline_id, control_dag_id, name}`

### DAG 팩토리 (`etl_workflows_dynamic.py`)
- CDC 워크플로우에 `params.action = Param("start", enum=[deploy, start, stop, monitor])`
  — «트리거 4종류»가 여기에 붙는다
- CDC 노드 = `TriggerDagRunOperator(trigger_dag_id=control_dag_id, conf={"action": "{{ params.action }}"}, wait_for_completion=False)`
- `workflow_done`·Asset 신호 없음 (CDC 엔 «완료»가 없다), 스케줄 없음 (수동 전용)
- `trigger_run_id` 는 SUBWF 와 같은 규칙(`manual__{부모 run_id}__{node_key}`) — 예약 접두사 충돌 방지

### 설계 화면 (`WorkflowCanvasPage`)
- CDC 트리 항목을 끌어다 놓을 수 있게, `[cdc]` 배지 노드
- CDC 노드에는 연결선을 못 잇게 막기 (`onConnect` 가드 + 서버 검증 양쪽)
- CDC 워크플로우의 실행 → 액션 4개 선택
- 스케줄 구역 숨김

### 실시간 모니터링 (`AirflowDashboardPage`)
- CDC 워크플로우 상태 = 소속 파이프라인 런타임 상태 집계 (위 표)
- 파이프라인 개별 제어(실행 설정)는 그대로

### 규모
백엔드 하루, DAG 반나절, 화면 하루~이틀.

## 6. 정리

**태스크 단위 트리거는 안 되지만 필요하지도 않다.** 파이프라인 단위 제어는 제어 DAG 가
이미 하고, 워크플로우는 «일괄 지시»만 맡는다. 워크플로우가 CDC 의 «실행 상태»를 소유하려
들면(A) Airflow 의 자원 모델과 부딪히고, «지시»만 맡기면(B) 지금 구조를 그대로 살릴 수 있다.

## 7. 현재 반영 상태

- 설계 팔레트에 CDC 트리는 **보이게만** 해 두었다(선택·드래그 불가). 노드 종류에 CDC 가 없어
  지금 얹으면 저장은 되고 게시가 깨지기 때문이다.
- 나머지는 전부 미착수.
