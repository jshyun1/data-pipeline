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
| triggerer 없음 | `deferrable=True` 로 슬롯 문제를 피하려면 triggerer 서비스가 필요한데 `docker-compose.yml` 에 없다 (apiserver·scheduler·dag-processor 뿐) — **2026-09-17 해소**: `airflow-triggerer` 추가, 하위 워크플로우 노드 `deferrable=True` (WORK_LOG 38) |

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

## 7. 현재 반영 상태 — 2026-09-17 구현 완료 (방안 B)

상세는 WORK_LOG 43절. 위 5절 계획과 달라진 점·더한 점만 적는다.

| 항목 | 계획(5절) | 구현 |
|---|---|---|
| 마이그레이션 | V70 | **V77** `etl_workflow_node.pipeline_id` (V70 은 그사이 다른 용도로 쓰였다) |
| CDC 워크플로우 구분 | 명시 안 함 | 따로 저장하지 않고 **노드 구성에서 읽는다**(CDC 노드가 하나라도 있으면). 스펙 `kind=CDC`, DAG 태그 `cdc-workflow` |
| 게시 검증 | 연결선 금지, CDC 노드만 | V12: 파이프라인 필수·실재 / 연결선 금지 / CDC 노드만 / **스케줄 금지** / 같은 파이프라인 중복은 경고 |
| 액션 | deploy·start·stop·monitor | **start·stop·restart·deploy** — monitor 는 개별 파이프라인 전용 통로라 뺐다 |
| 상태 표시 | 집계 표 | 실시간 모니터링 속성창에 집계 + 파이프라인별 실제 상태 + 누르면 CDC 관리 상세 창이 바로 열림(`/cdc/pipelines?pipelineId=`) |
| 파이프라인 삭제 | 가드 필요 | 살아 있는 워크플로우에 얹혀 있으면 거부. 지운 워크플로우(소프트 삭제)의 노드 참조는 비우고 삭제 |

### 7-1. «여럿 중 하나만 실패」 조치 (4절 «작은 틈»의 답)

1. 알아채기 — 실패한 파이프라인의 제어 DAG 감시가 failed, 파이프라인 상태 FAILED, 워크플로우 속성창 «실행 상태: 실패»와 해당 줄 «실패».
2. 조치 — 그 줄을 눌러 CDC 관리 상세 창(Connector 탭: 재시작 · 싱크 설정 재적용 · 멈춘 한 건 건너뛰기). 나머지는 건드리지 않는다.
3. 일괄 복구 — 원인을 고친 뒤 워크플로우를 **restart** 로 실행. 백엔드 재시작이 «실패한 커넥터만» 다시 띄우므로 멀쩡한 파이프라인은
   «다시 띄울 커넥터가 없습니다»로 끝나고 실패한 것만 살아난다. 밀린 건은 멈춘 자리부터 이어서 적재된다(E2E 실측).

### 7-2. 구현하며 새로 막은 두 가지

- **할 일이 없으면 실패가 아니라 건너뜀** — 제어 DAG `skip_reason`: start 인데 DEPLOYED / stop 인데 STOPPED·READY·CREATED /
  restart 인데 DEPLOYED·FAILED 가 아님. 4절의 «중복 start → 제어 Run FAILED» 틈이 이것으로 닫혔다.
- **감시는 파이프라인당 하나** — 일괄 restart 가 이미 감시받던 파이프라인에 감시 Run 을 하나 더 붙여 `max_active_runs=2` 를 채우면
  이후 stop 이 큐에 갇힌다(검증 중 실측). 감시 센서가 `GET /api/airflow/dag-catalog/{dagId}/watch-runs` 로 «먼저 뜬 감시»를 확인하고
  늦게 뜬 쪽이 물러난다. 백엔드·화면의 «감시 중» 판정에 restart 도 포함했다.

### 7-3. 섞어서 설계할 때 (2026-09-17 추가 확인 · 수정)

CDC 노드와 ETL job·하위 워크플로우는 실행 방식이 다르다(ETL 은 순서대로 돌고 끝나기를 기다림, CDC 는 지시만 쏘고 끝).
섞이면 DAG 팩토리가 워크플로우 전체를 CDC 로 취급해 **스케줄·완료 신호가 꺼진다**(컨테이너에서 혼합 스펙으로 DAG 를 직접 만들어 확인 -
job 태스크와 send 가 병렬, NullTimetable, workflow_done 없음). 그래서 모든 경로에서 막는다.

| 경로 | 수정 전(실측) | 수정 후 |
|---|---|---|
| 설계 화면에서 ETL 워크플로우에 CDC 파이프라인을 끌어 놓기 | 놓아지고, 저장하면 **스케줄이 경고 없이 지워짐**(0 3 * * * → NULL) | 놓는 순간 경고·거부. 스케줄은 «전부 CDC» 일 때만 비움(이미 섞인 데이터도 저장 시 스케줄 유지 확인) |
| CDC 워크플로우에 ETL job 을 끌어 놓기 | 놓아짐(게시에서 V12 거부) | 놓는 순간 거부 |
| 하위 워크플로우 노드로 CDC 워크플로우를 넣기 | 검증 통과. 상위 실행 → 하위 CDC 가 conf 없이 떠 **action 기본값 start** → 파이프라인 READY→DEPLOYED(실측) | 화면에서 거부 + V8 거부. 팔레트에 «· CDC» 표시 |
| ETL 워크플로우의 선행 = CDC 워크플로우 | 검증 통과, CDC 는 완료 신호가 없어 **영영 안 돎** | V10 거부 |
| CDC 워크플로우에 선행 지정 | 검증 통과, 팩토리가 무시해 **자동 실행 안 됨** | V12 거부 |
| 다른 워크플로우의 하위/선행으로 쓰이는 워크플로우를 CDC 로 바꿔 게시 | (위 두 문제가 뒤늦게 생김) | V12 거부 |
| 섞인 워크플로우의 게시 검증 문구 | «CDC 워크플로우에는 CDC 노드만» + «스케줄 금지» (ETL 워크플로우에 엉뚱한 안내) | «섞을 수 없습니다(CDC n · 그 밖 m)», 적은 쪽 노드를 짚음, 섞였으면 CDC 전용 규칙은 내지 않음 |

### 7-4. 로그 화면 «이 작업만 실행»(CDC 노드)

수정 전 실측: 같은 run_id 로 제어 DAG 를 다시 띄우려다 `Dag Run already exists` 로 실패, **워크플로우 Run 전체가 failed**, 지시는 안 감.
수정: 재시도면 `manual__{상위 run}__{노드키}__retryN` 으로 새 제어 Run 을 만든다(첫 시도 이름은 그대로 - 화면이 이 이름으로 하위 실행을 찾는다).
기존 Run 을 되살리는 reset_dag_run 은 쓰지 않는다 - start Run 은 감시가 살아 있는 Run 이라 되살리면 감시가 끊긴다.
수정 후 실측: 재지시 Run `…__cdc_a__retry2` 생성 → 건너뜀 판정 → 워크플로우 Run success.

### 7-5. 남은 것

- 실시간 모니터링 트리에서 CDC 워크플로우는 ETL 가지 아래에 놓인다(dag_id 가 etl_wf_* 라서). 업무 구분·유형 표기는 CDC 로 고쳤다.
