# ETL 워크플로우 — 플랜 C: 확장 옵션 (필요 시 선택 적용)

> **전제**: 플랜 A 완료(필수), 항목별로 플랜 B 완료 여부 무관(각 항목에 명시).
> 전부 "있으면 좋은 것"이며 서로 독립 — 필요해진 순서대로 하나씩 얹는다.

---

## C-1. 최상위 일괄 실행 — `TRIGGER_WF` 노드

**요구**: "IMP를 한 번 실행하면 IMP_DAILY·IMP_MONTHLY가 다 돈다."

| 항목 | 내용 |
|---|---|
| 스키마 | **DDL 변경 없음** — `etl_workflow_node.node_type`에 `TRIGGER_WF` 값 추가, `sub_workflow_id` 재사용 |
| 컴파일 | `TriggerDagRunOperator(trigger_dag_id=자식 dag_id, wait_for_completion=True, deferrable=True, failed_states=["failed"])` — 자식 실패가 부모 태스크 실패로 전파 |
| 검증 추가 | V10: 자식이 자체 `schedule_cron`을 가지면 **이중 실행 경고**(부모 트리거 + 자기 스케줄) — 오케스트레이터 자식은 스케줄 NULL 권장. V11: TRIGGER_WF 순환 금지 |
| 화면 | 팔레트에 "워크플로우" 탭 추가(드래그 소스 = 게시된 워크플로우 목록) |
| 재시작 주의 | 부모에서 TRIGGER_WF 태스크를 clear하면 자식 DAG가 **처음부터 새 run** — "자식 내부 실패 지점부터"는 자식 DAG 화면에서 조작해야 함(2단계). UI에 안내 문구 |
| SUBWF와 구분 | SUBWF = 자식 그래프를 **TaskGroup으로 인라인 펼침**(한 DAG 안, 재시작 일원화) / TRIGGER_WF = **독립 DAG 호출**(자식이 독립 스케줄·이력 유지). 용도에 따라 선택 |

## C-2. 워크플로우 → 워크플로우 Asset 체이닝 (설계서 S4)

**요구**: "IMP_DAILY 끝나면 IMP_REPORT 자동 실행" — 트리거 강결합 없이.

| 항목 | 내용 |
|---|---|
| 스키마 | V59: `etl_workflow` + `produces_asset_uri VARCHAR(200)`, `consumes_asset_expr JSONB`(any/all 조합) |
| 컴파일 | 생산: **DAG 종단에 `wf_done` 태스크**(trigger_rule=all_success) + `outlets=[Asset(uri)]` — "태스크 성공=이벤트" 함정을 종단 태스크로 회피. 소비: `schedule=[Asset…]` 또는 `AssetAll/AssetAny` 조합(시간 스케줄과 택일, `AssetOrTimeSchedule`도 옵션) |
| 검증 추가 | V12: 소비 Asset의 생산자 존재 경고, Asset URI 규칙(`cerebro://etl/{workflow_key}`) 강제 |
| 화면 | 워크플로우 속성 패널에 "산출 Asset / 선행 워크플로우" 필드. 목록 화면에 체이닝 그래프(간단히) |
| 주의 | backfill과 부조화·상류 재실행 시 하류 재트리거(멱등이라 무해하나 인지 필요) — UI 안내 |

## C-3. 완료 대기 deferrable 전환 (성능 최적화)

**언제**: 동시 실행 워크플로우가 많아져 `mode="reschedule"` poke가 스케줄러/워커에 부담될 때.

- 커스텀 `BaseTrigger` 구현(async로 `GET /api/etl/job-runs/{token}` 폴링) → sensor `deferrable=True`
- 효과: 대기 중 워커 슬롯 0 점유(Triggerer로 오프로드). 플랜 B-1(콜백)과 결합 시 대기 자체가 초 단위라 우선순위 낮음
- 더 나아가면: 콜백 수신 시 백엔드가 Airflow REST로 **event를 직접 깨우는** 방식(AssetWatcher류)도 가능 — 필요성 확인 후

## C-4. 운영 마감 (품질·정리)

| 항목 | 내용 | 관련 |
|---|---|---|
| **OBSERVED 장기화 경고** | `completion_source='OBSERVED'` 비율이 임계 초과 시 알림 규칙 추가 — "콜백 배선이 빠졌거나 죽었다"를 자동 감지 | AlertEngine 규칙 1개 |
| **유령 DAG 정리 자동화** | `airflow_dag_catalog`에서 spec index에 없는 `etl_wf_*`/`nifi_pipeline_*` 행 주기 정리 | 검토 리포트 L-6 |
| **dag_id 승계 도구** | 레거시 `nifi_pipeline_*` 이력을 새 워크플로우로 잇고 싶을 때 `dag_id_override` 일괄 매핑 화면 | 플랜 A §7⑤ |
| **워크플로우 버전 이력** | `etl_workflow_revision`(published_spec 스냅샷 보관) — "누가 언제 무엇을 바꿨나" | Informatica 리포지토리 이력 대응 |
| **권한 세분화** | 게시(publish)를 별도 권한으로 분리(NIFI WRITE와 구분) 필요 시 | authz |

---

## 우선순위 제안

```
플랜 A → (플랜 B 병행 가능) → C-2(Asset 체이닝: 월배치→후속 리포트 등 실수요 생기면)
                            → C-1(일괄 실행: 실제 요구 확인되면)
                            → C-4(OBSERVED 경고는 B-1 진행 중 조기 적용 가치 있음)
                            → C-3(규모 커진 뒤)
```
