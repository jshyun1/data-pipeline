# NiFi 플로우 전달 묶음 (2026-08-14 기준)

로컬 NiFi 2.2.0 캔버스에서 그룹 단위로 다시 뽑은 flow definition이다.
전부 `includeReferencedServices=true`로 받아서 그룹이 참조하는 컨트롤러 서비스가
파일 안에 들어 있다(`externalControllerServices`는 전부 비어 있음 - 확인 완료).

| 파일 | 그룹 | 프로세서 | 커넥션 | 비고 |
|---|---|---|---|---|
| `DZ.json` | DZ (초기적재) | 20 | 20 | 출력포트 5, CS 3 |
| `DW.json` | DW (DZ→DW 가공) | 15 | 15 | 입력포트 5, CS 2 |
| `DZ_UPSERT.json` | DZ_UPSERT (변경적재) | 20 | 28 | 파라미터 컨텍스트 `DZ_UPSERT` 11개 포함 |
| `unstructured.json` | 비정형 | 18 | 25 | 하위그룹 5, CS 6 |
| `http-ingest.json` | http-ingest | 2 | 1 | ListenHTTP |
| `logfile.json` | logfile | 5 | 4 | CS 4 |

**제외한 그룹**: `PERF-N1-ORA-TO-PG` / `PERF-N2-PG-TO-PG` / `PERF-N3-PG-TO-ORA` /
`PERF-N4-ORA-TO-ORA` / `perf-test-scratch`. 성능측정용 임시 그룹이라 뺐다. 필요하면
아래 명령의 그룹 id만 바꿔서 같은 방식으로 뽑으면 된다.

**`nifi/backup/flow-*.json.gz`는 전달하지 말 것.** 그건 캔버스 전체 스냅샷이라
복원하면 받는 쪽 NiFi의 모든 그룹이 통째로 교체된다.

## 뽑은 명령

```bash
TOKEN=$(curl -sk -X POST https://localhost:18443/nifi-api/access/token \
  -d 'username=admin' --data-urlencode "password=$NIFI_SINGLE_USER_PASSWORD")

curl -sk --http1.1 -H "Authorization: Bearer $TOKEN" \
  "https://localhost:18443/nifi-api/process-groups/{그룹ID}/download?includeReferencedServices=true" \
  -o DZ.json
```

`--http1.1`이 없으면 100KB 넘는 응답에서 HTTP/2 framing 오류로 끊긴다(`curl: (16)`).

| 그룹 | 로컬 그룹 id |
|---|---|
| DZ | `9e2da75d-019f-1000-1f21-9e2ab492cf11` |
| DW | `9e2dc2c9-019f-1000-9b51-5735a6840d95` |
| DZ_UPSERT | `c68c6a27-019f-1000-705c-a58368837909` |
| 비정형 | `a7c2d807-019f-1000-765c-b6ffc73d6e3a` |
| http-ingest | `6894340e-019f-1000-4a66-a0ddf2ad58f4` |
| logfile | `59d6f6f7-019f-1000-3a91-49b23eaf89a1` |

## 받는 쪽에서 가져오는 방법

### UI

캔버스 빈 곳에 **Process Group 아이콘 드래그** → 이름 칸 옆 **browse** 버튼으로 JSON 선택 → `Add`.

### REST API

```bash
ROOT=<대상 NiFi 루트 그룹 id>
curl -sk --http1.1 -X POST \
  -F "id=$ROOT" -F "groupName=DZ" -F "positionX=-360" -F "positionY=300" \
  -F "clientId=$(cat /proc/sys/kernel/random/uuid)" \
  -F "disconnectedNodeAcknowledged=false" \
  -F "file=@DZ.json;type=application/json" \
  "https://<host>/nifi-api/process-groups/$ROOT/process-groups/upload"
```

`POST /process-groups/{id}/process-groups` 본문에 `versionedFlowSnapshot`을 넣는 방식은
쓰면 안 된다. NiFi 2.2.0은 그 필드를 무시하고 **이름만 같은 빈 그룹**을 만든다(HTTP 201이
떨어져서 성공한 것처럼 보인다).

## 가져온 뒤 반드시 해야 하는 것

### 1. DB 비밀번호 재입력 + 컨트롤러 서비스 Enable

**비밀번호는 스냅샷에 담기지 않는다**(속성 자체가 빠짐 - 이번 묶음도 확인 완료).
접속 URL·사용자명은 들어 있으니 비밀번호만 채우면 된다.

그룹 우클릭 → Configure → Controller Services:

| 서비스 | 넣을 것 |
|---|---|
| `cp-tarantula-192-168-50-12` | 타란툴라DB 비밀번호 |
| `cp-oracle-192-168-204-128` | 원천 Oracle 비밀번호 (DZ / DZ_UPSERT) |

Enable을 안 하면 프로세서가 전부 invalid로 뜨는데 사유가 "Controller Service is disabled"라
다른 문제로 오해하기 쉽다.

### 2. DZ → DW 루트 커넥션 5개 다시 잇기

그룹과 그룹 "사이"의 연결이라 어느 쪽 스냅샷에도 들어가지 않는다. 이름이 같은 짝끼리
이어야 한다 - **다른 짝을 이어도 NiFi는 막지 않고, 에러 없이 엉뚱한 테이블에 적재된다.**

```
DZ to-dw-COM001M  →  DW from-dz-COM001M
DZ to-dw-COM002L  →  DW from-dz-COM002L
DZ to-dw-COM003M  →  DW from-dz-COM003M
DZ to-dw-COM004M  →  DW from-dz-COM004M
DZ to-dw-POP003L  →  DW from-dz-POP003L
```

이 연결은 Airflow 제어 DAG가 "DZ 다음 DW"를 판단하는 근거이기도 하다
(`airflow/dags/nifi_pipelines_dynamic.py`의 `build_group_chain`).

### 3. 적재 대상 테이블 만들기

비정형 그룹은 `unstructured` 스키마가 있어야 동작한다.

```bash
psql -h <타란툴라DB> -U <계정> -d postgres -f db/tarantula-init/01_unstructured_schema.sql
```

### 4. 포트 충돌 확인

`ListenHTTP-unstructured`가 컨테이너 안에서 **8444**, `http-ingest`가 **8442**를 연다.
받는 쪽에서 이미 쓰는 포트면 프로세서 시작 시 bind 실패로 죽는다.

```bash
docker exec nifi sh -c 'ss -ltn 2>/dev/null | grep -E "8442|8444" || echo "비어있음"'
```

쓰고 있으면 프로세서의 `Listening Port`와 docker-compose의
`NIFI_LISTENHTTP_PORT` / `NIFI_UNSTRUCTURED_HTTP_PORT`를 같이 맞춘다.

## 같이 전달하면 좋은 것

- `nifi/FLOW_RUNBOOK.md` - 플로우 구조·운영 절차
- `docs/cheatsheet.md` - 그룹 id ↔ Airflow DAG 매핑표
- `db/tarantula-init/` - 적재 대상 스키마 DDL
