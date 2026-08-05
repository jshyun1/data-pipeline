# NiFi 플로우 내보내기 (그룹 단위)

여기 있는 JSON은 프로세스 그룹 하나를 통째로 담은 **flow definition**이다. 다른 NiFi에
**기존 플로우를 건드리지 않고 그룹만 추가**할 때 쓴다.

`nifi/backup/`의 `flow-*.json.gz`와 혼동하지 말 것 - 그쪽은 **캔버스 전체 스냅샷**이라
복원하면 그 NiFi의 모든 그룹이 통째로 교체된다. 운영 서버처럼 이미 다른 플로우가 있는
곳에는 절대 그걸 쓰면 안 된다.

| 파일 | 내용 |
|---|---|
| `DZ.json` | 원천(Oracle) → DZ 적재. 프로세서 20개 |
| `DW.json` | DZ → DW 적재. 프로세서 15개 |
| `unstructured.json` | 비정형 5종(이미지/동영상/로그/XML/CSV). 하위 그룹 5개 포함 |

## 만든 방법

`includeReferencedServices=true`가 핵심이다. 이게 없으면 그룹 밖(루트)에 있는 DBCP
컨트롤러 서비스가 `externalControllerServices`로만 참조돼서, 가져간 쪽에 같은 서비스가
없으면 프로세서가 전부 깨진다.

```bash
curl -sk -H "Authorization: Bearer $TOKEN" \
  "https://localhost:18443/nifi-api/process-groups/{그룹ID}/download?includeReferencedServices=true"
```

## 가져오는 방법 (NiFi UI)

1. 캔버스 빈 곳에 **Process Group 아이콘을 드래그**
2. 이름 입력 칸 옆의 **파일 업로드(browse)** 버튼으로 이 JSON을 선택
3. `Add` - 그룹이 통째로 생성된다

## 가져오는 방법 (REST API)

UI를 못 쓰는 상황이면 **multipart 업로드 엔드포인트**를 쓴다.

```bash
ROOT=<대상 NiFi의 루트 그룹 id>
curl -sk --http1.1 -X POST \
  -F "id=$ROOT" -F "groupName=DZ" -F "positionX=-360" -F "positionY=300" \
  -F "clientId=$(cat /proc/sys/kernel/random/uuid)" \
  -F "disconnectedNodeAcknowledged=false" \
  -F "file=@DZ.json;type=application/json" \
  "https://<host>/nifi-api/process-groups/$ROOT/process-groups/upload"
```

**`POST /process-groups/{id}/process-groups` 본문에 `versionedFlowSnapshot`을 넣는 방식은
쓰면 안 된다.** NiFi 2.2.0은 그 필드를 조용히 무시하고 **이름만 같은 빈 그룹**을 만든다
(HTTP 201이 떨어지고 응답의 `component`도 정상이라 성공한 것처럼 보인다 - 실측 확인).
그룹을 열어봐야 비어 있는 걸 알 수 있다.

내려받을 때도 `--http1.1`이 필요하다. 응답이 100KB를 넘으면 HTTP/2 framing 오류로
끊긴다(`curl: (16)`).

## 가져온 뒤 반드시 해야 하는 것 3가지

### 1. DB 비밀번호 재입력 + 컨트롤러 서비스 활성화

**비밀번호는 스냅샷에 아예 담기지 않는다**(속성 자체가 빠진다 - 실측 확인). 접속 URL과
사용자명은 들어 있으므로 비밀번호만 넣으면 된다.

그룹 우클릭 → Configure → Controller Services 탭에서:

| 서비스 | 넣을 것 |
|---|---|
| `cp-tarantula-192-168-50-12` | 타란툴라DB 비밀번호 |
| `cp-oracle-192-168-204-128` | 원천 Oracle 비밀번호 (DZ만 해당) |

넣은 뒤 각 서비스를 **Enable**한다. 안 하면 프로세서가 전부 invalid로 뜨는데, 사유가
"Controller Service is disabled"라서 다른 문제로 오해하기 쉽다.

### 2. DZ → DW 루트 커넥션 5개 다시 잇기

그룹과 그룹 "사이"의 연결이라 어느 쪽 스냅샷에도 들어가지 않는다. 캔버스에서 DZ를
DW로 드래그해서 아래 5쌍을 이름이 같은 것끼리 이어야 한다.

```
DZ to-dw-COM001M  →  DW from-dz-COM001M
DZ to-dw-COM002L  →  DW from-dz-COM002L
DZ to-dw-COM003M  →  DW from-dz-COM003M
DZ to-dw-COM004M  →  DW from-dz-COM004M
DZ to-dw-POP003L  →  DW from-dz-POP003L
```

**이름이 다른 짝을 이으면 NiFi는 막지 않는다** - 에러 없이 엉뚱한 테이블에 적재된다.
이 연결은 Airflow 제어 DAG가 "DZ 다음에 DW"를 판단하는 근거이기도 하다
(`nifi_pipelines_dynamic.py`의 `build_group_chain`).

### 3. 적재 대상 테이블 만들기

비정형 그룹은 `unstructured` 스키마가 있어야 동작한다.

```bash
psql -h <타란툴라DB> -U <계정> -d postgres -f db/tarantula-init/01_unstructured_schema.sql
```

## 포트 충돌 확인

비정형 그룹의 `ListenHTTP-unstructured`가 컨테이너 안에서 **8444**를 연다. 가져가는
NiFi에서 그 포트를 이미 쓰고 있으면 프로세서를 시작할 때 bind 실패로 죽는다(캔버스에서만
확인 가능한 형태로 실패해서 원인 찾기가 번거롭다).

가져가기 전에 두 가지를 확인한다.

```bash
# ① 그 NiFi의 기존 플로우에 포트를 여는 프로세서가 있는지
docker exec nifi python3 -c "
import gzip, json
d = json.load(gzip.open('/opt/nifi/nifi-current/conf/flow.json.gz'))
def walk(g, path=''):
    p = path + '/' + g['name']
    for pr in g.get('processors', []):
        t = pr.get('type', '').split('.')[-1]
        props = pr.get('properties', {}) or {}
        ports = [(k, v) for k, v in props.items() if v and 'ort' in k]
        if ports or t.startswith(('Listen', 'HandleHttp')):
            print(p, '|', pr.get('name'), '|', t, '|', ports)
    for c in g.get('processGroups', []):
        walk(c, p)
walk(d['rootGroup'])
"

# ② 컨테이너 안에서 이미 그 포트를 듣고 있는 프로세스가 있는지
docker exec nifi sh -c 'ss -ltn 2>/dev/null | grep 8444 || echo "8444 비어있음"'
```

①이 비어 있고 ②가 "비어있음"이면 충돌이 없다. 이미 쓰고 있다면 가져온 뒤
`ListenHTTP-unstructured`의 `Listening Port`를 빈 포트로 바꾸고, docker-compose의
`NIFI_UNSTRUCTURED_HTTP_PORT`도 같은 값으로 맞춘다.

**참고**: 2026-07-29 기준 MSA 서버(192.168.50.30) NiFi에는 포트를 여는 프로세서가
하나도 없어서 8444는 비어 있다.

## MSA 서버 이관 기록 (2026-07-29 완료)

192.168.50.30 NiFi(2.2.0)에 세 그룹을 **추가**했다. 기존 DM / file / 수입테이블4개컬럼매핑은
건드리지 않았고, 작업 후에도 카운트가 그대로다.

| 그룹 | MSA 그룹 id | 상태 |
|---|---|---|
| DZ | `ad0c12a1-019f-1000-5562-e5616d1a834c` | 프로세서 20, 커넥션 20, 출력포트 5 |
| DW | `ad0c9365-019f-1000-dea0-d9078538c9af` | 프로세서 15, 커넥션 15, 입력포트 5 |
| 비정형 | `ad0c93f9-019f-1000-2a1b-14fb8491cce5` | 프로세서 18, 커넥션 25, 하위그룹 5 |

같이 끝낸 것:

- `cp-tarantula-192-168-50-12` 비밀번호 입력 후 3개 그룹 모두 Enable. NiFi 검증 API로
  실제 접속까지 확인했다(`Establish Connection: Successfully established`)
- 리더/라이터 컨트롤러 서비스 7종 Enable (avro/xml/grok/json/csv)
- DZ→DW 루트 커넥션 5개 연결 (이름 같은 짝끼리)
- `unstructured` 스키마는 **이미 있었다** - MSA도 같은 192.168.50.12를 보므로 로컬 작업
  때 만든 5개 테이블을 그대로 쓴다. 추가 작업 없음

**남은 것**: DZ의 `cp-oracle-192-168-204-128`은 원천 Oracle 비밀번호를 몰라서 DISABLED로
뒀다. 그래서 `extract-tb-*` 5개가 invalid다(사유: "Controller Service ... is disabled").
비밀번호를 넣고 Enable하면 5개 모두 valid가 된다. 그 전까지 DZ는 실행할 수 없다.

모든 프로세서는 STOPPED 상태다. 실행은 Airflow가 담당한다.

## 스냅샷 갱신

캔버스를 고친 뒤에는 이 파일들도 다시 뽑아야 한다. 안 그러면 다음 이관 때 옛 구조가
올라간다.

```bash
# TOKEN 발급 후 각 그룹 id로 위 download 요청을 다시 실행
```
