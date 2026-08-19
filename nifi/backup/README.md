# NiFi 플로우 백업

NiFi 캔버스에서 만든 플로우(프로세스 그룹/프로세서/커넥션)는 `nifi-conf` 도커 볼륨
안에만 존재하고 코드로는 관리되지 않는다. 이 저장소의 다른 설정들과 달리 `docker
compose down -v`나 볼륨 손상 한 번이면 통째로 사라지고, 실제로 메모리 압박 상황의
컨테이너 재시작에서 캔버스 작업이 유실된 전례가 반복됐다. 그래서 중요한 변경 전후로
여기에 스냅샷을 떠 둔다.

## 파일

- `flow-<타임스탬프>.json.gz` — 플로우 정의 전체(그룹/프로세서/커넥션/컨트롤러 서비스)
- `users-<타임스탬프>.xml` — NiFi 사용자(테넌트)
- `authorizations-<타임스탬프>.xml` — 접근 정책

## 백업 뜨는 법

```bash
TS=$(date +%Y%m%d-%H%M%S)
docker cp nifi:/opt/nifi/nifi-current/conf/flow.json.gz       nifi/backup/flow-$TS.json.gz
docker cp nifi:/opt/nifi/nifi-current/conf/users.xml          nifi/backup/users-$TS.xml
docker cp nifi:/opt/nifi/nifi-current/conf/authorizations.xml nifi/backup/authorizations-$TS.xml
```

## 복구하는 법

NiFi를 **정지한 상태에서** 되돌려 넣어야 한다. 실행 중에 넣으면 종료 시점에 현재
메모리 상태로 덮어써져 무의미해진다.

```bash
docker compose stop nifi
docker cp nifi/backup/flow-<타임스탬프>.json.gz nifi:/opt/nifi/nifi-current/conf/flow.json.gz
docker compose start nifi
```

## 주의: 비밀번호는 이 백업만으로 복구되지 않는다

DBCP 컨트롤러 서비스의 비밀번호 같은 민감 속성은 `enc{...}` 형태로 암호화돼 있고,
복호화 키(`nifi.sensitive.props.key`)는 `nifi.properties` 안에만 있다. 즉 볼륨이
통째로 날아간 상황에서 이 백업으로 복구하면 **플로우 구조는 되살아나지만 비밀번호는
직접 다시 입력해야 한다.** 프로세서가 invalid로 뜨면 이걸 의심할 것.

## 다른 NiFi로 캔버스를 통째로 넘길 때

**받는 쪽 NiFi가 완전히 비어 있을 때만** 쓸 수 있다. 기존 플로우가 하나라도 있으면
그게 전부 사라지므로, 그런 경우엔 `nifi/flow-exports/`의 그룹 단위 JSON을 써야 한다.

전제 조건 두 가지:

- 받는 쪽 NiFi도 **2.2.0** (flow.json 인코딩 버전 2.0)
- 받는 쪽 캔버스가 비어 있음

### 넘길 파일

`flow-<타임스탬프>.json.gz` **하나만** 넘긴다.

`users.xml` / `authorizations.xml`은 **넘기지 말 것.** Single User 인증이라 받는 쪽
컨테이너가 자기 자격증명으로 이 파일들을 이미 만들어 뒀고, 우리 것으로 덮으면 저쪽
`NIFI_SINGLE_USER_USERNAME`과 신원이 어긋나 로그인이 막힌다.

### 적용 (받는 쪽에서)

```bash
docker compose stop nifi
docker cp flow-<타임스탬프>.json.gz nifi:/opt/nifi/nifi-current/conf/flow.json.gz
docker compose start nifi
```

실행 중에 넣으면 종료 시점의 메모리 상태로 덮어써져 무의미하다.

### 민감값(비밀번호) 처리 — 넘기기 전에 결정할 것

스냅샷 안에 `enc{...}` 형태의 암호화된 민감 속성이 **6개** 들어 있다(DBCP 비밀번호 등).
복호화 키 `nifi.sensitive.props.key`는 flow.json.gz가 아니라 **그 인스턴스의
`nifi.properties`에만** 있고, 이 프로젝트는 키를 `.env`로 고정하지 않아서 컨테이너가
처음 뜰 때 인스턴스마다 다른 값이 생성된다. 즉 **받는 쪽 키는 우리와 다르다.**

선택지는 둘이다.

1. **키를 같이 넘긴다** — 받는 쪽 `nifi.sensitive.props.key`를 우리 값으로 맞춘다.
   비밀번호까지 그대로 살아난다. 단 이건 **DB 비밀번호를 넘기는 것과 같으므로**
   안전한 경로로 전달하고, 상대가 그 자격증명을 가져도 되는 사람일 때만 한다.
   절차는 아래 "키까지 같이 넘기는 절차" 참고.
2. **키를 안 넘긴다** — `enc{...}` 값을 **미리 제거한 사본**을 넘기고, 받는 쪽에서
   DBCP 서비스 6개에 비밀번호를 직접 입력한 뒤 Enable한다. 그룹 단위 JSON으로 넘길 때와
   똑같은 작업량이다. 절차는 아래 "비밀번호를 뺀 사본 만들기" 참고.

**키도 안 넘기고 제거도 안 한 파일을 그대로 주면 안 된다.** 받는 쪽 키로는 그 6개 값을
복호화할 수 없고, "일단 올리고 비밀번호만 새로 입력"이 되지 않는다. 둘 중 하나는 해야 한다.

`nifi/backup/nifi-*.properties`에 이 키가 그대로 들어 있다. **커밋하지 말 것.**

### 비밀번호를 뺀 사본 만들기

`strip-sensitive.py`가 `enc{...}` 속성을 통째로 제거한 사본을 만든다. 속성 키 자체를
지우기 때문에 NiFi가 그룹 단위 export를 할 때 만드는 모양과 같다(그 방식으로 MSA 서버
이관이 성공한 전례가 있다).

```bash
python3 nifi/backup/strip-sensitive.py \
  nifi/backup/flow-<타임스탬프>.json.gz \
  nifi/backup/flow-<타임스탬프>-nopw.json.gz
```


받는 쪽에는 `-nopw` 파일만 넘긴다. 적용 절차는 위 "적용 (받는 쪽에서)"와 같고, 키는
전달할 필요가 없다. 올린 뒤 아래 6개에 비밀번호를 입력하고 Enable한다.

| 컨트롤러 서비스 | 위치 | 넣을 것 |
|---|---|---|
| `cp-tarantula-192-168-50-12` | 루트 | 타란툴라DB 비밀번호 |
| `cp-oracle-192-168-204-128` | 루트 | 원천 Oracle 비밀번호 |
| `cp-perf-pg-target` | 루트 | 성능측정용 - PERF 그룹 안 쓰면 생략 |
| `cp-perf-oracle-50-91` | 루트 | 성능측정용 - PERF 그룹 안 쓰면 생략 |
| `cp-target-db` | http-ingest | 타깃DB 비밀번호 |
| `cp-target-db` | logfile | 타깃DB 비밀번호 |

Enable을 안 하면 프로세서가 invalid로 뜨는데 사유가 "Controller Service is disabled"라
다른 문제로 오해하기 쉽다.

### 키까지 같이 넘기는 절차

**키는 `flow.json.gz` 안에 없다**(스냅샷에는 `enc{...}` 암호문만 들어 있고 키 평문은
포함되지 않는다 - 확인 완료). 그래서 **파일 하나 + 키 문자열 하나**를 따로 전달해야 한다.

키 확인:

```bash
docker exec nifi grep '^nifi.sensitive.props.key=' /opt/nifi/nifi-current/conf/nifi.properties
docker exec nifi grep '^nifi.sensitive.props.algorithm=' /opt/nifi/nifi-current/conf/nifi.properties
# 알고리즘은 NIFI_PBKDF2_AES_GCM_256 (2.2.0 기본값) - 받는 쪽도 같아야 한다
```

키 자체는 DB 비밀번호를 여는 열쇠다. **메신저·이메일·git에 올리지 말고** 비밀번호
관리도구 등 안전한 경로로 전달한다.

받는 쪽 적용 순서 — **키를 먼저 넣고 그 다음에 플로우를 올린다.** 순서가 바뀌면 키가
없는 상태로 플로우를 읽어 복호화에 실패한다.

```bash
# ① .env 에 키를 넣는다 (이미지 start.sh가 nifi.properties에 반영해준다)
echo 'NIFI_SENSITIVE_PROPS_KEY=<전달받은 키>' >> .env
# docker-compose.yml의 nifi 서비스 environment에 아래 한 줄이 없으면 추가
#   NIFI_SENSITIVE_PROPS_KEY: ${NIFI_SENSITIVE_PROPS_KEY}

# ② 컨테이너를 한 번 띄워 conf 볼륨과 키를 만든다 (처음 기동이라면)
docker compose up -d nifi

# ③ 정지 → 플로우 투입 → 기동
docker compose stop nifi
docker cp flow-<타임스탬프>.json.gz nifi:/opt/nifi/nifi-current/conf/flow.json.gz
docker compose start nifi

# ④ 키가 실제로 반영됐는지 확인
docker exec nifi grep '^nifi.sensitive.props.key=' /opt/nifi/nifi-current/conf/nifi.properties
```

이 프로젝트의 compose는 `NIFI_SENSITIVE_PROPS_KEY`를 넘기지 않아서 인스턴스마다 키가
자동 생성된다. 받는 쪽에서 위 ①을 빼먹으면 저쪽 자동 생성 키가 그대로 쓰여 복호화에
실패한다.

기동 후 컨트롤러 서비스 6개(`cp-tarantula-192-168-50-12`, `cp-oracle-192-168-204-128`,
`cp-perf-pg-target`, `cp-perf-oracle-50-91`, http-ingest/logfile의 `cp-target-db`)가
비밀번호를 물고 올라왔는지 확인한다. Enable 후 DBCP 검증 API로 실제 접속까지 보면 확실하다.

### 그룹 단위 JSON과 비교

통째로 넘기면 그룹 단위로는 안 따라가는 것들이 같이 온다.

| | 전체 스냅샷 | 그룹 단위 JSON |
|---|---|---|
| DZ→DW 루트 커넥션 5개 | 포함 | **직접 다시 연결해야 함** |
| 루트 컨트롤러 서비스 5개 | 포함 | `includeReferencedServices=true`로 인라인 |
| 파라미터 컨텍스트 | 포함 | 포함 |
| 비밀번호 | 키를 같이 넘기면 유지 | 항상 재입력 |
| 받는 쪽 기존 플로우 | **전부 교체됨** | 보존 |

### 적용 후 확인

받는 쪽 캔버스에 그룹 11개가 올라온다 — `DZ` `DW` `DZ_UPSERT` `비정형` `http-ingest`
`logfile`, 그리고 성능측정용 `PERF-N1~N4` `perf-test-scratch`. PERF 계열이 필요 없으면
받는 쪽에서 지우면 된다.

프로세서는 전부 STOPPED로 올라오는 게 정상이다(아래 참고).

## 참고: 재시작하면 항상 STOPPED로 올라온다

`nifi.flowcontroller.autoResumeState=false`가 `nifi/apply-nifi-properties.sh`에서
기동할 때마다 강제된다. NiFi 기본값(`true`)이면 재시작할 때 직전에 RUNNING이던
프로세서를 자동으로 되살리는데, 그 탓에 아무도 지시하지 않은 대량 적재가 두 번
발생했다(OOM 무한 재시작 루프 한 번, 배치 그룹 전량 재적재 한 번).

그래서 **되돌리지 않는다.** 적재 시작은 오직 Airflow 제어 DAG가 지시한다.
