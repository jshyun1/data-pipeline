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

## 참고: 재시작하면 항상 STOPPED로 올라온다

`nifi.flowcontroller.autoResumeState=false`가 `nifi/apply-nifi-properties.sh`에서
기동할 때마다 강제된다. NiFi 기본값(`true`)이면 재시작할 때 직전에 RUNNING이던
프로세서를 자동으로 되살리는데, 그 탓에 아무도 지시하지 않은 대량 적재가 두 번
발생했다(OOM 무한 재시작 루프 한 번, 배치 그룹 전량 재적재 한 번).

그래서 **되돌리지 않는다.** 적재 시작은 오직 Airflow 제어 DAG가 지시한다.
