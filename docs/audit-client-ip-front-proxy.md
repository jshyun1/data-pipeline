# 감사 로그에 실제 클라이언트 IP 남기기 (front-proxy)

`permission_audit_log.client_ip` 에 작업 PC 의 IP 가 아니라 `172.18.0.1` 같은 도커 브리지
게이트웨이 주소가 남는 문제와, 그 해결(front-proxy)에 대한 운영 문서.

## 1. 왜 게이트웨이 IP 가 찍혔나

앱 문제가 아니다. 백엔드는 이미 `X-Forwarded-For → X-Real-IP → remoteAddr` 순으로 IP 를
읽고( `PermissionAuditService.currentRequestIp()` ), UI nginx 도 `/api/` 에
`X-Forwarded-For $proxy_add_x_forwarded_for` 를 넣는다.

문제는 그 앞단이다. `ports:` 로 포트를 공개하면 트래픽이 **docker-proxy(userland-proxy)** 를
거치는데, 이 과정에서 출발지 주소가 브리지 게이트웨이로 SNAT 된다. 즉 컨테이너가 보는
`$remote_addr` 자체가 이미 원 클라이언트가 아니므로, 뒤에서 아무리 헤더를 잘 넘겨도
원래 IP 를 복원할 수 없다.

실측(수정 전):

```
cerebroetl-ui 액세스 로그:  172.18.0.1 - - "GET /api/health" ...
```

## 2. 선택한 방법과 버린 방법

| 방법 | 판단 |
|---|---|
| **front-proxy 컨테이너를 host 네트워크로 앞에 둔다** | ✅ 채택. 우리 스택 안에서만 끝난다 |
| docker daemon `userland-proxy: false` | ❌ **호스트 전역** 설정이라 같은 호스트의 다른 솔루션 컨테이너까지 영향. 적용에 docker 재시작(= 전 컨테이너 재시작) 필요 |
| cerebroetl-ui 자체를 host 네트워크로 | ❌ 성립 불가. `nginx.conf` 가 도커 내장 DNS(`resolver 127.0.0.11`)와 서비스명 upstream 에 의존하고, kafka-connect 는 호스트 포트가 아예 없다. NiFi mTLS 의 Host/SNI 전제도 깨진다 |

## 3. 구조

```
사용자 PC ──▶ front-proxy (network_mode: host, :13001 / :18086)
                 │  X-Forwarded-For: <실제 IP>   ← 클라이언트가 보낸 XFF 는 덮어쓴다(위조 차단)
                 ▼
             127.0.0.1:13101  cerebroetl-ui (nginx)
                 │  X-Forwarded-For: <실제 IP>, 172.18.0.1   ← 체인 맨 앞이 원 클라이언트
                 ▼
             pipeline-api  →  permission_audit_log.client_ip = <실제 IP>
```

- front-proxy 는 **host 네트워크**라 docker-proxy 를 안 거친다 → `$remote_addr` 가 진짜 IP.
- 이미지는 `data-pipeline-cerebroetl-ui` 를 **재사용**하고 설정만 갈아끼운다
  → 폐쇄망 반입 이미지 목록(`offline/image-manifest.json`)이 그대로다.
- UI/API 컨테이너는 `127.0.0.1` 에만 붙는다 → 공개 포트를 우회해 XFF 를 위조할 경로가 없다.
- pipeline-api 공개 포트(18086)도 front-proxy 가 함께 받는다. UI 를 안 거치는 직접 호출
  (MSA 포털·스크립트·curl)의 작업도 실제 IP 로 기록된다.

## 4. 켜는 법 (리눅스 서버)

서버 `.env` 에 **한 줄** 추가:

```env
COMPOSE_FILE=docker-compose.yml:docker-compose.realip.yml
```

그리고 평소처럼:

```bash
docker compose up -d
```

포트 번호나 그 밖의 설정은 건드릴 게 없다. 공개 주소(`http://<서버>:13001`,
`http://<서버>:18086`)도 그대로 - front-proxy 가 같은 포트를 대신 받으므로 문서·북마크·
MSA 포털 설정을 바꿀 필요가 없다.

되돌리기: 그 한 줄을 지우고

```bash
docker rm -f front-proxy && docker compose up -d
```

루프백 포트(13101 / 18186)는 `docker-compose.realip.yml` 안에 고정돼 있다. 서버에서 그
포트가 이미 쓰이고 있을 때만 그 파일에서 바꾸면 된다(파일 안 세 군데를 같이 맞출 것).

## 5. 주의

- **Docker Desktop(Windows/Mac/WSL)에서는 켜지 말 것.** host 네트워크 컨테이너의 포트를
  호스트로 노출하지 않아 공개 포트에 접속이 안 된다. 그래서 기본값은 꺼짐이고, 켜지 않으면
  로컬은 기존과 완전히 똑같이 동작한다.
- 스케줄러에서 나는 감사(NiFi 캔버스 폴러, 신원 동기화 등)는 HTTP 요청 컨텍스트가 없어
  `client_ip` 가 비어 있다. 이건 설계상 정상이며 네트워크 구성과 무관하다.
- `deploy/front-proxy/default.conf.template` 은 이미지에 굽지 않고 **마운트**해서 읽는다.
  서버에 그 파일이 없으면 도커가 그 자리에 빈 디렉터리를 만들어 버리는데
  (데몬 기본 동작이라 `create_host_path: false` 로도 못 막는다 - 실측), 그대로 두면 nginx 가
  이미지에 구워진 UI 설정으로 떠서 공개 포트는 안 열리고 호스트의 80/8080 만 점유하는
  조용한 사고가 난다. 그래서 엔트리포인트에서 파일 존재를 먼저 확인하고 없으면 메시지를
  남기고 죽는다(포트를 잡지 않는다). `docker compose ps` 에 Restarting 으로 보이고
  `docker logs front-proxy` 에 원인이 찍힌다.

## 6. 검증 기록 (2026-08-24, 로컬)

브리지 네트워크의 컨테이너(`172.18.0.13`)에서 호출.

| 경로 | front-proxy 가 본 주소 | 감사 로그 `client_ip` |
|---|---|---|
| `:13001` (UI 경유) | `172.18.0.13` | `172.18.0.13` ✅ |
| `:18086` (API 직접) | `172.18.0.13` | `172.18.0.13` ✅ |
| (수정 전) docker-proxy 경유 | `172.18.0.1` | `172.18.0.1` ❌ |

```
occurred_at                | actor_id       | action     | client_ip
2026-08-24 17:39:31.452902 | __iptest_api   | LOGIN_FAIL | 172.18.0.13
2026-08-24 17:39:31.423491 | __iptest_front | LOGIN_FAIL | 172.18.0.13
```

(`__iptest_*` 는 검증용 존재하지 않는 계정으로 만든 LOGIN_FAIL 기록이다.)

구성을 `docker-compose.realip.yml` 오버라이드로 단순화한 뒤 같은 시나리오로 재검증했고
(`__iptest2_*`), 오버라이드를 끈 뒤 기본 동작(13001 / 18086, NiFi·Airflow 프록시)에
회귀가 없는 것도 확인했다.
