# P5b — NiFi/Airflow 개인계정(실명) 배포 런북

> 사용자/권한 설계서 §7.7(D4). NiFi/Airflow 콘솔(iframe)을 **공유계정 → 사용자별 실명**으로 전환.
> 백엔드 코드는 **플래그 게이팅(기본 off)** 으로 이미 병합돼 있어, 이 런북의 인프라 단계를 밟기
> 전까지는 기존 공유계정 동작이 그대로다. **P5a 스파이크로 NiFi 2.2.0 대행인증은 실증 완료.**

## 0. 이미 구현된 것(코드, 기본 off)
- `POST /api/authz/proxy-credential?system=NIFI|AIRFLOW` — nginx auth_request 종단점. 세션쿠키로
  사용자 식별 → 권한 확인 → `X-Proxied-Entity: <userId>` 반환 or 401/403.
- 로그인 시 `cetl_session` 세션쿠키(iframe은 Authorization 헤더를 못 실어 쿠키 필요). `authz.proxy.enabled` 일 때만.
- `NifiTenantSyncService` — 역할 변경 시 NiFi 사용자+정책 동기화(조회자=view, 관리자=+modify/operate). `authz.identity-sync.enabled` 일 때만, best-effort.
- 플래그(모두 기본 false): `AUTHZ_PROXY_ENABLED`, `AUTHZ_IDENTITY_SYNC_ENABLED`, `AUTHZ_PROXY_COOKIE_SAMESITE`(기본 Lax).

## 1. ⚠️ P5a에서 확인한 함정(반드시 반영)
1. **truststore는 `keytool -importcert`로 생성** — openssl cert-only PKCS12는 Java가 0 entries로
   인식해 클라이언트 인증서를 리셋한다(전부 실패의 원인이었음).
2. 프록시 클라이언트 인증서에 **`extendedKeyUsage=clientAuth`** 필요.
3. `cerebro-proxy` 신원에 **`/proxy`(Proxy User Requests) write 정책** 필수(없으면 대행 거부).
4. NiFi 정책 API는 **`%2F` 인코딩 슬래시를 거부**(Jetty "Ambiguous URI") → `/policies/read/flow`
   처럼 비인코딩 경로 사용. (코드의 `ensureNifiUserPolicy`는 이미 비인코딩으로 처리)

## 1-1. ⚠️ 2026-08-25 배포 실측으로 추가된 함정

5. **인증서 없이 배포하면 웹 전체가 죽는다(콘솔만이 아니다).** `deploy/p5b-certs/` 는 gitignore
   대상이라 저장소·pull 로 따라오지 않는데, docker-compose 는 그 경로를 bind mount 한다.
   호스트에 파일이 없으면 Docker 가 **같은 이름의 빈 디렉터리**를 만들고, nginx 가
   `PEM_read_bio_X509_AUX() failed ... no start line` 로 기동에 실패해 **재시작 무한 루프**에
   빠진다. 13001 이 통째로 안 열린다. **소스 배포 전에 §2 를 먼저 수행할 것.**

6. **`AUTHZ_PROXY_ENABLED=false` 인 채로 새 nginx.conf 를 올리면 콘솔이 401 이 된다.**
   nginx 는 `/nifi*` `/airflow/` 에 `auth_request` 를 **무조건** 건다. 그런데 그 종단점이 보는
   `cetl_session` 쿠키는 `AuthController` 가 **플래그가 켜져 있을 때만** 심는다. 즉 nginx 는
   요구하는데 쿠키는 발급되지 않는 반쪽 상태가 된다. 새 nginx.conf 를 배포하는 환경은
   **반드시 플래그도 같이 켜야** 한다(끄고 쓸 거면 nginx.conf 의 auth_request 를 빼야 한다).

7. **`/proxy` write 정책이 이미 있으면 POST 로 새로 만들면 안 된다.** 신규 환경은 404 라
   생성이 맞지만, 기존 정책이 있는 환경은 `GET /nifi-api/policies/write/proxy` 로 받아
   **기존 users 배열에 추가해 PUT** 해야 한다. POST 로 덮으면 기존 사용자가 날아간다.

8. **truststore 변경은 NiFi 재기동이 필요하다.** 배포 과정에서 NiFi 가 어차피 재시작한다면
   그 전에 CA 를 넣어 두면 재기동 한 번으로 끝난다.

9. **인증서를 배포 트리 안에 두면 배포가 지운다.** 젠킨스(`docker-control.sh up prod`)가
   소스 트리를 동기화하면서 `deploy/` 를 통째로 교체해 gitignore 대상인 `p5b-certs/` 가
   사라졌고, 웹 전체가 재시작 루프에 빠졌다. **`P5B_CERT_DIR` 로 트리 밖을 가리킬 것.**
   ```env
   P5B_CERT_DIR=/home/dataworld/certs
   ```
   적용 순서가 중요하다 — **인증서를 새 경로에 먼저 두고 나서** 새 compose 를 배포한다.
   반대로 하면 같은 장애가 한 번 더 난다.

10. **호스트 경로가 디렉터리→파일로 바뀌면 `up -d` 로는 안 살아난다.** 컨테이너가 생성
    시점의 마운트 타입을 기억하기 때문에 `not a directory` 로 죽는다(exit 127). compose 파일이
    그대로면 재생성도 하지 않는다. **`docker compose up -d --force-recreate <서비스>`** 로
    올려야 한다.

## 1-2. 🤖 자동화 스크립트 (권장)

아래 §2·§3(인증서 발급 → truststore 등록 → NiFi 재기동 → 사용자·정책 생성 → 검증)은
`deploy/p5b-setup.sh` 한 번으로 끝난다. 위 함정 1·2·3·7·8을 코드로 강제하므로,
폐쇄망 신규 환경에서는 손으로 하지 말고 이걸 쓸 것.

```bash
./deploy/p5b-setup.sh               # 전체 셋업(멱등 - 이미 된 단계는 건너뜀)
./deploy/p5b-setup.sh --verify-only # 아무것도 바꾸지 않고 현재 상태만 점검
./deploy/p5b-setup.sh --help
```

스크립트가 하는 일: nifi-conf 볼륨 백업 → 인증서 발급(clientAuth EKU 강제) →
`keytool -importcert`로 CA 등록 → NiFi 재기동 후 healthy 대기 → `CN=cerebro-proxy` 사용자 →
`/proxy` write 정책(**기존 정책이 있으면 GET→users 추가→PUT**, 함정 #7) → 검증 →
남은 애플리케이션 단계(.env 플래그, `--force-recreate`) 안내 출력.

아래 §2·§3은 스크립트가 무엇을 하는지에 대한 참조용으로 남겨 둔다.

## 2. 인증서 발급 (프록시용 mTLS)
```bash
# CA
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 3650 -subj "/CN=cerebro-proxy-ca"
# 프록시 클라이언트 인증서 (clientAuth EKU 필수)
openssl req -newkey rsa:2048 -nodes -keyout cerebro-proxy.key -out cerebro-proxy.csr -subj "/CN=cerebro-proxy"
openssl x509 -req -in cerebro-proxy.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out cerebro-proxy.crt -days 3650 \
  -extfile <(printf "extendedKeyUsage=clientAuth\nkeyUsage=digitalSignature")
```

## 3. NiFi 일회성 설정 (⚠️ nifi-conf 볼륨 백업 먼저)
```bash
docker run --rm -v nifi-conf:/from -v "$PWD/backup":/to alpine tar czf /to/nifi-conf-$(date +%Y%m%d-%H%M).tgz -C /from .
```
1. **CA를 NiFi truststore에 추가** (keytool — 함정 #1):
   ```bash
   docker exec nifi keytool -importcert -trustcacerts -noprompt -alias cerebro-proxy-ca \
     -file /path/ca.crt -keystore /opt/nifi/nifi-current/conf/truststore.p12 -storetype PKCS12 -storepass "$NIFI_TRUSTSTORE_PASSWORD"
   ```
   (autoreload=false이므로 NiFi 재기동 필요)
2. **`cerebro-proxy` 사용자 + `/proxy` write 정책** (스파이크에서 검증한 API):
   ```
   POST /nifi-api/tenants/users  {revision:{version:0}, component:{identity:"CN=cerebro-proxy"}}
   POST /nifi-api/policies        {revision:{version:0}, component:{resource:"/proxy", action:"write", users:[{id:<위 사용자 id>}]}}
   ```

## 4. nginx 변경 (cerebroetl-ui/nginx.conf)
공유 bearer 주입을 per-user 대행으로 교체:
```nginx
# (1) auth_request 서브요청 정의
location = /internal/authz-nifi {
    internal;
    proxy_pass http://pipeline-api:8081/api/authz/proxy-credential?system=NIFI;
    proxy_pass_request_body off;
    proxy_set_header Content-Length "";
    proxy_set_header Cookie $http_cookie;
    proxy_set_header X-Original-Method $request_method;
}

# (2) /nifi-api/ 등 각 nifi location 에서:
location /nifi-api/ {
    auth_request /internal/authz-nifi;
    auth_request_set $proxied_user $upstream_http_x_proxied_entity;
    proxy_ssl_certificate     /etc/nginx/certs/cerebro-proxy.crt;   # mTLS 클라이언트 인증서
    proxy_ssl_certificate_key /etc/nginx/certs/cerebro-proxy.key;
    proxy_set_header X-ProxiedEntitiesChain "<$proxied_user>";       # ← $nifi_shared_bearer 제거
    # (기존 proxy_set_header Authorization $nifi_shared_bearer; 줄 삭제)
    ...
}
```
- `/nifi/`, `/nf/`, `/nifi-content-viewer/` 도 동일하게 교체.
- docker-compose: cerebroetl-ui 에 `cerebro-proxy.crt/.key` 를 `/etc/nginx/certs/` 로 마운트.

## 5. Airflow (개인 FAB 계정) — 후속 코드
- `AirflowUserSyncService`(미구현): 계정 생성/역할변경 시 Airflow FAB 사용자 생성·역할 매핑
  (ETL관리자→Admin, ETL조회자→Viewer). Airflow 3 FabAuthManager `/auth/fab/v1/users` 사용.
- nginx `/airflow*` 의 `$airflow_shared_cookie` 주입을 per-user 세션쿠키로 교체(auth_request?system=AIRFLOW).
- ⚠️ Airflow 3 FAB API 경로/인증은 배포 환경에서 스파이크로 확인 후 확정.

## 6. 배포 순서 & 검증
1. `nifi-conf` 백업(§3).
2. 인증서 발급(§2), NiFi truststore/사용자/정책 설정(§3), NiFi 재기동.
3. nginx.conf + 인증서 마운트(§4) 배포.
4. `.env`: `AUTHZ_PROXY_ENABLED=true`, `AUTHZ_IDENTITY_SYNC_ENABLED=true` → pipeline-api 재기동.
5. 각 사용자 로그인 후 역할 배정 → `NifiTenantSyncService`가 NiFi 사용자/정책 생성.
6. **검증**: 조회자로 로그인 → ETL>관리(NiFi 캔버스) **보기는 되고 수정/실행은 막힘**, NiFi 감사
   로그에 **실명**이 남는지 확인. 관리자는 수정 가능.

## 7. 롤백
- `.env` 플래그 off + pipeline-api 재기동 → 즉시 공유계정 동작 복귀(nginx는 auth_request가 있어도
  proxy_credential이 200을 주면 동작하나, 확실히는 nginx.conf도 이전 버전으로).
- NiFi 문제 시: `nifi-conf` 백업 복원 + NiFi 재기동.
- **truststore 오조작 시 NiFi 접속 두절** → 반드시 §3 백업에서 복원.

## 부록. 검증된 대행인증 흐름(P5a 스파이크)
```
[브라우저] --cetl_session 쿠키--> [nginx /nifi-api/]
   auth_request -> pipeline-api /api/authz/proxy-credential?system=NIFI
                   (쿠키로 사용자 식별 + NIFI READ 확인) -> 200 + X-Proxied-Entity: <userId>
   nginx: proxy_ssl_certificate(cerebro-proxy) + X-ProxiedEntitiesChain: <userId>
          --mTLS--> [NiFi] : 신원=userId 로 인가(조회자=보기만, 관리자=수정) ✅ 실증완료
```
