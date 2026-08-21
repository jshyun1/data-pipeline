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
