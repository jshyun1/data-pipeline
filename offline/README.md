# 폐쇄망 배포 가이드

설계서 `docs/kafka-webservice-design.md` §13을 이 프로젝트의 실제 서비스에 맞게 구현한 스크립트 모음이다.
설계서는 `docs/offline-install-guide.md`/`operation-guide.md`/`troubleshooting.md` 3개 문서로 나누자고
제안하지만, 지금은 POC 단계라 내용이 아직 많지 않아서 하나로 합쳐뒀다 - 실제 운영 단계로 가면서
내용이 늘어나면 그때 분리해도 된다.

## 핵심 원칙 (§13.1, §13.7)

폐쇄망 서버에서는 절대 안 하는 것:

- `docker compose build`
- `docker pull`
- Maven/npm/Gradle 등 외부 패키지 다운로드

폐쇄망 서버에서 하는 것은 이 5단계뿐이다: 이미지 tar 반입 → `load-images.sh` → `.env` 작성 →
`install.sh` → `healthcheck.sh`.

## 1. 외부망(인터넷 되는 곳)에서: 이미지 빌드 + 저장

```bash
# 릴리스 버전을 정하고 (latest 금지 - §13.7)
export APP_VERSION=1.0.0

# 빌드 + docker save를 한 번에 (kafka-connect/connect-init/nifi/pipeline-api/cerebroetl-ui를
# APP_VERSION 태그로 빌드하고, apache/kafka·postgres·filebeat까지 포함해서 tar 하나로 묶음)
./offline/save-images.sh

# 로컬 Oracle/Target DB 컨테이너까지 포함한 완전 자체완결형 데모로 반입하고 싶다면:
./offline/save-images.sh --with-poc
```

결과물은 `release/data-pipeline-${APP_VERSION}/images/data-pipeline-images-${APP_VERSION}.tar` 하나로
생성된다. 이 tar와 함께 아래를 같은 릴리스 폴더에 모아서 압축해 반입한다:

```text
release/data-pipeline-1.0.0/
├── images/data-pipeline-images-1.0.0.tar   (save-images.sh가 생성)
├── docker-compose.yml
├── .env.example
├── offline/                                 (이 디렉터리 전체 - 스크립트+manifest+이 문서)
└── db/, kafka-connect/, nifi/, connect-init/, web/  (Dockerfile 등 소스 - 참고/재빌드용, 폐쇄망에서 실행 안 함)
```

반입 전에 `offline/image-manifest.json`의 `releaseVersion`을 `APP_VERSION`과 맞춰뒀는지 확인한다.

## 2. 폐쇄망 서버에서: 반입 + 설치

```bash
tar -xzf data-pipeline-1.0.0.tar.gz
cd data-pipeline-1.0.0

# 이미지 로드 (기본 경로: ./images)
./offline/load-images.sh

# 환경설정 - 실제 회사 Target DB 주소로 채운다 (CDC 연결정보는 기동 후 Cerebro ETL 웹 화면에서 등록)
cp .env.example .env
vi .env   # TARGET_DB_HOST 등을 회사 DB로, APP_VERSION을 반입한 버전으로

# 서비스 기동 (빌드 없이, load된 이미지만 사용)
./offline/install.sh

# 상태 확인 - 이게 통과해야 운영 승인
./offline/healthcheck.sh
```

로컬 Target DB 컨테이너까지 포함해서 반입했다면(`--with-poc`로 저장한 경우) `install.sh --with-poc`로
그 컨테이너까지 같이 띄울 수 있다.

## 2-1. 알림 발송(이메일 / SMS) 켜기

기본값은 «발송 안 함»이다. 사내에 SMTP 릴레이나 문자 게이트웨이가 있으면 **재빌드 없이
`.env` 만으로** 켤 수 있다. 폐쇄망에서 게이트웨이 규격을 그때 확인해도 되도록 본문 형식과
인증 헤더까지 설정으로 받는다.

```env
# 이메일 - 사내 SMTP 릴레이가 있어야 한다
NOTIFICATION_EMAIL_ENABLED=true
SPRING_MAIL_HOST=smtp.company.local
NOTIFICATION_EMAIL_FROM=alerts@company.local

# SMS - 사내 문자 서버 / 사업자 API
NOTIFICATION_SMS_ENABLED=true
NOTIFICATION_SMS_GATEWAY_URL=http://sms.company.local/api/send
NOTIFICATION_SMS_SENDER=0212345678
```

게이트웨이가 다른 필드명이나 인증을 요구하면 이 두 줄로 맞춘다(코드 수정 불필요):

```env
NOTIFICATION_SMS_GATEWAY_HEADERS=Authorization: Bearer <토큰>; X-API-KEY: <키>
NOTIFICATION_SMS_GATEWAY_BODY_TEMPLATE={"receiver":"{to}","msg":"{text}","sender":"{sender}"}
# form 방식이면 (본문은 자동으로 to=...&text=... URL 인코딩)
NOTIFICATION_SMS_GATEWAY_CONTENT_TYPE=application/x-www-form-urlencoded
```

> 치환자는 `{to} {text} {sender}` 다. **`${to}` 처럼 `$` 를 붙이지 말 것** - docker compose 가
> 자기 변수로 먼저 치환해 빈 값이 들어간다.

값을 채웠으면 `docker compose up -d pipeline-api` 로 반영하고, 화면에서:

1. **설정 > 알림/발송 관리 > 발송 채널** — «발송 경로»가 `설정됨` 인지 확인하고 «사용»을 켠다.
   (`미설정` 이면 위 `.env` 가 컨테이너까지 전달되지 않은 것이다.)
2. **수신자** 탭에서 담당자의 이메일·휴대폰을 등록하고 채널을 구독시킨다. 수신자가 없으면
   보낼 곳이 없어 한 건도 나가지 않는다.
3. 발송 채널의 **[발송]** 버튼으로 테스트한다. 결과는 아래에서 확인한다:

```bash
docker compose exec metadata-db psql -U pipeline_app -d pipeline_meta \
  -c "select channel_type, status, failure_reason, failure_detail
      from notification_delivery order by id desc limit 5;"
```

| failure_reason | 뜻 |
|---|---|
| (없음) + `SENT` | 정상 발송 |
| `NO_RELAY` | `.env` 의 SMTP 호스트 / 게이트웨이 주소가 비어 있다 |
| `CHANNEL_OFF` | 화면의 «사용» 토글이 꺼져 있다 |
| `NO_ADDRESS` | 수신자에 이메일·휴대폰이 없다 |
| `SMTP_ERROR` / `SMS_ERROR` | 릴레이·게이트웨이가 거절했다(`failure_detail` 에 원문) |

발송 규칙: 메일·문자는 **«위험(CRITICAL)»만** 즉시 나간다. 경고·정보는 화면 알림
(조치 대기열·헤더 배지)에만 표시되고 외부로 보내지 않는다 - 경고까지 문자로 보내면
곧 무시하게 되고, 정작 위험이 왔을 때 같이 묻힌다.

## 3. 이후 운영

```bash
./offline/stop.sh    # 정지 (컨테이너/볼륨은 안 지움 - docker compose down 아님)
./offline/start.sh    # 재기동
./offline/healthcheck.sh   # 상태 확인
```

신규 버전 배포 시: 외부망에서 새 `APP_VERSION`으로 1번부터 다시 진행 → 새 tar를 폐쇄망에 반입 →
`load-images.sh` → `.env`의 `APP_VERSION` 갱신 → `start.sh`로 재기동.

## 트러블슈팅

- **`install.sh`가 이미지를 못 찾는다는 오류**: `.env`의 `APP_VERSION`이 `load-images.sh`로 실제 로드한
  이미지 태그와 일치하는지 확인 (`docker images | grep data-pipeline`).
- **`healthcheck.sh`가 특정 서비스에서 실패**: `docker compose logs <서비스명>`으로 원인 확인. 이 프로젝트에서
  실제로 겪었던 문제/원인은 `WORK_LOG.md`의 "알려진 환경 이슈" 절 참고 (예: Oracle 계정 역할 분리, Postgres
  wal_level, Filebeat config 소유권 등 - 폐쇄망에서도 동일한 원인으로 재발할 수 있음).
