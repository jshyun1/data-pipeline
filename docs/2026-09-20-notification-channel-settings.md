# 발송 채널을 화면에서 설정한다 + 카카오 알림톡 추가 — 설계 (2026-09-20)

**왜**: 이 제품이 들어갈 곳(폐쇄망·사내망·외부망)이 정해져 있지 않다. 어디에 들어가든 **화면에서 설정만 하면**
그곳의 메일·문자·카카오톡으로 알림이 나가야 한다. 지금은 발송 설정이 환경변수(.env)에만 있어서, 현장마다
파일을 고치고 재기동해야 한다. 다음 주 시연에서는 **카카오 알림톡이 실제로 발송되는 것**을 보여야 한다.

사용자 결정(2026-09-20): 알림톡(정보성), 대행사 미정·가입 예정, 대상 망 미정, **위험(CRITICAL) 등급만**
발송(메일·문자와 동일), 실패 시 문자 대체발송 **없음**(카카오만).

---

## 1. 지금 구조 (조사 결과)

- 채널은 enum 이 아니라 `notification_channel_config` 의 **행 + 문자열**(`IN_APP`·`EMAIL`·`SMS`).
- 발송기는 `NotificationService` 한 클래스의 if-else. 공통 인터페이스는 없다.
- 판정(`alert_*`) → `enqueueForInstance` 가 `notification_delivery`(아웃박스)에 INSERT → `dispatch()`(5초 주기)가 실제 발송.
- 재시도 2분 간격·최대 5회. 실패 사유는 문자열(`NO_RELAY`·`NO_ADDRESS`·`SMTP_ERROR`·`SMS_ERROR`).
- 외부 채널은 **CRITICAL 만** 나간다(`rank >= 1` 이면 IN_APP 만 남기고 종료).
- SMS 는 이미 «설정으로 현장 게이트웨이를 흡수»하는 구조(URL·헤더·Content-Type·본문 템플릿). 다만 **값이 환경변수에만** 있다.
- `notification_channel_config` 에 `config_json` · `encrypted_secret` · `secret_key_fingerprint` 컬럼이 **선언만 돼 있고 아무도 안 쓴다**.
- 같은 «화면에서 등록» 패턴이 GPT 제공자(`llm_provider_config`)에 이미 있다 - 암호화 저장(`PasswordCryptoService`) + 끝자리 힌트 + 연결 테스트.

## 2. 목표

1. 메일·문자·카카오 설정을 **화면에서 입력·수정**하고, 저장하면 **다음 발송부터 즉시 반영**(재기동 없음).
2. 비밀값(메일 비밀번호·게이트웨이 토큰·카카오 API 키)은 **암호화 저장**, 화면에는 끝 4자리만.
3. 환경변수는 **폴백으로 유지** - 화면 설정이 없으면 기존 .env 값을 그대로 쓴다(폐쇄망에서 파일로만 관리하는 현장 배려).
4. 카카오 알림톡 채널 추가. 대행사가 정해지지 않았으므로 **특정 대행사에 맞춘 코드를 쓰지 않는다**.

## 3. 설계

### 3-1. 설정 저장 (채널 1행 = 설정 1벌)

`notification_channel_config` 의 기존 컬럼을 쓴다.
- `config_json` - 비밀이 아닌 값 전부(주소·포트·발신자·템플릿코드·본문 템플릿·성공 판정 규칙 …)
- `encrypted_secret` - 비밀값 1개(메일 비밀번호 / 게이트웨이 토큰 / 카카오 API 키). `PasswordCryptoService` 로 암호화.
- `secret_hint`(신규 컬럼, V78) - 화면 표시용 끝 4자리. GPT 제공자의 `api_key_hint` 와 같은 방식.

채널별 `config_json` 키:

| 채널 | 키 |
|---|---|
| EMAIL | `host` `port` `username` `from` `auth`(bool) `starttls`(bool) |
| SMS | `url` `headers` `contentType` `bodyTemplate` `sender` |
| KAKAO | `url` `headers` `contentType` `bodyTemplate` `senderKey` `templateCode` `sender` `successPath` `successValue` |

`headers` 는 기존 SMS 와 같은 `"이름: 값; 이름: 값"` 형식. 본문·헤더에서 치환자 `{secret}` 로 비밀값을 끼운다
(대행사마다 키를 헤더로 받기도, 본문으로 받기도 한다).

### 3-2. 설정 읽기 (DB 우선, 환경변수 폴백)

`ChannelSettingsService.effective(channelType)`
1. DB 행의 `config_json`/`encrypted_secret` 을 읽어 값이 있으면 그것을 쓴다.
2. 비어 있는 항목은 환경변수(`notification.*`) 값으로 채운다.
3. 10초 캐시 - `dispatch()` 가 5초마다 도는데 매 건 복호화·조회하면 낭비다. 저장 시 캐시를 비운다.

### 3-3. 발송

- `relayEmail` - DB 설정이 있으면 그 값으로 `JavaMailSenderImpl` 을 만들어 보낸다(캐시). 없으면 기존 Spring 빈.
- `relaySms` - 지금 로직 그대로, 값만 설정 서비스에서 받는다.
- `relayKakao`(신규) - SMS 와 같은 HTTP POST. 다른 점 두 가지:
  - 알림톡은 **승인된 템플릿**에 변수만 채운다 → 본문 템플릿에 `{templateCode}` `{senderKey}` 치환자를 둔다.
  - 대행사가 **HTTP 200 에 실패를 실어 보내는** 경우가 많다 → `successPath`(점 표기 JSON 경로) 와 `successValue` 가
    설정돼 있으면 그 값까지 맞아야 성공으로 본다. 미설정이면 2xx 만으로 판정(지금 SMS 와 동일).
- 실패 사유: `KAKAO_ERROR`(HTTP·예외), 성공 판정 불일치도 같은 코드에 상세를 남긴다.

### 3-4. 수신자·구독

- 카카오는 **휴대폰 번호**로 보낸다 → 기존 `notification_recipient.phone` 을 그대로 쓴다(새 컬럼 없음).
- 구독은 기존 `notification_subscription` 에 `channel_type='KAKAO'` 한 줄. 전화번호가 있으면 수신자 생성 시 기본 구독을 넣는다(SMS 와 동일).
- 심각도 게이트는 메일·문자와 같다(CRITICAL 만).

### 3-5. 화면

설정 → 발송 채널 탭
- 채널 줄마다 **[설정]** 버튼 → 채널별 입력 창(메일/문자/카카오 각각 다른 항목).
- 비밀값은 저장돼 있으면 `****1234` 로 보이고, **비워 두면 기존 값 유지**(GPT 등록과 같은 규칙).
- 저장 후 같은 창에서 **[테스트 발송]** - 기존 채널 테스트 API 를 그대로 쓴다.
- 수신자 탭에 «카카오 수신» 토글 추가(전화번호 없으면 비활성).

## 4. 하지 않는 것 (이번 범위 밖)

- 실패 시 문자 대체발송(사용자 결정: 카카오만).
- 채널별 레이트리밋·서킷브레이커(컬럼만 있고 미구현) - 그대로 둔다.
- 아웃박스 이중 발송 방지(`FOR UPDATE SKIP LOCKED` 없음, 2대 기동 시 중복) - **별건으로 남긴다**.
  알림톡은 건당 과금이라 운영에서 2대로 띄우기 전에 반드시 처리해야 한다.
- 알림톡 템플릿 등록·승인, 대행사 가입 - 사용자가 진행.

## 5. 시연 대비 순서

1. 코드: 화면 설정 + 카카오 발송기 + 목 서버로 전 구간 검증(대행사 없이).
2. 대행사·템플릿 준비되면 **화면에서 값만 입력** → 실제 발송으로 전환. 재배포 없음.
