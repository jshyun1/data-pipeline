# GPT 설정 — 분석과 설계

2026-09-11 ~ 09-13 사용자와 요구사항을 정리하고 결정한 내용이다. 구현 진행 상황과 설계에서 달라진 점은 [12절](#12-현재-반영-상태)에 적는다.

출발점 — 프로젝트에 LLM 을 연결해 두 가지를 하고 싶다.

1. ETL job · CDC 파이프라인 · 워크플로우 노드 속성창에서, 선택한 대상에 대해 질문하면 답해 준다
2. ETL · CDC · 워크플로우 로그 화면에서 오류가 나면, 그 오류를 분석해 원인과 조치를 안내한다

그러려면 어떤 LLM 을 쓸지 등록·선택하는 설정 화면이 먼저 있어야 한다.

---

## 1. 결정 사항

| 항목 | 결정 |
|---|---|
| 메뉴 | 대메뉴 **GPT 설정**, 중메뉴 **등록 / 관리** |
| 제공자 | 외부(Claude · ChatGPT · Gemini) + 내부(폐쇄망 반입 오픈 LLM) |
| 외부 제공자 금지 스위치 | **두지 않는다.** 외부는 언제나 선택 가능 |
| 선택 | 한 번에 **하나만**. **0개 허용** — 선택이 없으면 GPT 기능이 원래 없던 것처럼 안 보인다 |
| 설정 권한 | ADMIN |
| 사용 권한 | **해당 화면을 조회할 수 있는 사용자면 누구나** |
| 대화 저장 | 본문은 저장하지 않는다. 호출 기록만 남긴다 |
| 개인정보 마스킹 | **등록 항목 체크박스**. 기본값은 외부 = 켜짐, 내부 = 꺼짐 |
| 모델 서빙 | 프로젝트 밖(별도 GPU 서버 등). 우리는 주소로 연결만 한다 |
| 구현 순서 | GPT 설정 → 오류 분석 → 속성창 GPT 탭 → (선택) 도구 호출형 에이전트 |

## 2. 전제 — 외부 LLM 은 무료여도 API 키가 필요하다

처음 요구는 «무료가 기본, 유료를 체크하면 키를 등록»이었다. 그대로는 성립하지 않는다.

- Claude · ChatGPT · Gemini 를 **프로그램에서 호출**하려면 세 곳 모두 API 키가 필수다
- 사람이 무료로 쓰는 웹 화면(claude.ai 등)은 API 가 아니라 연결 대상이 될 수 없다
- Gemini 는 무료 등급이 있지만 그때도 키는 발급받아야 하고, 입력이 제품 개선에 쓰일 수 있다는 조건이 붙는다

키 없이 붙는 것은 **내부에 직접 띄운 오픈 LLM** 뿐이다. 그래서 두 축을 나눈다.

| 축 | 값 | 역할 |
|---|---|---|
| 배치 위치 | 외부 / 내부 | 키 필수 여부를 결정한다. **외부는 항상 필수**, 내부는 선택 |
| 과금 구분 | 무료 / 유료 | 표시·관리용. 키 필수 여부와 무관 |

## 3. GPT 설정 화면

### 3-1. 메뉴

```
GPT 설정            /gpt              대메뉴 (CDC 40 과 설정 50 사이 → 45)
 ├ 등록             /gpt/register     신규 · 수정 겸용 (?id= 로 수정)
 └ 관리             /gpt/manage       목록 · 선택 · 연결 테스트 · 삭제
```

`app_menu` 에 3행을 넣는다. 형태는 V63 · V72 를 따른다.

```sql
INSERT INTO app_menu (menu_id, parent_id, menu_nm, menu_url, icon, system_code, required_bits, sort_ord, use_yn) VALUES
  ('GPT',          NULL,  'GPT 설정', NULL,            'gpt', 'ADMIN', 1, 45, 'Y'),
  ('GPT_REGISTER', 'GPT', '등록',     '/gpt/register', NULL,  'ADMIN', 7, 46, 'Y'),
  ('GPT_MANAGE',   'GPT', '관리',     '/gpt/manage',   NULL,  'ADMIN', 1, 47, 'Y');
```

### 3-2. 등록 항목

```
배치 위치     ● 외부   ○ 내부(폐쇄망)                          기본: 외부
제공자       외부 → Claude / ChatGPT / Gemini
             내부 → OpenAI 호환 (Ollama · vLLM · LM Studio 등)
과금 구분     ● 무료   ○ 유료                                  기본: 무료
이름         예) 운영용 Claude
모델명       자유 입력 (제공자별 기본값 제안)
API 주소     외부: 제공자 기본값 자동 / 내부: 필수 (예: http://10.0.0.5:8000/v1)
API 키       외부: 필수 / 내부: 선택
최대 입력 길이  기본값 제안, 내부 모델은 작게
개인정보 마스킹 □ 전송 시 개인정보 마스킹                        기본: 외부 켜짐 / 내부 꺼짐
설명
[연결 테스트]  [저장]
```

- **내부 LLM 이 쉽게 붙는 이유** — Ollama · vLLM · LM Studio 등은 대부분 OpenAI 와 같은 형식의 API 를 제공한다.
  «OpenAI 호환» 하나에 주소만 넣으면 반입한 어떤 모델이든 연결된다.
- **모델명을 목록으로 박지 않는 이유** — 제공자들이 모델을 자주 추가·폐기한다. 목록을 코드에 넣으면 새 모델마다
  재배포해야 한다. 자유 입력 + 연결 테스트로 검증한다.
- **마스킹 기본값** — 배치 위치를 바꾸면 체크박스 기본값도 따라 바뀐다. 관리자가 직접 바꾼 뒤에는 따라가지 않는다.

### 3-3. 검증 규칙 (서버)

| 조건 | 규칙 |
|---|---|
| 배치 = 외부 | 제공자 ∈ {ANTHROPIC, OPENAI, GEMINI}, API 키 필수 |
| 배치 = 내부 | 제공자 = OPENAI_COMPATIBLE, API 주소 필수, 키 선택 |
| 이름 | 중복 불가 |
| 수정 시 키 칸이 비어 있음 | 기존 키 유지 |

### 3-4. 관리 화면

```
선택  이름          배치  제공자      모델          과금  마스킹  키      최근 테스트
 ●   운영용 Claude  외부  Claude     (모델명)      유료  켜짐    …abcd   성공 09-11 10:20
 ○   내부 Llama     내부  OpenAI호환 llama-3-70b   무료  꺼짐    -       성공 09-11 09:58
 ○   Gemini 테스트  외부  Gemini     (모델명)      무료  켜짐    …9f2c   실패(401)
                                                                    [선택 해제]
```

- 라디오로 하나를 고른다
- **0개를 허용하므로 [선택 해제] 가 필요하다.** 라디오는 한 번 고르면 스스로 해제할 수 없다
- 행마다 연결 테스트 · 수정(등록 화면으로 이동) · 삭제
- 선택된 항목은 삭제를 막는다. 먼저 다른 것을 고르거나 선택 해제한다

### 3-5. 연결 테스트

짧은 요청 1회로 결과를 분류해 보여 준다. 저장 전 입력값으로도, 저장된 항목으로도 할 수 있다.

| 결과 | 안내 |
|---|---|
| 성공 | 응답 시간 |
| 401 | API 키가 올바르지 않습니다 |
| 403 | 이 키로는 해당 모델을 쓸 권한이 없습니다 |
| 404 | 모델명 또는 주소를 확인하십시오 |
| 시간 초과 · 연결 불가 | 서버에서 해당 주소로 나갈 수 없습니다(폐쇄망·방화벽·프록시) |

pipeline-api 에는 지금 외부 통신용 프록시 설정이 없다. 사내망이 프록시로만 외부에 나갈 수 있다면 추가가 필요하다.

## 4. 데이터 모델

### 4-1. llm_provider_config

```sql
CREATE TABLE llm_provider_config (
    id                 BIGSERIAL    PRIMARY KEY,
    name               VARCHAR(100) NOT NULL UNIQUE,
    deployment         VARCHAR(20)  NOT NULL,              -- EXTERNAL | INTERNAL
    provider           VARCHAR(30)  NOT NULL,              -- ANTHROPIC | OPENAI | GEMINI | OPENAI_COMPATIBLE
    billing_type       VARCHAR(10)  NOT NULL DEFAULT 'FREE',
    base_url           VARCHAR(500),
    model_name         VARCHAR(200) NOT NULL,
    encrypted_api_key  TEXT,                               -- AES-256/GCM
    api_key_hint       VARCHAR(10),                        -- 화면 표시용 끝 4자리
    mask_sensitive     BOOLEAN      NOT NULL,              -- 전송 시 개인정보 마스킹
    max_input_chars    INTEGER,                            -- 수집 정보 자르기 기준
    is_active          BOOLEAN      NOT NULL DEFAULT FALSE,
    last_test_status   VARCHAR(20),
    last_tested_at     TIMESTAMPTZ,
    description        TEXT,
    created_by         VARCHAR(255),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- «선택은 최대 하나»를 DB 가 강제한다. 선례: etl_job_run 의 uq_etl_job_run_open
CREATE UNIQUE INDEX uq_llm_provider_config_active
    ON llm_provider_config (is_active) WHERE is_active;
```

### 4-2. llm_usage_log — 호출 기록

대화 본문은 저장하지 않는다. SQL · 오류 원문이 DB 에 쌓이면 그 자체가 보안 관리 대상이 되기 때문이다.
대신 누가 · 언제 · 무엇에 · 어느 LLM 으로 물었는지와 비용 파악용 토큰 수만 남긴다. 탭을 닫으면 대화는 사라진다.

```sql
CREATE TABLE llm_usage_log (
    id                  BIGSERIAL    PRIMARY KEY,
    user_id             VARCHAR(255) NOT NULL,
    agent_type          VARCHAR(30)  NOT NULL,   -- JOB_ANALYSIS | ERROR_ANALYSIS
    target_type         VARCHAR(30)  NOT NULL,   -- 7절 표
    target_id           VARCHAR(100),
    provider_config_id  BIGINT       REFERENCES llm_provider_config (id) ON DELETE SET NULL,
    provider            VARCHAR(30)  NOT NULL,   -- 설정이 지워져도 기록이 읽히도록 스냅샷
    model_name          VARCHAR(200) NOT NULL,
    deployment          VARCHAR(20)  NOT NULL,
    masked              BOOLEAN      NOT NULL,
    input_tokens        INTEGER,
    output_tokens       INTEGER,
    duration_ms         INTEGER,
    status              VARCHAR(20)  NOT NULL,   -- SUCCESS | FAILED | TIMEOUT
    error_code          VARCHAR(40),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

### 4-3. 선택 변경

- 한 트랜잭션에서 **기존 해제 → 새 항목 선택** 순서로 처리한다. 유니크 인덱스는 행마다 즉시 검사되므로 순서가 바뀌면 실패한다
- 관리자 둘이 동시에 다른 항목을 고르면 두 번째 요청이 인덱스에 걸린다 → «다른 사용자가 먼저 변경했습니다»
- 선택 해제는 선택된 행의 `is_active` 를 false 로 바꾸는 것뿐이다

### 4-4. API 키

연결정보 화면의 DB 비밀번호 처리 방식을 그대로 따른다.

- 저장: `AesPasswordCryptoService`(AES-256/GCM, 키는 `.env`)
- 조회 응답: 키를 내리지 않는다. 화면에는 `api_key_hint` 만
- 수정: 키 칸이 비어 있으면 기존 키 유지 (`ConnectionService` 와 동일)
- `app_setting` 에 넣지 않는다 — V22 주석에 «메타DB 덤프에 실려 나가므로 시크릿 금지»로 명시돼 있다
- 등록 · 수정 · 삭제 · 선택 변경은 감사 로그(`PermissionAuditService`)에 남기되 키 값은 기록하지 않는다

## 5. 사용 기능

### 5-1. 기능이 켜져 있는지 — status API

```
GET /api/llm/status   →   { "enabled": true,  "providerName": "운영용 Claude", "deployment": "EXTERNAL" }
                      →   { "enabled": false }
```

`enabled: false` 면 GPT 탭과 [GPT 분석] 버튼을 **렌더링하지 않는다.** 회색 비활성으로 보여 주는 게 아니라 원래 없던 화면이 된다.
서버도 선택된 LLM 이 없으면 분석 요청을 거절한다. 응답에 키는 없다.

### 5-2. 속성창 GPT 탭

| 화면 | 속성창 | 대상 종류 |
|---|---|---|
| ETL 관리 | job 속성창 (`ConsoleFramePage` 의 `ProcessGroupDetailPanel`) | `ETL_JOB` |
| CDC 관리 | 파이프라인 속성창 | `CDC_PIPELINE` |
| 워크플로우 캔버스 | 노드 속성창 (job 노드 · 하위 워크플로우 노드) | `WORKFLOW_NODE` |

속성창을 `[속성] [GPT]` 탭으로 나누고, GPT 탭은 **공용 컴포넌트 하나**(`GptChatPanel`)를 세 화면이 같이 쓴다.

```
┌ 속성 │ GPT ──────────────────────────────────────┐
│ ⓘ 이 job 의 정보가 함께 전달됩니다  [보기]           │
│   전송 대상: 운영용 Claude (외부) · 마스킹 켜짐       │
│ ────────────────────────────────────────────────  │
│ 나: 이 job 이 매번 전량을 다시 읽는 이유가 뭐야?       │
│ GPT: 04_SELECT 의 추출 SQL 에 조건절이 없어서…        │
│ ────────────────────────────────────────────────  │
│ [질문 입력                               ] [보내기] │
└───────────────────────────────────────────────────┘
```

**기본 정보는 서버가 조립한다.** 화면이 모아 보내면 화면마다 코드가 중복되고, 보내면 안 되는 값이 섞일 위험을
화면이 떠안는다. 화면은 `대상 종류 + 대상 id + 질문 + 이번 탭의 대화` 만 보내고, 서버의 대상별 수집기가 채운다.

| 대상 | 기본 정보 | 제외 |
|---|---|---|
| ETL job | 이름 · 경로 · 적재 방식, 단계별 SQL · 대상 테이블 · UPSERT 키 · 스케줄, 최근 실행 N건 상태 · 건수, 최근 오류 요약 | 연결정보 계정 · 비밀번호 |
| CDC 파이프라인 | 소스 · 타깃 테이블, 적재 방식, 커넥터 상태, 지연 | 접속 계정 |
| 워크플로우 노드 | 노드 유형, 연결된 job 요약, 선후행 노드, 스케줄, 최근 실행 상태 | — |

ETL job 수집기는 기존 데이터로 만들 수 있다 — `EtlJobStep` 에 `sql_text` · `target_table` · `statement_type` · `update_keys` 가 이미 있다.

**[보기]** 는 실제 전송될 정보를 **마스킹이 적용된 모습 그대로** 펼쳐 보여 준다.

**입력 길이** — `max_input_chars` 를 넘으면 수집기가 우선순위대로 자른다: 오류 원문 > 단계 SQL > 실행 이력.

### 5-3. 오류 분석

| 화면 | 버튼 위치 | 대상 종류 |
|---|---|---|
| ETL 로그 | 통합 표의 실패 행 펼침 메시지 옆 | `ETL_JOB` |
| 워크플로우 로그 | 실패 로그 창 (`TaskFailureLogModal`) | `WORKFLOW_TASK` |
| CDC 로그 | **처리 · 오류 이력 통합 후** 실패 행 펼침 메시지 옆 | `CDC_PIPELINE` |

- **CDC 는 DLQ 탭이 대상이 아니다.** [CDC 실패 처리 정책 전환](2026-09-09-cdc-error-policy-dlq.md)에 따라
  `errors.tolerance = none` 으로 바뀌면 DLQ 는 없어지고, 실패 정보는 **커넥터 태스크가 멈춘 원인(오류 추적 메시지)** 이 된다.
  또 CDC 로그의 처리 이력 · 오류 이력 탭을 ETL 로그처럼 한 표로 합칠 예정이라, CDC 쪽 버튼은 그 작업 뒤에 붙인다
- 워크플로우 실패 로그 창은 이미 잡음을 걸러낸 실패 원인을 보여 주므로, **화면에 보이는 오류 텍스트**를 넘기고
  해당 대상의 요약 정보는 5-2 의 수집기를 재사용한다

답변 형식은 고정한다. 자유 서술이면 매번 모양이 달라 읽기 어렵다.

```
■ 원인 추정     04_SELECT 에서 ORA-12170 (접속 시간 초과)
■ 확인할 것     1. 소스 DB 방화벽  2. 연결정보의 호스트 · 포트
■ 조치 방법     …
■ 확신도        높음 / 보통 / 낮음
[추가 질문하기]
```

### 5-4. 에이전트 구분

job 분석과 오류 분석은 역할 · 입력 · 답변 형식이 달라 나눈다. 다만 1단계는 **분석 유형형**으로 만든다.

| | 분석 유형형 (1단계) | 도구 호출형 (4단계, 선택) |
|---|---|---|
| 동작 | 유형별 **역할 지시문 + 정보 수집기 + 답변 형식**을 정해 한 번에 전달 | LLM 이 필요할 때 «최근 실행 더 보기» 같은 조회를 스스로 호출 |
| 내부 오픈 LLM | 모든 모델에서 동작 | 도구 호출을 제대로 지원하는 모델만 |
| 무엇이 전송되는지 | 정확히 안다 | LLM 이 무엇을 조회할지 모른다 |

폐쇄망 내부 모델까지 지원해야 하므로 1단계는 분석 유형형으로 간다.

```
LlmAgent
 ├ JobAnalysisAgent      대상: ETL_JOB · CDC_PIPELINE · WORKFLOW_NODE   (대상 종류에 따라 수집기만 바뀜)
 └ ErrorAnalysisAgent    대상: 오류 텍스트 + 해당 대상 요약
```

새 분석 유형이 필요해지면 에이전트 클래스 하나를 추가한다.

## 6. 전송 데이터 보호

두 층으로 나눈다.

| 층 | 대상 | 적용 |
|---|---|---|
| 비밀값 원천 제외 | 연결 비밀번호 · API 키 · 인증 토큰 | **항상**. 수집 단계에서 아예 넣지 않는다 |
| 개인정보 패턴 마스킹 | 전화번호 · 이메일 · 주민/외국인등록번호 · 카드번호 | 선택된 LLM 의 `mask_sensitive` 체크박스를 따른다 |

마스킹은 값을 지우지 않고 **종류를 남긴다.** GPT 가 «이 칸에 전화번호가 있었다»를 알 수 있어야 분석이 된다.

```
원문    VALUES ('홍길동', '010-1234-5678', 'hong@company.com', '900101-1234567') was aborted:
        duplicate key value violates unique constraint "customer_pkey"
전송    VALUES ('홍길동', '<전화번호>', '<이메일>', '<주민번호>') was aborted:
        duplicate key value violates unique constraint "customer_pkey"
```

- 분석 품질은 거의 떨어지지 않는다. 원인 파악에 필요한 건 `customer_pkey` 중복이라는 구조다
- 날짜 형식 오류 · 길이 초과처럼 값 자체가 원인인 오류는 개인정보 패턴이 아니라 그대로 보내진다
- **한계** — 형태가 정해진 값만 잡는다. 위 예시의 '홍길동' 처럼 이름 · 주소 · 자유 문장은 못 가린다.
  마스킹은 «완전 차단»이 아니라 «위험 줄이기»다. 그래서 탭에 전송 대상 · 마스킹 여부를 표시하고 [보기] 를 함께 둔다

## 7. 권한

설정은 ADMIN, 사용은 **그 화면을 조회할 수 있으면 누구나**다. 화면에서 탭을 숨기는 것만으로는 부족해서
`/api/llm/chat` 이 요청마다 대상 종류로 권한을 다시 확인한다. 워크플로우는 캔버스와 로그 화면의 권한이 달라 대상 종류를 둘로 나눈다.

| 대상 종류 | 호출 위치 | 필요 권한 |
|---|---|---|
| `ETL_JOB` | ETL 관리 속성창 · ETL 로그 | NIFI 읽기 |
| `CDC_PIPELINE` | CDC 관리 속성창 · CDC 로그 | KAFKA 읽기 |
| `WORKFLOW_NODE` | 워크플로우 캔버스 노드 속성창 | NIFI 읽기 |
| `WORKFLOW_TASK` | 워크플로우 로그 실패 로그 창 | AIRFLOW 읽기 |

| API | 권한 |
|---|---|
| `GET /api/llm/providers`, `POST /api/llm/providers/test` | ADMIN 읽기 |
| 등록 · 수정 · 삭제 · 선택 · 선택 해제 | ADMIN 쓰기 |
| `GET /api/llm/status` | 로그인 사용자 |
| `POST /api/llm/chat` | 위 대상 종류별 권한 |

## 8. 스트리밍과 nginx

LLM 답변은 수 초 ~ 수십 초가 걸리고 내부 모델은 더 느리다. 다 받고 한 번에 보여 주면 멈춘 화면처럼 보이므로
**글자가 흘러나오는 방식(SSE)** 으로 보낸다.

지금 `web/cerebroetl-ui/nginx.conf` 의 `/api/` 는 기본값이라 **응답 버퍼링 켜짐 · 읽기 제한 60초**다.
그대로면 스트리밍이 끝날 때까지 모았다가 한 번에 보내고, 60초를 넘으면 끊긴다. LLM 전용 경로를 둔다.

```nginx
location /api/llm/ {
    proxy_buffering    off;     # 흘러나오는 대로 전달
    proxy_read_timeout 300s;    # 느린 내부 모델 대비
    ...                         # 나머지는 /api/ 와 동일
}
```

## 9. 구성

```
백엔드  com.company.pipeline.llm
 ├ config    LlmProviderConfig · Repository · Service · Controller   등록 · 선택 · 연결 테스트
 ├ client    LlmClient ─ AnthropicClient / OpenAiClient(+호환) / GeminiClient   스트리밍
 ├ agent     LlmAgent ─ JobAnalysisAgent / ErrorAnalysisAgent             역할 지시문 · 답변 형식
 ├ context   EtlJob · CdcPipeline · WorkflowNode · WorkflowTask 수집기, 마스킹
 ├ usage     LlmUsageLog
 └ api       GET /api/llm/status, POST /api/llm/chat (SSE)

화면
 ├ pages/gpt/GptRegisterPage · GptManagePage
 ├ components/GptChatPanel          속성창 GPT 탭 공용
 └ components/GptErrorAnalysis      로그 화면 공용

DB      llm_provider_config(V73) · llm_usage_log(V74) · app_menu 3행
nginx   /api/llm/ 스트리밍 경로
권한    AppLayout 메뉴 · App.tsx 라우트(RequirePermissionRoute ADMIN)
```

## 10. 구현 순서

| 단계 | 내용 | 비고 |
|---|---|---|
| 1 | GPT 설정 — 등록 · 관리 · 연결 테스트 · status API · 메뉴 | 이후 단계의 전제 |
| 2 | 오류 분석 — ETL 로그 · 워크플로우 로그 | 대상이 명확하고 1회성이라 작고, 체감 효과가 크다 |
| 2' | 오류 분석 — CDC 로그 | CDC 로그 탭 통합 이후 |
| 3 | 속성창 GPT 탭 — ETL · CDC · 워크플로우 노드 | 대화형이고 대상이 셋이라 가장 크다 |
| 4 | (선택) 도구 호출형 에이전트 | 지원 모델 한정 |

## 11. 구현 전에 확인할 것

- 에이전트별 **역할 지시문(프롬프트) 초안** — 답변 형식 · 금지 사항(추측을 사실처럼 말하지 않기 등)
- 제공자별 스트리밍 · 토큰 수 응답 형식 — 구현 시점 공식 문서 기준으로 확인
- 현장 사내망이 외부로 나갈 때 프록시가 필요한지
- 속성창 · 로그 화면의 디자인 적용 진행 상황과 탭 모양 맞추기

## 12. 현재 반영 상태

**작업 분담 (2026-09-16)** — ETL 로그 · ETL 속성창(대상 `ETL_JOB`)은 ETL 담당 작업자가 맡고, 워크플로우 · CDC 쪽만 여기서 진행한다.

| 단계 | 상태 | 비고 |
|---|---|---|
| 1 GPT 설정 | 완료 (커밋 `14fd690`) | V73, 등록 · 관리 · 연결 테스트 · status API · 메뉴 |
| 2 오류 분석 — 워크플로우 로그 | 완료 (2026-09-16) | 실패 로그 창(`TaskFailureLogModal`)의 task 마다 [GPT 분석] |
| 2 오류 분석 — ETL 로그 | ETL 담당 | 공용 기반(아래)을 그대로 쓰고 `ETL_JOB` 수집기만 붙이면 된다 |
| 2' 오류 분석 — CDC 로그 | 완료 (2026-09-16) | 펼친 실패 줄에 [GPT 분석]. CDC 로그 탭 통합(`b2c650d`) 이후 |
| 3 속성창 GPT 탭 — CDC · 워크플로우 노드 | 완료 (2026-09-17) | CDC 관리 «파이프라인 상세» 창의 GPT 탭, 워크플로우 캔버스 노드 속성창 [속성] [GPT]. ETL 속성창은 ETL 담당 |
| 4 도구 호출형 | 미착수 | |

**2단계에서 만든 공용 기반** — ETL 쪽도 같이 쓴다.

- 백엔드 `llm/client/LlmChatClient` — 제공자 4종 스트리밍(SSE). 스트리밍을 무시하고 JSON 으로 답하는 호환 서버도 받는다
- `llm/context/LlmContextCollector` — 대상 종류별 수집기 인터페이스. **스프링 빈으로 추가하면 자동으로 붙는다**
  (`WorkflowTaskContextCollector`, `CdcPipelineContextCollector` 가 예). `LlmContext` 가 우선순위대로 자르고,
  `SensitiveDataMasker` 가 마스킹한다(주민번호는 날짜 검사, 카드번호는 Luhn 검사로 긴 숫자 오탐을 막는다)
- `llm/agent/ErrorAnalysisAgent` — 역할 지시문과 답변 형식(원인 추정 / 확인할 것 / 조치 방법 / 확신도)
- `llm/chat` — `POST /api/llm/chat`(SSE: meta → delta … → done | error), `POST /api/llm/chat/preview`([보내는 정보 보기]).
  대상 종류별 권한은 인가 스위치가 켜진 서버에서 요청마다 확인
- `llm/usage` — V74 `llm_usage_log`. 본문 없이 누가 · 무엇에 · 어느 LLM · 글자 수 · 토큰 · 소요 · 결과
- nginx `location /api/llm/` — 버퍼링 끔 · 읽기 제한 330초
- 화면 `components/gpt/GptErrorAnalysis`(분석 창, 추가 질문, 중지, 보내는 정보 보기) · `useGptEnabled`(선택된 LLM 이 없으면 버튼을 그리지 않음)

**3단계에서 더한 것 (2026-09-17)**

- `JobAnalysisAgent`(속성창 질문 지시문) · 수집기 `WorkflowNodeContextCollector`(대상 `WORKFLOW_NODE`) · 공용 `EtlJobContextText`
  (job 단계 · 최근 실행) · `WorkflowGraphText`(선행 · 후행 · 상위 경로). 워크플로우 노드는 <b>저장된 설계 기준</b>이고, 저장 전 노드도
  job · 하위 워크플로우 자체 정보는 보낸다. 캔버스에 저장하지 않은 변경이 있으면 탭에 안내한다
- 화면 `GptChatPanel`(속성창 탭) · `useGptConversation`(대화 상태 공용 훅) · `GptConversationView`(대화 목록 · 입력칸 · 보기 창).
  오류 분석 창도 같은 훅으로 옮겼다. 대상을 바꾸거나 창을 닫으면 대화가 사라지고, [새 대화] 로 비울 수 있다

**대화 길이 관리** — 설정의 «최대 입력 길이» 중 60% 를 대상 정보에, 나머지를 대화에 쓴다(대상 정보가 짧으면 남는 자리도 대화가 쓴다).
대화가 넘치면 서버가 줄이고 SSE `compacted` 이벤트로 알린다. 화면은 «앞 문답 N개를 …» 안내를 띄운다.

| | 오류 분석 (로그) | 속성창 GPT 탭 |
|---|---|---|
| 방식 | 오래된 추가 문답부터 **뺀다** (`DROPPED`) | 오래된 문답을 **요약한다** (`SUMMARIZED`, 요약 호출 1회) |
| 항상 남는 것 | 대상 정보 · 첫 분석 · 이번 질문 | 대상 정보 · 요약 · 이번 질문 |
| 요약 보관 | - | 서버는 저장하지 않는다. 요약을 화면에 넘기고, 화면이 요약된 문답은 흐리게 남긴 채 다음부터 요약만 보낸다 |
| 요약 실패 시 | - | 빼기로 물러난다 |

요약 호출도 `llm_usage_log` 에 `agent_type = CONVERSATION_SUMMARY` 로 따로 남는다. 요약 지시문은 `ConversationSummarizer`(1,500자 이내,
설정 · SQL 원문은 되풀이하지 않음). 줄일 때는 «질문 + 답» 쌍으로만 빼서 역할 순서(user · assistant)가 깨지지 않게 한다 - Anthropic ·
Gemini 는 순서를 강제한다. 답을 못 받은 질문은 대화에서 건너뛴다.

**설계와 달라진 점**

- SSE 를 `SseEmitter`(비동기)가 아니라 요청 스레드에서 직접 쓴다. 비동기 디스패치는 JWT 필터를 다시 타지 않아 완료 시점에
  인가 필터가 익명으로 거절하는 문제가 있고, 관리 도구라 동시 분석 수가 적다
- `llm_usage_log` 에 `input_chars`(실제 전송 글자 수)를 더했다. OpenAI 호환 서버는 스트리밍에서 토큰 수를 안 주는 경우가 많다
  (gemma4 프록시도 안 준다) — 토큰 칸이 비어도 양을 가늠할 수 있게
- 답변 제목에 «■» 를 붙이지 않는다. 화면이 네 제목을 알아보고 제목 모양으로 그린다

**검증 (로컬, gemma4 · KT Cloud 프록시)** — 스트리밍 첫 글자 2.4초, 워크플로우 실패(PutSQL 접속 거부) · CDC 실패(ORA-12541)
모두 형식대로 답함, 추가 질문 · 보내는 정보 보기 · 마스킹(전화번호 · 이메일) 확인. 선택 해제 409 · 없는 대상 404 · 미로그인 401 ·
ETL 대상 400(수집기 없음). 권한이 없는 사용자로는 확인하지 않았다.

**3단계 검증 (로컬, gemma4)** — 속성창 질문: CDC 파이프라인 · 저장된 job 노드(후행 노드 · 실행 조건까지 답함) · 하위 워크플로우 노드
(안의 job 순서와 상위 스케줄) · 저장 전 노드(job 정보만), 질문 없이 400. 대화 줄이기: 속성창 40개 문답(약 13만 자) → 오래된 5쌍 요약
(4초) 후 답, 오류 분석 41개 → 오래된 6쌍을 빼고 첫 분석의 확신도를 기억해 답함. 화면(puppeteer): CDC 상세 창 GPT 탭 질문 · 추가 질문 ·
새 대화 · 보기, 캔버스 [속성] 탭 내용 그대로 · [GPT] 탭 답 · 다른 노드 선택 시 새 대화, 로그 화면 분석 창 회귀, 페이지 오류 0.
화면에서 요약이 일어나는 긴 대화(흐리게 남는 표시)는 직접 눌러 보지 않았다 - 서버 동작만 API 로 확인했다.

**추가 검증 (2026-09-17)**
- 화면 요약 표시 - gemma4 입력 한도를 잠시 6,000자로 낮추고 CDC GPT 탭에서 질문 8개: 6번째에 앞 문답이 흐려지고 경계 줄 · 안내가 뜸,
  8번째에 한 번 더 요약. 끝난 뒤 한도는 100,000자로 되돌림
- 이때 역할 지시문 길이가 한도 계산에서 빠져 실제 전송이 한도를 넘는 것(6,000자에 7,048자)을 찾아 고쳤다 - 이제 지시문 · 머리말까지
  센다(수정 후 최대 5,554자)
- 권한 - 권한 시험 계정 `permtest` 에 임시 역할(COMMON · NIFI 읽기만)을 붙여 확인: CDC 분석 · CDC 보기 · 워크플로우 실패 분석 403,
  워크플로우 노드 보기 200, GPT 상태 200. 끝난 뒤 역할 삭제 · 배정 해제 · 계정 비활성으로 되돌림(비밀번호는 임시값으로 바뀐 채 남음)
