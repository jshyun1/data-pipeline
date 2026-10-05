# NiFi 화면 ②안 안내 - 기존 덧칠 방식과 무엇이 달라졌나 · 폐쇄망 확인 · 남은 결정 (2026-10-05)

대상: Cerebro ETL 관리 화면 / NiFi 화면을 고치는 개발자(ETL 담당자 포함). 자세한 작업 기록은 WORK_LOG §78 · §79,
절차는 `nifi/ui-custom/README.md`.

## 1. 한 줄 요약

예전에는 Cerebro 화면이 **NiFi 를 다 그린 뒤 밖에서 고쳤고**(①), 이제는 **NiFi 화면 소스 자체를 고쳐서 처음부터 그 모습으로 그린다**(②).
화면 모양은 같고, 첫 진입 깜빡임이 없어졌고, 가만히 있을 때 쓰던 CPU 가 거의 0 이 됐다. 로컬에만 적용돼 있고, 언제든 예전으로 되돌릴 수 있다.

## 2. 기존 방식(①) - «밖에서 덧칠»

- NiFi 는 공식 이미지 그대로(영어 · NiFi 로고 · 청록색). Cerebro «ETL 관리»(`/etl/manage`)가 NiFi 를 iframe 으로 띄운다.
- `web/cerebroetl-ui/src/pages/ConsoleFramePage.tsx`(약 4,700줄 중 NiFi 고치기 약 1,500줄)가 iframe 이 열리면:
  1. CSS 묶음(`HIDE_TOOL_CHROME_CSS`)을 NiFi 문서에 끼워 넣고
  2. 요소를 찾아 표식(`data-cerebro-top-toolbar` · `data-cerebro-tool-allowed` …)을 달아 그 CSS 가 걸리게 하고
     (머리줄 → 왼쪽 아래 공구막대, 상태줄 · 메뉴 · 경로줄 숨김, 도구 아이콘 바꾸기)
  3. 번역 사전(133개)으로 글자를 한글로 바꾼다 - DOM 변화 감시 + **1초마다 문서 전체 훑기**.
- 문제(실측):
  - 첫 진입 때 NiFi 원래 배치가 **약 0.4초 보였다가** 바뀐다 - NiFi 가 그린 뒤에야 표식을 달 수 있어서.
  - 가만히 둬도 10초에 **0.55~0.84초** 스크립트가 돈다(1초 주기 훑기).
  - NiFi 버전이 바뀌어 요소 구조 · 글자가 달라지면 조용히 안 맞는다(예: 패널 배치 기능은 «Navigation» 글자로 패널을 찾는데,
    번역이 먼저 «탐색»으로 바꿔서 이미 동작하지 않고 있었다).

## 3. 지금 방식(②) - «소스에서 그리기»

- NiFi **엔진은 공식 그대로**, **화면(nifi-frontend, Angular)만** 공식 소스 + Cerebro 수정분으로 빌드해 끼운다.
  - 우리 저장소에는 전체 소스가 아니라 **수정분 4개**만 있다: `nifi/ui-custom/patches/0001~0004`.
  - 이미지 빌드 때 `nifi/ui-custom/build.sh` 가 같은 버전(2.2.0)의 공식 소스를 받아 수정분을 적용 · 빌드하고,
    공식 `nifi-server-nar` 안의 화면 파일(`nifi-ui-2.2.0.war`)만 바꾼 NAR 을 만든다.
- 수정분이 하는 일
  | 수정분 | 내용 |
  |---|---|
  | 0001 | NiFi 로고 · 로딩 그림 · 오류 화면 그림 제거, 사용자 이름 · 로그아웃은 Cerebro 안에서만 숨김 |
  | 0002 | 공구막대 · 상태줄 · 탐색/작업 패널 한글, Cerebro 안에서는 도구 3개만 · 상태 숫자 숨김 |
  | 0003 | 색(slate 계열) · 글꼴(Pretendard) |
  | 0004 | **예전 덧칠을 옮긴 것** - Cerebro 안 배치(CSS), 번역기(같은 사전 · 같은 규칙), 도구 아이콘, «Cerebro 판» 표시 |
- `ConsoleFramePage.tsx` 는 iframe 안 문서에 `data-cerebro-nifi-ui` 표시가 있으면 덧칠을 **건너뛰고** 바로 보여준다.
  연동(프로세서 편집 잠금 · 캔버스 선택 → 오른쪽 상세 · 그룹 진입 → 왼쪽 트리)은 그대로 이 파일이 한다.
- 원칙: 기능을 줄이는 변경(도구 숨김 · 메뉴 숨김 등)은 **Cerebro 안(iframe)에서만**, 모양 변경(한글 · 색 · 글꼴)은 어디서나.
  NiFi 에 직접 들어온 관리자는 깔때기 · 포트 · 메뉴를 다 쓴다.

## 4. 차이 한눈에

| | 기존(①) | 지금(②) |
|---|---|---|
| 고치는 곳 | Cerebro `ConsoleFramePage.tsx` (iframe 밖) | NiFi 화면 소스 수정분 `nifi/ui-custom/patches` (iframe 안) |
| 적용 시점 | NiFi 가 그린 뒤 | 처음 그릴 때부터 |
| 첫 진입 깜빡임 | 약 0.4초 원래 배치가 보임 | 없음(181프레임 중 0) |
| 가만히 있을 때 스크립트(10초) | 0.55~0.84초 | 0.019초 |
| 복잡한 캔버스(프로세서 42 · 연결 32) | 가만히 10초에 1~1.3초, 멈춤 10회, 휠 조작 중 2.8초 굳은 적 있음 | 0.1초, 멈춤 0회, 계속 60fps |
| 직접 접속(/nifi/) | 공식 화면 그대로(영어 · 로고) | 한글 · Cerebro 색 · 글꼴, 기능은 전부 |
| 반영 방법 | 화면 컨테이너 재빌드(빠름) | NiFi 이미지 재빌드(화면 빌드 2~3분, 메모리 2GB+) |
| NiFi 버전 올릴 때 | 선택자 · 글자가 조용히 안 맞을 수 있음 | 수정분이 안 맞으면 빌드가 멈춰서 바로 안다 |

## 5. 지금 상태

- **로컬**: ② 적용(`.env` 의 `CEREBRO_NIFI_UI=custom`). http://localhost:13001 → ETL → ETL 관리.
- **MSA 서버**: 적용 안 됨(빌드 옵션 기본값 off - 이미지가 예전과 같다). 커밋은 아직 push 안 함.
- 어느 쪽을 보고 있는지 구별: ETL 관리 화면을 열었을 때 첫 진입에 위쪽 머리줄이 잠깐 보이거나, NiFi 에 직접 들어가서(/nifi/)
  왼쪽 위에 **nifi 로고**와 영어 «Navigation / Operation» 이 보이면 공식(①)이다. 로고가 없고 «탐색 / 작업»이면 ②.

## 6. 켜기 · 되돌리기

```bash
# .env
CEREBRO_NIFI_UI_BUILD=on      # 이미지에 Cerebro 화면을 같이 빌드 (off 면 예전 이미지와 같음)
CEREBRO_NIFI_UI=custom        # 기동 때 Cerebro 화면 사용 / official 이면 공식 화면

docker compose build nifi     # on 일 때 화면 빌드 2~3분, 메모리 2GB 이상(8GB PC 는 스택을 멈추고)
docker compose up -d nifi
```

- **되돌리기**: `CEREBRO_NIFI_UI=official` → `docker compose up -d nifi`(약 1분 반). 다시 빌드할 필요 없음.
  ETL 관리 화면은 표시가 없는 공식 화면을 보고 **예전 덧칠을 그대로 다시 한다**(시험함 - 선택 · 메뉴 · 끌어다 놓기 · 진입 모두 동작).
- **완전히 예전 이미지**: `docker tag data-pipeline-nifi:before-ui data-pipeline-nifi:latest` → `docker compose up -d nifi`.
- 화면 파일만 바뀐다. 캔버스 흐름 · 설정 · 실행 이력은 볼륨이라 그대로다.

## 7. 앞으로 고치는 방법

```bash
nifi/ui-custom/patch-tool.sh checkout /tmp/nifi-ui     # 공식 소스(2.2.0) + 수정분 적용본 받기
#   → /tmp/nifi-ui/nifi-frontend/src/main/frontend/ 아래를 고친다(아래 표)
nifi/ui-custom/patch-tool.sh save /tmp/nifi-ui         # 고친 것을 patches/*.patch 로 저장
git diff nifi/ui-custom/patches                         # 확인 후 커밋 → NiFi 이미지 재빌드
```

| 바꾸고 싶은 것 | 고칠 파일 (`nifi-frontend/src/main/frontend/` 기준) |
|---|---|
| ETL 관리 안 배치 · 숨길 것 · 공구막대 모양 · 캔버스 색 | `apps/nifi/src/app/cerebro/_cerebro-embedded.scss` |
| 한글 낱말 추가 · 수정 | `apps/nifi/src/app/cerebro/cerebro-ko.ts` 의 `TEXT` |
| 공구막대 아이콘 | `apps/nifi/src/app/pages/flow-designer/ui/canvas/header/new-canvas-item/new-canvas-item.component.html` |
| 도구 이름 · ETL 관리 안에서 보일 도구 | `apps/nifi/src/app/pages/flow-designer/ui/canvas/header/header.component.html` |
| 상태줄 | `.../canvas/header/flow-status/flow-status.component.html` |
| 로고 · 사용자 이름 | `apps/nifi/src/app/ui/common/navigation/navigation.component.html` |
| 색 · 글꼴 | `libs/shared/src/assets/themes/material.scss` |

- **`ConsoleFramePage.tsx` 의 예전 덧칠 코드는 ②의 ETL 관리 화면에서는 실행되지 않는다** - NiFi 화면 모양을 바꾸려고 거기를 고치면 ②에 반영되지 않는다.
  (공식 NiFi 화면으로 되돌렸을 때와 워크플로우 관리(Airflow) 화면에서는 지금도 돈다 - 10-2 참고.)
- 한글 낱말을 바꿀 때 주의: `ConsoleFramePage` 의 연동이 일부 글자를 읽는다 - 작업 패널의 «Process Group / 프로세스 그룹»으로 선택된 그룹을,
  대화상자의 «Apply / 적용»으로 편집 잠금 대상 버튼을 찾는다. 그래서 대화상자 안은 번역하지 않는다.
- 수정분은 작게(들여쓰기 그대로) - NiFi 버전을 올릴 때 다시 맞추는 양이 준다.

## 8. ETL 담당자 확인 부탁

1. `ConsoleFramePage.tsx` 변경(커밋 참고): `isCerebroNifiUi` · `isFrameDocumentLoaded` 추가, `handleFrameLoad` 와 1.5초 타이머에서
   Cerebro 판이면 덧칠 4종을 건너뜀. 연동 코드는 손대지 않음.
2. 예전 덧칠 코드 정리 방향 - 아래 10-2. **워크플로우 관리(Airflow iframe) 화면에도 같은 덧칠이 걸리고 있어서** 그냥 지우면 안 된다.
3. 아직 영어인 곳: 대화상자(일부러), 사전에 없는 메뉴(Enter Group · Center In View · Copy 등). 필요하면 `cerebro-ko.ts` 에 추가.

## 9. 폐쇄망에서도 쓸 수 있나 (2026-10-05 확인)

이 프로젝트의 폐쇄망 방식(`offline/README.md`)은 **외부망 PC 에서 이미지 빌드 → tar 로 반입 → 폐쇄망 서버는 `load-images.sh` · `install.sh`
(빌드 없이 `up --no-build`)** 이다. ②도 이 방식을 그대로 따른다.

### 결론

- **실행(폐쇄망 서버) - 그대로 된다.** 인터넷 없이 ①과 똑같이 동작하는 것을 확인했다.
- **패키징 - 아래 A · B · C 를 적용해 ②가 반입되게 했다(같은 날 적용).** 적용 전에는 기본값(릴리스 빌드 off · 설치 official) 때문에
  폐쇄망에 기존(①) 화면이 들어갈 수 있었다. ①로 떠도 ETL 관리는 동작하지만 깜빡임 · 렉은 예전 그대로다.
- **전체 릴리스 예행연습은 아직 안 했다** - 11절 1번.

### 확인한 것

| 항목 | 방법 | 결과 |
|---|---|---|
| 화면이 외부로 요청하나 | 브라우저에서 Cerebro 서버 말고 다른 곳으로 가는 요청을 전부 막고 ETL 관리 · NiFi 직접 화면 · 요약 · 알림판 · 구성 이력 · Cerebro 대시보드 · CDC · 로그를 열었다 | 막힌 요청 **0건**(전체 331건 모두 Cerebro 서버), 화면 오류 0건 |
| 글꼴 | 위 상태에서 브라우저의 글꼴 로드 상태 확인 | Pretendard · Inter · flowfont · FontAwesome 모두 화면 묶음 안 파일로 로드 |
| 화면 묶음 속 외부 주소 | NiFi 화면 파일 전체에서 주소 문자열 검색 | 라이선스 · 설명 링크와, NiFi 의 글꼴 로더(WebFontLoader)가 «외부 글꼴을 쓰도록 설정했을 때만» 쓰는 주소(Google Fonts · Typekit 등)뿐. NiFi 는 내장 글꼴만 쓰도록 설정돼 있어 실제 요청은 없다(위 0건) |
| 화면 고르기(custom/official) | 네트워크 장치가 아예 없는 컨테이너에서 이미지의 고르기 단계만 실행 | custom · official 모두 정상(파일 복사만 한다) |
| 이미지 크기 | 시험 전 이미지와 비교 | +10.6MB(Cerebro 화면 NAR · 공식 사본 · 글꼴) |
| 폐쇄망 설치 스크립트 | `install.sh` 확인 | `up -d --no-build` - 빌드 인자는 쓰지 않으므로 바꿀 것 없음. 반입 이미지 목록(`image-manifest.json`)도 그대로(node 는 빌드 때만 쓰고 이미지에 안 들어감) |

### 외부망 빌드 PC 에 필요한 것 (②로 늘어난 것)

- 접속: 기존(Docker Hub · Maven Central · PyPI)에 더해 **GitHub**(NiFi 화면 소스 2.2.0, 19MB) · **npm 저장소**(패키지 1,668개 · Pretendard 72MB) ·
  **Debian 패키지 저장소**(빌드 단계의 git · python3) · Docker Hub 의 `node:22-bookworm-slim`.
- 메모리: NiFi 화면 빌드에 2GB 이상. 8GB PC 는 스택을 멈추고 빌드한다(멈추지 않으면 WSL 이 멈춘다 - 실측).
- 시간: NiFi 이미지 빌드 약 2~3분 늘어남.

### 바꾼 것 (2026-10-05 적용)

| # | 파일 | 지금 | 바꿀 내용 | 이유 |
|---|---|---|---|---|
| A | `offline/save-images.sh` | 빌드 PC 의 `.env` 값을 따른다(`.env.example` 기본 off) | 릴리스 빌드는 `CEREBRO_NIFI_UI_BUILD=on` 으로 고정하고, 빌드 뒤 이미지에 `/opt/nifi/cerebro-ui/custom` 이 있는지 검사해 없으면 멈춘다 | 빌드 PC 설정에 따라 ②가 빠진 채 반입되는 것을 막는다 |
| B | `.env.example` | `CEREBRO_NIFI_UI=official` | `custom` | 폐쇄망 서버는 `.env.example` 을 복사해 쓴다. Cerebro 화면이 없는 이미지면 경고만 남기고 공식 화면으로 뜨므로 안전하다. `docker-compose.yml` 의 기본값(official)은 그대로 둬서 MSA 등 기존 `.env` 에는 영향이 없다 |
| C | `offline/README.md` | ② 언급 없음 | 빌드 PC 요구 사항, 반입 전 확인 명령(`docker run --rm --entrypoint ls data-pipeline-nifi:<버전> /opt/nifi/cerebro-ui/custom`), 되돌리기(`CEREBRO_NIFI_UI=official`) 추가 | 설치 담당자가 알아야 한다 |

## 10. 정해야 할 것

### 10-1. MSA 서버 반영 방법

| 방법 | 내용 | 장점 | 단점 |
|---|---|---|---|
| **① 빌드 PC 에서 이미지를 만들어 옮기기 (추천)** | 외부망 PC 에서 `CEREBRO_NIFI_UI_BUILD=on` 으로 nifi · cerebroetl-ui 이미지 빌드 → `docker save` → MSA 로 복사 → `docker load` → `.env` 에 `CEREBRO_NIFI_UI=custom` → `docker compose up -d --no-build nifi cerebroetl-ui` | 폐쇄망 반입과 같은 절차라 **폐쇄망 예행연습**이 된다. MSA 에 빌드 부하 · GitHub/npm 접속이 필요 없다. 같은 이미지를 그대로 쓰니 로컬에서 본 그대로 뜬다 | 이미지 파일(nifi 약 1.1GB)을 옮겨야 한다 |
| ② MSA 에서 직접 빌드 | MSA 에서 `git pull` 후 `.env` 에 on · custom → `docker compose build nifi cerebroetl-ui` | 지금 MSA 배포 습관과 같다 | MSA 에서 GitHub · npm 에 닿아야 하고, 빌드 중 메모리 2GB 이상 - MSA 여유 메모리를 먼저 봐야 한다. 빌드 동안 다른 서비스가 느려질 수 있다 |

- 어느 쪽이든 NiFi 재기동(약 1분 반) 동안 ETL 관리 화면이 잠깐 안 뜬다. 되돌리기는 `CEREBRO_NIFI_UI=official` → `up -d nifi`.
- NAR 완료 감시(커밋 6a7b6e6 · 2c0f677)도 같은 nifi 이미지에 들어 있어 함께 반영된다.

### 10-2. 예전 덧칠 코드(`ConsoleFramePage.tsx`) 정리

**확인된 사실**: 예전 덧칠은 ETL 관리(②)에서는 안 돌지만, **워크플로우 관리(Airflow iframe, `/airflow/manage`) 화면에는 지금도 걸린다**
(CSS 주입, 1초 주기 번역 · 표식, 패널 배치 감시, 편집 잠금 감시 - 2026-10-05 화면에서 확인). NiFi 용으로 만든 것이 Airflow 화면에도 붙어 있는 상태다.

| 방법 | 내용 | 장점 | 단점 |
|---|---|---|---|
| **① NiFi 전용 덧칠을 NiFi iframe 에만 걸기 (추천)** | 덧칠 4종(CSS 주입 · 번역/표식 · 패널 배치 · 옛 미니 막대 정리)을 «NiFi 화면일 때만» 실행하게 조건을 건다. 공식 NiFi 화면 대비용으로는 남긴다 | Airflow 화면의 1초 주기 훑기 · 감시가 사라진다. 되돌리기 대비는 유지 | Airflow 화면에서 덧칠로 바뀌던 것이 있었다면(일반 낱말 번역 등) 원래대로 돌아간다 - 먼저 화면 비교 필요 |
| ② 그대로 두기 | 지금 상태 유지 | 바꿀 것 없음 | Airflow 화면에 불필요한 상시 부하가 계속 있다 |
| ③ 모두 지우기 | 덧칠 4종 코드 삭제(약 1,500줄) | 코드가 가장 단순해진다 | 공식 NiFi 화면으로 되돌리면 ETL 관리 모양이 깨진다(영어 · 원래 배치). Airflow 화면 영향도 ①과 같다 |

- 연동 기능(편집 잠금 · 캔버스 선택 · 경로 동기화)은 어느 방법이든 남긴다 - ②도 이것을 쓴다.

## 11. 아직 안 한 것

| # | 할 일 | 상태 · 이유 | 필요한 것 |
|---|---|---|---|
| 1 | **폐쇄망 전체 예행연습** - `save-images.sh`(tar 생성) → 이미지 삭제 → `load-images.sh` → `install.sh` → `healthcheck.sh` → ETL 관리 화면이 Cerebro 화면으로 뜨는지 | 안 함. 화면 실행(외부 요청 0건)과 스크립트 문법 · 이미지 검사 · 설치 기본값은 확인했지만, 처음부터 끝까지 한 번에는 돌리지 않았다 | 이미지 4종 재빌드 + 약 3GB tar 생성. 로컬은 스택을 10~20분 멈춰야 한다. 폐쇄망 반입 전에 한 번 하는 것을 추천 |
| 2 | MSA 서버 반영 | 안 함 - 10-1 결정 대기. MSA 는 지금 기존(①) 화면이고 렉도 그대로다. NAR 완료 감시도 같은 nifi 이미지라 함께 미반영 | 반영 방법 결정, MSA 작업 지시 |
| 3 | 예전 덧칠 코드 정리 | 안 함 - 10-2 결정 대기. 워크플로우 관리(Airflow) 화면에도 걸려 있어 그냥 지우면 안 된다 | 정리 방향 결정 |
| 4 | push | 안 함 - 커밋 7개 대기(6a7b6e6 · 2c0f677 · abf4227 · f6a2681 · 42f46f8 · 50261df · 3801a57) | push 지시 |
| 5 | 남은 영어 | 대화상자(일부러 번역 안 함 - 편집 잠금이 «Apply» 를 읽음), 사전에 없는 메뉴(Enter Group · Center In View · Copy 등) | 필요하면 `cerebro-ko.ts` 에 낱말 추가 |
| 6 | ETL 담당자 공유 | 8절 내용 전달 | - |
