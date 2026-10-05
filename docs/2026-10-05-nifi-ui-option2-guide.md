# NiFi 화면 ②안 안내 - 기존 덧칠 방식과 무엇이 달라졌나 (2026-10-05)

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

- **`ConsoleFramePage.tsx` 의 예전 덧칠 코드는 ②에서는 실행되지 않는다** - 화면 모양을 바꾸려고 거기를 고치면 ②에 반영되지 않는다.
  (공식 화면으로 되돌렸을 때만 쓰인다. ②를 계속 쓰기로 정하면 지워도 된다.)
- 한글 낱말을 바꿀 때 주의: `ConsoleFramePage` 의 연동이 일부 글자를 읽는다 - 작업 패널의 «Process Group / 프로세스 그룹»으로 선택된 그룹을,
  대화상자의 «Apply / 적용»으로 편집 잠금 대상 버튼을 찾는다. 그래서 대화상자 안은 번역하지 않는다.
- 수정분은 작게(들여쓰기 그대로) - NiFi 버전을 올릴 때 다시 맞추는 양이 준다.

## 8. ETL 담당자 확인 부탁

1. `ConsoleFramePage.tsx` 변경(커밋 참고): `isCerebroNifiUi` · `isFrameDocumentLoaded` 추가, `handleFrameLoad` 와 1.5초 타이머에서
   Cerebro 판이면 덧칠 4종을 건너뜀. 연동 코드는 손대지 않음.
2. ②를 계속 쓸지 - 쓰면 예전 덧칠 약 1,500줄을 정리할 수 있다. 공식 화면 대비가 필요하면 남겨 둔다.
3. 아직 영어인 곳: 대화상자(일부러), 사전에 없는 메뉴(Enter Group · Center In View · Copy 등). 필요하면 `cerebro-ko.ts` 에 추가.
