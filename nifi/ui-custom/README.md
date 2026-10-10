# Cerebro NiFi 화면 (②안, 2026-10-05)

NiFi 엔진은 공식 이미지를 그대로 쓰고, **화면(nifi-frontend)만** 공식 소스 + 이 폴더의 수정분으로 빌드해 끼운다.
공식 `nifi-server-nar` 안의 화면 파일(`nifi-ui-<버전>.war`)만 바꾼 NAR 을 이미지에 같이 넣고, 기동 때 어느 쪽을 쓸지 고른다.

예전(①)에는 Cerebro «ETL 관리» 화면(`web/cerebroetl-ui/src/pages/ConsoleFramePage.tsx`)이 iframe **밖에서** NiFi 화면을 고쳤다
(배치 바꾸기 CSS · 요소 표식 · 1초마다 전체 훑어 한글로 바꾸기). NiFi 가 다 그린 뒤에 고치므로 첫 진입 때 원래 배치가 0.4초쯤 보였고,
가만히 있어도 10초에 0.5~0.8초씩 스크립트가 돌았다. 지금(②)은 같은 결과를 NiFi 화면 **소스 안에서** 처음부터 그린다.
`ConsoleFramePage` 는 iframe 안 문서에 `data-cerebro-nifi-ui` 표시가 있으면 덧칠을 건너뛰고 연동(편집 잠금 · 선택 · 경로 동기화)만 한다.
공식 화면으로 되돌리면 표시가 없어 예전 덧칠이 그대로 돈다.

## 무엇을 어디서 고치나

수정분은 NiFi 소스 기준 경로다(`nifi-frontend/src/main/frontend/` 아래).

| 바꾸고 싶은 것 | 파일 | 수정분 |
|---|---|---|
| Cerebro 안(iframe)에서의 배치 · 디자인(etl-manage) - 공구막대 · 메뉴 · 탐색/작업 창 · 경로 알약 · 그룹/프로세서/연결선 색 | `apps/nifi/src/app/cerebro/_cerebro-embedded.scss` | 0004 |
| 한글 낱말(사전) · 번역 규칙 | `apps/nifi/src/app/cerebro/cerebro-ko.ts` | 0004 |
| 공구막대 도구 아이콘(Cerebro 안, 디자인 8종) | `apps/nifi/src/app/pages/flow-designer/ui/canvas/header/new-canvas-item/new-canvas-item.component.html` · `.ts` | 0004 |
| 미니맵 상자 색 | `apps/nifi/src/app/pages/flow-designer/service/birdseye-view.service.ts` | 0004 |
| 프로세서 제목띠 사각형(공식 화면은 투명, 색은 위 scss 가 Cerebro 안에서만) | `apps/nifi/src/app/pages/flow-designer/service/manager/processor-manager.service.ts` | 0004 |
| 시작 때 할 일(표시 · Cerebro 안 판정 · 번역 켜기) | `apps/nifi/src/app/cerebro/cerebro-ui.ts`, `apps/nifi/src/main.ts`, `apps/nifi/src/styles.scss` | 0004 |
| 도구 이름 · 새로고침 단추(Cerebro 안) | `apps/nifi/src/app/pages/flow-designer/ui/canvas/header/header.component.html` · `.ts` | 0002 |
| 상태줄 이름 · 숨김 | `.../canvas/header/flow-status/flow-status.component.html` · `.ts` | 0002 |
| 탐색 · 작업 패널 제목 | `.../canvas/graph-controls/navigation-control/…html`, `operation-control/…html` | 0002 |
| 로고 · 사용자 이름 · 로딩 · 오류 화면 그림 | `apps/nifi/src/app/ui/common/navigation/navigation.component.*`, `app.component.scss`, `pages/{error,route-not-found,login,logout}/…scss` | 0001 |
| 「Cerebro 안인가」 판정 | `apps/nifi/src/app/cerebro/cerebro-embed.ts` | 0001 |
| 색(팔레트) · 글꼴 | `libs/shared/src/assets/themes/material.scss` | 0003 |

원칙: **기능을 줄이는 변경은 Cerebro 안(`html.cerebro-embedded`)에서만, 모양 변경(한글 · 색 · 글꼴)은 어디서나** -
NiFi 에 직접 들어온 관리자는 깔때기 · 포트 등 원래 도구와 메뉴를 다 쓴다. 대화상자 안은 번역하지 않는다(편집 잠금이 «Apply» 글자를 읽는다).

## 고치는 순서

```bash
cd ~/data-pipeline
nifi/ui-custom/patch-tool.sh checkout /tmp/nifi-ui      # 공식 소스(같은 버전) + 수정분 적용본
# /tmp/nifi-ui/nifi-frontend/src/main/frontend/ 아래 파일을 고친다
nifi/ui-custom/patch-tool.sh save /tmp/nifi-ui          # 고친 내용을 patches/*.patch 로 되돌려 쓴다
git diff nifi/ui-custom/patches                          # 바뀐 수정분 확인 후 커밋

# 로컬 반영(화면 빌드에 메모리 2GB 이상 - 8GB PC 는 스택을 멈추고)
docker compose stop <켜져 있던 서비스들>
docker compose build nifi                                # 2~3분
docker compose start <나머지>; docker compose up -d --no-deps nifi
```

- 새 파일을 만들었으면 `save` 가 «어느 수정분에도 속하지 않은 변경»으로 알려준다. 처음 한 번만 알맞은 .patch 에 넣는다
  (`git -C /tmp/nifi-ui diff --full-index -- <새 파일> >> patches/000N-….patch`). 다음부터는 `save` 가 챙긴다.
- 수정분은 작게 유지한다(들여쓰기를 바꾸지 말 것) - 버전을 올릴 때 다시 맞추는 양이 그만큼 준다.
- `ConsoleFramePage.tsx` 의 예전 덧칠 코드는 공식 화면으로 되돌렸을 때를 위해 남아 있다. NiFi 화면 모양은 Cerebro 화면(수정분) 쪽만 고치면 된다.
  단, 이 덧칠은 워크플로우 관리(Airflow iframe) 화면에도 걸리고 있어 그냥 지우면 안 된다(`docs/2026-10-05-nifi-ui-option2-guide.md` 10-2).

## 켜기 · 되돌리기

폐쇄망 반입도 같은 이미지로 된다 - 실행 때 외부 요청이 없다(2026-10-05 확인). 릴리스 빌드에서 켜는 방법과 남은 설정은
`docs/2026-10-05-nifi-ui-option2-guide.md` 9절.

```bash
# .env (로컬)
CEREBRO_NIFI_UI_BUILD=on     # 이미지에 Cerebro 화면을 같이 빌드
CEREBRO_NIFI_UI=custom       # 기동 때 Cerebro 화면 사용 (official 이면 공식 화면)
```

- **되돌리기(즉시)**: `CEREBRO_NIFI_UI=official` 로 바꾸고 `docker compose up -d nifi`. 다시 빌드하지 않는다 - 공식 화면 NAR 사본이 이미지에 늘 들어 있다.
  ETL 관리 화면은 표시가 없는 공식 화면을 보고 예전 덧칠을 다시 한다.
- **완전히 예전 이미지로**: 시험 전에 떠 둔 `data-pipeline-nifi:before-ui` 태그를 `latest` 로 되돌리고 `docker compose up -d nifi`.
- 화면 파일만 바뀐다. 캔버스 흐름 · 설정 · 실행 이력은 볼륨이라 영향이 없다.
- `CEREBRO_NIFI_UI_BUILD` 기본값은 off - off 면 이미지 내용이 예전과 같다(GitHub · npm 접속도 필요 없다).

## 파일

| 파일 | 하는 일 |
|---|---|
| `patches/0001~0004` | 위 표의 수정분 |
| `build.sh` | (docker 빌드 안) 공식 소스(같은 버전 태그) 받기 → 수정분 적용 → Pretendard 넣기 → 빌드 → NAR 만들기 |
| `repack_nar.py` | 공식 NAR 에서 화면 war 의 화면 파일만 바꿈(META-INF · WEB-INF 와 다른 항목은 그대로) |
| `patch-tool.sh` | 수정분 받기(checkout) · 저장(save) |

## NiFi 버전을 올릴 때

1. `patch-tool.sh checkout <폴더> <새 버전>` - 수정분이 안 맞으면 여기서 멈춘다. `git apply --3way` 나 손으로 맞추고 `save`.
2. 화면 구조가 바뀌었으면 `_cerebro-embedded.scss` 의 선택자(`fd-header header.nifi-header`, `nav.nifi-navigation > div …`, `new-canvas-item`,
   `fd-footer`, `operation-control component-context`)와 `ConsoleFramePage` 연동(작업 패널 글자로 선택 읽기 · 편집 잠금)을 다시 확인한다.
3. 화면은 엔진과 같은 버전이어야 한다 - `NIFI_IMAGE_TAG` 와 같은 태그로 빌드된다.

참고(2026-10-05 실측): 2.2.0 → 2.3.0 한 단계에 화면 432개 파일, 2.2.0 → 2.12.0 에 1,591개 파일이 바뀌었다.
