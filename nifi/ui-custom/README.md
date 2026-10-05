# Cerebro NiFi 화면 (②안 시험, 2026-10-05)

NiFi 엔진은 공식 이미지를 그대로 쓰고, **화면(nifi-frontend)만** 공식 소스 + 이 폴더의 수정분으로 빌드해 끼운다.
공식 `nifi-server-nar` 안의 화면 파일(`nifi-ui-<버전>.war`)만 바꾼 NAR 을 이미지에 같이 넣고, 기동 때 어느 쪽을 쓸지 고른다.

| 파일 | 하는 일 |
|---|---|
| `patches/0001-cerebro-logo-user-splash.patch` | NiFi 로고 · 로딩 그림 · 오류 화면 배경 그림 제거, 사용자 이름 · 로그아웃은 Cerebro 안(iframe)에서만 숨김 |
| `patches/0002-cerebro-canvas-toolbar-ko.patch` | 캔버스 공구막대 · 상태줄 · 탐색/작업 패널 한글화, Cerebro 안에서는 도구 3개(프로세서 · 프로세스 그룹 · 라벨)만, 상태 숫자 줄 숨김 |
| `patches/0003-cerebro-theme-font.patch` | 밝은 테마 색을 slate 계열로, 글꼴을 Pretendard 로(상태 색 빨강 · 초록 · 노랑은 그대로) |
| `build.sh` | 공식 소스(같은 버전 태그) 받기 → 수정분 적용 → 글꼴 넣기 → 빌드 → NAR 만들기 |
| `repack_nar.py` | 공식 NAR 에서 화면 war 의 화면 파일만 바꿈(META-INF · WEB-INF 와 다른 항목은 그대로) |

«기능을 줄이는 변경은 Cerebro 안에서만, 모양 변경은 어디서나»가 원칙이다 - NiFi 에 직접 들어온 관리자는 깔때기 · 포트 등 원래 도구를 다 쓴다
(`apps/nifi/src/app/cerebro/cerebro-embed.ts`). 한글 낱말은 ETL 관리 화면 덧칠(`web/cerebroetl-ui/src/pages/ConsoleFramePage.tsx`)의
번역 사전과 같게 맞췄다 - 덧칠이 `tooltip` · `title` 속성의 영어/한글 이름으로 도구와 상태 항목을 찾는다.

## 켜기 · 되돌리기

```bash
# .env (로컬)
CEREBRO_NIFI_UI_BUILD=on     # 이미지에 Cerebro 화면을 같이 빌드
CEREBRO_NIFI_UI=custom       # 기동 때 Cerebro 화면 사용 (official 이면 공식 화면)

docker compose build nifi    # 화면 빌드에 메모리 2GB 이상 - 로컬(8GB)은 스택을 멈추고 빌드할 것
docker compose up -d nifi
```

- **되돌리기(즉시)**: `CEREBRO_NIFI_UI=official` 로 바꾸고 `docker compose up -d nifi`. 다시 빌드하지 않는다 - 공식 화면 NAR 사본이 이미지에 늘 들어 있다.
- **완전히 예전 이미지로**: 시험 전에 떠 둔 `data-pipeline-nifi:before-ui` 태그를 `latest` 로 되돌리고 `docker compose up -d nifi`.
- 화면 파일만 바뀐다. 캔버스 흐름 · 설정 · 실행 이력은 볼륨이라 영향이 없다.
- `CEREBRO_NIFI_UI_BUILD` 기본값은 off - off 면 이미지 내용이 예전과 같다(GitHub · npm 접속도 필요 없다).

## 수정분 고치기

```bash
git clone --depth 1 --filter=blob:none --sparse --branch rel/nifi-2.2.0 https://github.com/apache/nifi.git nifi-ui-src
cd nifi-ui-src && git sparse-checkout set nifi-frontend
for p in <이 폴더>/patches/*.patch; do git apply "$p"; done
# ... 소스를 고친 뒤, 고친 파일이 속한 수정분을 다시 만든다(새 파일은 먼저 git add -N)
git diff -- <그 수정분의 파일들> > <이 폴더>/patches/000N-....patch
```

수정분은 작게 유지한다(들여쓰기를 바꾸지 말 것) - 버전을 올릴 때 다시 맞추는 양이 그만큼 준다.

## NiFi 버전을 올릴 때

1. 새 태그로 위처럼 받아 `git apply --check` 로 세 수정분이 맞는지 본다. 안 맞으면 `git apply --3way` 나 손으로 맞추고 수정분을 다시 만든다.
2. 화면 구조가 바뀌었으면 ETL 관리 화면 덧칠과의 맞물림(도구 이름 · 상태 항목 title)을 다시 확인한다.
3. 화면은 엔진과 같은 버전이어야 한다 - `NIFI_IMAGE_TAG` 와 같은 태그로 빌드된다(build.sh 가 그 태그를 받는다).

참고(2026-10-05 실측): 2.2.0 → 2.3.0 한 단계에 화면 432개 파일, 2.2.0 → 2.12.0 에 1,591개 파일이 바뀌었다.
