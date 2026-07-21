# 개발 협업 가이드 (2인 개발 기준)

## 브랜치 전략

- `main`: 항상 배포 가능한 상태 유지, 보호 브랜치(Protected). 직접 push 금지.
- `develop`: 통합 브랜치. 기능 브랜치는 여기로 병합.
- `feature/<이름>`: 기능 단위 작업 브랜치 (예: `feature/oracle-cdc-connector`, `feature/nifi-unstructured-flow`).
- `fix/<이름>`: 버그 수정 브랜치.

작업 흐름: `feature/*` → MR(Merge Request) → 리뷰 1인 이상 승인 → `develop` 병합
→ 안정화 후 `develop` → `main` 병합(릴리스 태그 부여).

## GitLab 설정 권장값 (gitlab.com)

1. Settings → Repository → Protected branches: `main` (Maintainer만 merge/push 허용)
2. Settings → Merge requests: "Pipelines must succeed" 체크, Merge 시 Squash 옵션 권장
3. Settings → CI/CD → Runners: gitlab.com SaaS 공용 Runner 사용(기본 활성화)
4. 두 사람 모두 Maintainer 이상 권한으로 등록, MR 상호 리뷰 필수

## 커밋 컨벤션

`type: 짧은 요약` 형식 권장 (`feat`, `fix`, `docs`, `chore`, `refactor`).
예: `feat: Debezium Oracle 커넥터 등록 스크립트 추가`

## 로컬 환경 변수

`.env` 파일은 git에 포함되지 않습니다(`.gitignore`). 새 팀원은
`cp .env.example .env` 후 필요한 값(비밀번호 등)을 채워야 합니다.
비밀번호 등 민감 값은 Slack/사내 메신저 등 별도 채널로 공유하고,
저장소나 MR 코멘트에 남기지 마세요.
