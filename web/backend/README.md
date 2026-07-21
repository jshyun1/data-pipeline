# pipeline-backend

Kafka CDC 파이프라인을 웹에서 관리하기 위한 Spring Boot 백엔드. 설계 배경은
`docs/kafka-webservice-design.md` 참고. 이번 증분(Step 1+2)은 프로젝트
스캐폴딩과 `pipeline_connection` CRUD만 포함하며, Docker/docker-compose
연동은 다음 증분에서 진행한다.

## 로컬 실행

Gradle Wrapper가 커밋돼 있어 JDK 21만 있으면 된다 (시스템 Gradle 불필요).

```bash
# 1. 검증용 임시 Postgres (docker-compose.yml과 무관)
docker run -d --name pipeline-meta-test -p 5433:5432 \
  -e POSTGRES_DB=pipeline_meta -e POSTGRES_USER=pipeline_app -e POSTGRES_PASSWORD=devpassword \
  postgres:16-alpine

# 2. 필수 환경변수
export SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5433/pipeline_meta
export SPRING_DATASOURCE_USERNAME=pipeline_app
export SPRING_DATASOURCE_PASSWORD=devpassword
export PIPELINE_CRYPTO_SECRET=$(openssl rand -hex 32)
export PIPELINE_CRYPTO_SALT=$(openssl rand -hex 16)

# 3. 빌드 + 테스트 + 실행
./gradlew clean build
./gradlew bootRun
```

`PIPELINE_CRYPTO_SECRET`/`PIPELINE_CRYPTO_SALT`는 기본값이 없다 — 설정 안 하면
부팅이 실패한다 (하드코딩 키 방지).

## 필수 환경변수

| 변수 | 설명 | 기본값 |
|---|---|---|
| `SPRING_DATASOURCE_URL` | 메타데이터 DB JDBC URL | `jdbc:postgresql://localhost:5432/pipeline_meta` |
| `SPRING_DATASOURCE_USERNAME` | 메타데이터 DB 사용자 | `pipeline_app` |
| `SPRING_DATASOURCE_PASSWORD` | 메타데이터 DB 비밀번호 | `devpassword` |
| `PIPELINE_CRYPTO_SECRET` | `encrypted_password` 컬럼 AES 암호화 키 | 없음 (필수) |
| `PIPELINE_CRYPTO_SALT` | 위 암호화용 salt (hex) | 없음 (필수) |
| `SERVER_PORT` | 서버 포트 | `8081` |

## 테스트

`ConnectionRepositoryIT`는 Testcontainers로 실제 Postgres 컨테이너를 띄워 Flyway
마이그레이션 5개가 전부 적용되는지 검증하므로, `./gradlew test`/`build` 실행 시
Docker가 떠 있어야 한다.
