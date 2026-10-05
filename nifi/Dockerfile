ARG NIFI_IMAGE_TAG=2.2.0

# Cerebro NiFi 확장(NAR) 빌드 - job 완료 · 실패 감시(Reporting Task). 소스: extensions/cerebro-nifi (2026-10-05).
# NiFi 소스는 건드리지 않고 공식 확장 방식으로 끼워 넣는다.
FROM maven:3.9-eclipse-temurin-21 AS cerebro-nar
WORKDIR /build
COPY extensions/cerebro-nifi/ ./
# 의존성은 BuildKit 캐시에 두고 다시 쓴다 - 빌드마다 새로 받으면 느리고, 받다 끊기면 빌드가 실패한다(2026-10-05 실제로 한 번 끊김).
# 끊김에 대비해 재시도도 늘린다.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -B -Dmaven.wagon.http.retryHandler.count=5 -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 package

FROM apache/nifi:${NIFI_IMAGE_TAG}

ARG POSTGRES_JDBC_VERSION=42.7.4
ARG OJDBC_VERSION=23.5.0.24.07
ARG MYSQL_JDBC_VERSION=8.4.0
ARG PSYCOPG2_BINARY_VERSION=2.9.10
ARG PYTHON_DOTENV_VERSION=1.0.1

USER root

# ExecuteStreamCommand에서 실행하는 온톨로지 Python 적재 스크립트 의존성
# apache/nifi 기반 이미지의 Python은 Debian의 externally-managed 환경이라
# 컨테이너 이미지 빌드 안에서만 시스템 Python 설치 우회를 명시한다.
RUN /usr/bin/python3 -m pip install --break-system-packages --no-cache-dir \
    "psycopg2-binary==${PSYCOPG2_BINARY_VERSION}" \
    "python-dotenv==${PYTHON_DOTENV_VERSION}"

# PostgreSQL(타란툴라DB 스탠드인), Oracle(향후 역방향 적재용), MySQL JDBC 드라이버
RUN mkdir -p /opt/nifi/nifi-current/drivers && \
    curl -fsSL -o /opt/nifi/nifi-current/drivers/postgresql-${POSTGRES_JDBC_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" && \
    curl -fsSL -o /opt/nifi/nifi-current/drivers/ojdbc11-${OJDBC_VERSION}.jar \
      "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/${OJDBC_VERSION}/ojdbc11-${OJDBC_VERSION}.jar" && \
    curl -fsSL -o /opt/nifi/nifi-current/drivers/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar \
      "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_JDBC_VERSION}/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar" && \
    chown -R nifi:nifi /opt/nifi/nifi-current/drivers

# Cerebro 확장은 lib 에 둔다. 자동 로드 폴더(nar_extensions)를 쓰지 않는 이유: 볼륨으로 잡히면 이미지에 넣은 파일이
# 예전 볼륨 내용에 가려진다. lib 는 이미지 쪽이라 이미지를 다시 만들면 항상 새 NAR 이 들어간다.
COPY --from=cerebro-nar --chown=nifi:nifi /build/cerebro-nifi-nar/target/cerebro-nifi-nar-*.nar /opt/nifi/nifi-current/lib/

# LAN IP(호스트명 아닌 IP 리터럴)로 접속 시 Jetty가 TLS SNI 불일치로 거부하는
# "HTTP ERROR 400 Invalid SNI" 문제 회피 (자체 서명 인증서가 IP를 SAN으로 포함하지 않음)
RUN echo 'java.arg.sniHostCheck=-Djetty.ssl.sniHostCheck=false' >> /opt/nifi/nifi-current/conf/bootstrap.conf

# conf는 볼륨이라 이미지에 구운 nifi.properties 변경이 기존 볼륨에는 반영되지 않는다.
# 기동할 때마다 필요한 속성을 다시 적용하고 원래 start.sh로 넘기는 래퍼를 둔다.
COPY apply-nifi-properties.sh /opt/nifi/scripts/apply-nifi-properties.sh
RUN sed -i 's/\r$//' /opt/nifi/scripts/apply-nifi-properties.sh && \
    chmod +x /opt/nifi/scripts/apply-nifi-properties.sh

USER nifi

ENTRYPOINT ["/opt/nifi/scripts/apply-nifi-properties.sh"]
