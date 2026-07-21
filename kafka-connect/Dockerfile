ARG CONNECT_IMAGE_TAG=7.7.1
FROM confluentinc/cp-kafka-connect:${CONNECT_IMAGE_TAG}

ARG DEBEZIUM_VERSION=3.6.0
ARG OJDBC_VERSION=23.26.2.0.0
ARG POSTGRES_JDBC_VERSION=42.7.4

USER root

# Debezium Oracle(LogMiner) 소스 커넥터 설치
# Confluent Hub에는 debezium-connector-oracle이 등록되어 있지 않아 confluent-hub install로는
# 받을 수 없음 -> Maven Central에 배포되는 plugin tar.gz를 직접 받아서 설치
RUN mkdir -p /usr/share/confluent-hub-components/debezium-debezium-connector-oracle && \
    curl -fsSL -o /tmp/debezium-connector-oracle.tar.gz \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/${DEBEZIUM_VERSION}.Final/debezium-connector-oracle-${DEBEZIUM_VERSION}.Final-plugin.tar.gz" && \
    tar -xzf /tmp/debezium-connector-oracle.tar.gz --strip-components=1 \
      -C /usr/share/confluent-hub-components/debezium-debezium-connector-oracle && \
    rm /tmp/debezium-connector-oracle.tar.gz

# LogMiner 연결에 필요한 Oracle JDBC 드라이버(ojdbc11, JDK 11+ 대응) 추가
# (plugin tar.gz는 lib/ 서브디렉토리 없이 jar를 최상위에 바로 풀어놓는 구조)
# 주의: plugin tar.gz에는 구버전 ojdbc8(+orai18n)이 이미 번들되어 있음. 같은 클래스패스에
# 서로 다른 버전의 oracle.jdbc.driver.* 클래스가 공존하면 로드 순서에 따라 뒤섞여
# O5LOGON 인증 핸드셰이크가 깨지고 ORA-01005(Login denied)가 발생하므로 반드시 제거하고
# 새 ojdbc11만 남긴다.
RUN rm -f /usr/share/confluent-hub-components/debezium-debezium-connector-oracle/ojdbc8-*.jar \
          /usr/share/confluent-hub-components/debezium-debezium-connector-oracle/orai18n-*.jar && \
    curl -fsSL -o /usr/share/confluent-hub-components/debezium-debezium-connector-oracle/ojdbc11-${OJDBC_VERSION}.jar \
      "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/${OJDBC_VERSION}/ojdbc11-${OJDBC_VERSION}.jar"

# 정형 CDC 착지용 JDBC Sink 커넥터 (Kafka 토픽 -> target-db 직접 적재).
# Confluent의 범용 kafka-connect-jdbc 대신 Debezium 자체 JDBC Sink(debezium-connector-jdbc)를
# 쓴다. Debezium CDC envelope(before/after/op)을 네이티브로 이해해서 별도 unwrap SMT 없이
# 바로 테이블에 매핑 가능하고, Confluent Hub 미등록이라 Oracle 커넥터와 동일하게
# Maven Central의 plugin tar.gz를 직접 받는다.
RUN mkdir -p /usr/share/confluent-hub-components/debezium-debezium-connector-jdbc && \
    curl -fsSL -o /tmp/debezium-connector-jdbc.tar.gz \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/${DEBEZIUM_VERSION}.Final/debezium-connector-jdbc-${DEBEZIUM_VERSION}.Final-plugin.tar.gz" && \
    tar -xzf /tmp/debezium-connector-jdbc.tar.gz --strip-components=1 \
      -C /usr/share/confluent-hub-components/debezium-debezium-connector-jdbc && \
    rm /tmp/debezium-connector-jdbc.tar.gz

# target-db(PostgreSQL) 적재에 필요한 JDBC 드라이버 추가
RUN curl -fsSL -o /usr/share/confluent-hub-components/debezium-debezium-connector-jdbc/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# Oracle을 타겟(싱크)으로 쓰는 파이프라인용 Oracle JDBC 드라이버 추가.
# Kafka Connect는 플러그인마다 클래스로더가 격리돼 있어(plugin isolation) 위에서
# debezium-debezium-connector-oracle 디렉터리에 넣은 ojdbc11로는 이 JDBC Sink 플러그인이
# 찾지 못한다 - 싱크 플러그인 디렉터리에도 동일 드라이버를 별도로 넣어야 한다.
RUN curl -fsSL -o /usr/share/confluent-hub-components/debezium-debezium-connector-jdbc/ojdbc11-${OJDBC_VERSION}.jar \
    "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/${OJDBC_VERSION}/ojdbc11-${OJDBC_VERSION}.jar"

# Debezium PostgreSQL(pgoutput) 소스 커넥터 설치 - Postgres를 CDC 소스로 쓰는 파이프라인용.
# Confluent Hub 미등록이라 Oracle/JDBC Sink와 동일하게 Maven Central plugin tar.gz를 직접 받는다.
# 별도 DB측 플러그인 설치 없이 Postgres 내장 pgoutput 로지컬 디코딩 플러그인을 사용한다
# (wal_level=logical, REPLICATION 권한 필요 - 소스 DB에서 별도로 설정해야 함).
RUN mkdir -p /usr/share/confluent-hub-components/debezium-debezium-connector-postgres && \
    curl -fsSL -o /tmp/debezium-connector-postgres.tar.gz \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}.Final/debezium-connector-postgres-${DEBEZIUM_VERSION}.Final-plugin.tar.gz" && \
    tar -xzf /tmp/debezium-connector-postgres.tar.gz --strip-components=1 \
      -C /usr/share/confluent-hub-components/debezium-debezium-connector-postgres && \
    rm /tmp/debezium-connector-postgres.tar.gz
