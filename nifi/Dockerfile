ARG NIFI_IMAGE_TAG=2.2.0
FROM apache/nifi:${NIFI_IMAGE_TAG}

ARG POSTGRES_JDBC_VERSION=42.7.4
ARG OJDBC_VERSION=23.5.0.24.07

USER root

# PostgreSQL(타란툴라DB 스탠드인) 및 Oracle(향후 역방향 적재용) JDBC 드라이버
RUN mkdir -p /opt/nifi/nifi-current/drivers && \
    curl -fsSL -o /opt/nifi/nifi-current/drivers/postgresql-${POSTGRES_JDBC_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" && \
    curl -fsSL -o /opt/nifi/nifi-current/drivers/ojdbc11-${OJDBC_VERSION}.jar \
      "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/${OJDBC_VERSION}/ojdbc11-${OJDBC_VERSION}.jar" && \
    chown -R nifi:nifi /opt/nifi/nifi-current/drivers

# LAN IP(호스트명 아닌 IP 리터럴)로 접속 시 Jetty가 TLS SNI 불일치로 거부하는
# "HTTP ERROR 400 Invalid SNI" 문제 회피 (자체 서명 인증서가 IP를 SAN으로 포함하지 않음)
RUN echo 'java.arg.sniHostCheck=-Djetty.ssl.sniHostCheck=false' >> /opt/nifi/nifi-current/conf/bootstrap.conf

USER nifi
