plugins {
    java
    id("org.springframework.boot") version "3.3.4"
    id("io.spring.dependency-management") version "1.1.6"
}

group = "com.company"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-security")
    // 알림 EMAIL 릴레이(JavaMailSender). spring.mail.host 가 있을 때만 자동설정되며,
    // 미설정(고객사 SMTP 없음) 환경에서는 빈이 없어 NO_RELAY 로 폴백한다.
    implementation("org.springframework.boot:spring-boot-starter-mail")
    implementation("org.springframework.security:spring-security-crypto")
    // cerebroetl-ui 자체 로그인 JWT 발급/검증 (Keycloak 제거, dw.cloud.auth.2026과 동일한
    // jjwt 기반 HMAC 서명 방식).
    implementation("io.jsonwebtoken:jjwt-api:0.12.6")
    runtimeOnly("io.jsonwebtoken:jjwt-impl:0.12.6")
    runtimeOnly("io.jsonwebtoken:jjwt-jackson:0.12.6")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql:42.7.4")
    // 통합 계정 테이블(ST_USER) 조회 전용 - 로그인 검증에만 쓴다(쓰기 없음).
    runtimeOnly("com.mysql:mysql-connector-j:8.4.0")
    // 파이프라인 생성 화면에서 소스/타겟 커넥션의 실제 스키마·테이블 목록을 조회하는
    // 용도(SchemaDiscoveryService). kafka-connect/nifi가 쓰는 것과 동일 버전으로 맞춘다.
    runtimeOnly("com.oracle.database.jdbc:ojdbc11:23.26.2.0.0")
    // 대시보드의 Kafka Broker 헬스체크(AdminClient.describeCluster)용. docker-compose의
    // apache/kafka:3.8.0과 버전을 맞춘다.
    implementation("org.apache.kafka:kafka-clients:3.8.0")

    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
