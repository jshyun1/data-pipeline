# ---------------------------------------------------------------------
# 빌드 스테이지: Gradle Wrapper로 fat jar를 만든다. 여기서만 Maven Central
# 등 외부망 다운로드가 일어난다 (connect-init/Dockerfile과 같은 원칙 -
# 폐쇄망에서는 이 이미지를 docker load만 하고 다시 빌드하지 않는다).
# ---------------------------------------------------------------------
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew --no-daemon bootJar

# ---------------------------------------------------------------------
# 런타임 스테이지: 빌드 산출물(jar)만 옮겨 담는다. 런타임에는 네트워크
# 접근이 전혀 필요 없다.
# ---------------------------------------------------------------------
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN if ! getent group nifi >/dev/null; then \
        if getent group 1000 >/dev/null; then groupadd nifi; else groupadd -g 1000 nifi; fi; \
    fi
RUN if ! id -u nifi >/dev/null 2>&1; then \
        if getent passwd 1000 >/dev/null; then useradd -g nifi -M -s /usr/sbin/nologin nifi; else useradd -u 1000 -g nifi -M -s /usr/sbin/nologin nifi; fi; \
    fi
COPY --from=build /workspace/build/libs/*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
