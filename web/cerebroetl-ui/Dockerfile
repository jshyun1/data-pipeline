# ---------------------------------------------------------------------
# 빌드 스테이지: npm install + vite build. 여기서만 npm 레지스트리 접근이
# 일어난다 (web/backend/Dockerfile과 같은 원칙 - 폐쇄망에서는 이 이미지를
# docker load만 하고 다시 빌드하지 않는다).
# ---------------------------------------------------------------------
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# ---------------------------------------------------------------------
# 런타임 스테이지: 빌드된 정적 파일만 nginx로 서빙. 런타임 네트워크 접근은
# /api 리버스 프록시(같은 docker 네트워크 안의 pipeline-api)와, NiFi/Airflow
# 공유 서비스계정 자격증명을 주기적으로 갱신하는 crond뿐이다.
# ---------------------------------------------------------------------
FROM nginx:alpine
RUN apk add --no-cache curl
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY refresh-credentials.sh /docker-entrypoint-credentials/refresh-credentials.sh
COPY docker-entrypoint-credentials.sh /docker-entrypoint-credentials.sh
RUN chmod +x /docker-entrypoint-credentials/refresh-credentials.sh /docker-entrypoint-credentials.sh \
  && mkdir -p /etc/nginx/generated \
  # nginx가 최초 기동 시(자격증명 갱신 전) include 대상이 없어 죽지 않도록 빈 값 스텁을 둔다.
  && printf 'set $nifi_shared_bearer "";\nset $airflow_shared_cookie "";\n' > /etc/nginx/generated/shared-credentials.conf \
  && echo "*/30 * * * * /docker-entrypoint-credentials/refresh-credentials.sh >> /var/log/refresh-credentials.log 2>&1" > /etc/crontabs/root
EXPOSE 80 8080
ENTRYPOINT ["/docker-entrypoint-credentials.sh"]
