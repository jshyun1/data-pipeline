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
# /api 리버스 프록시(같은 docker 네트워크 안의 pipeline-api)뿐이다.
# ---------------------------------------------------------------------
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY certs/localhost.crt /etc/nginx/certs/localhost.crt
COPY certs/localhost.key /etc/nginx/certs/localhost.key
EXPOSE 80 8080
