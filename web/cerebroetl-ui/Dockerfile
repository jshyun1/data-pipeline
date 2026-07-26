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
# Vite는 import.meta.env.VITE_*를 빌드 시점에 정적 파일로 굳혀버린다 - 런타임
# docker-compose environment로는 절대 못 바꾼다. 이 ARG를 안 넘기면 auth/oidc.ts의
# 기본값(https://localhost:8543/...)이 그대로 박혀서, localhost가 아닌 실제 서버
# 주소로 접속하는 사용자는 로그인 리다이렉트가 항상 localhost로 튀는 문제가 생긴다
# (다른 호스트에 배포해보고서야 드러난 버그 - PORTAL_PUBLIC_HOST로 계산해서 넘길 것).
ARG VITE_OIDC_AUTHORITY
ENV VITE_OIDC_AUTHORITY=${VITE_OIDC_AUTHORITY}
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
