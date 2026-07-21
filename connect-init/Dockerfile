FROM alpine:3.20

# curl/gettext(envsubst)를 빌드 시점에 심어둔다 — register-connector.sh가
# 컨테이너 기동 시(런타임)에 apk add를 하면 폐쇄망에서 실패하기 때문.
RUN apk add --no-cache curl gettext
