"""
Airflow(FAB) 인증 - Keycloak/OIDC 제거, 공유 서비스계정 기반 DB 인증(AUTH_DB)으로 전환.

로그인 계정은 airflow-init이 기동 시마다 멱등하게 생성하는
AIRFLOW_ADMIN_USERNAME/AIRFLOW_ADMIN_PASSWORD(.env)를 그대로 사용한다.
"""
from flask_appbuilder.security.manager import AUTH_DB

AUTH_TYPE = AUTH_DB
