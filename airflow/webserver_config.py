"""
Airflow(FAB) 인증을 Keycloak(cerebro realm) OIDC로 설정한다.

- 브라우저는 authorize/logout을 localhost:8543(도달 가능)로, Airflow 서버는 token/jwks를
  host.docker.internal:8543(백채널, 컨테이너 도달 가능)로 사용한다. Keycloak의
  KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true 덕분에 server_metadata_url을 백채널로 조회하면
  authorize_endpoint는 localhost(프론트), token_endpoint는 host.docker.internal(백채널)로 나온다.
- Keycloak realm role(portal_admin/portal_user)을 Airflow 역할(Admin/Viewer)로 매핑한다.
- 자체 서명 Keycloak 인증서는 REQUESTS_CA_BUNDLE(도커컴포즈에서 주입)로 신뢰한다.
"""
import base64
import json
import logging
import os

from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)
from flask_appbuilder.security.manager import AUTH_OAUTH

logger = logging.getLogger(__name__)

AUTH_TYPE = AUTH_OAUTH
# 첫 OIDC 로그인 시 사용자 자동 생성 + 매 로그인마다 realm role 동기화
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_ROLES_MAPPING = {
    "portal_admin": ["Admin"],
    "portal_user": ["Viewer"],
}

_REALM = os.environ.get("KEYCLOAK_REALM", "cerebro")
_BACKCHANNEL = os.environ.get("KEYCLOAK_BACKCHANNEL_URL", "https://host.docker.internal:8543")

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": "airflow",
            "client_secret": os.environ.get("KEYCLOAK_AIRFLOW_CLIENT_SECRET"),
            # 백채널로 메타데이터 조회 → authorize는 localhost(브라우저), token/jwks는 host.docker.internal
            "server_metadata_url": f"{_BACKCHANNEL}/realms/{_REALM}/.well-known/openid-configuration",
            "api_base_url": f"{_BACKCHANNEL}/realms/{_REALM}/protocol/openid-connect",
            "client_kwargs": {"scope": "openid email profile"},
        },
    }
]


def _decode_jwt_claims(token: str) -> dict:
    """서명 검증 없이 JWT payload만 디코드(토큰은 OAuth 플로우에서 이미 검증됨). 역할 추출용."""
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)  # base64 padding
    return json.loads(base64.urlsafe_b64decode(payload))


class KeycloakSecurityManager(FabAirflowSecurityManagerOverride):
    def get_oauth_user_info(self, provider, resp):
        if provider != "keycloak":
            return {}
        claims = _decode_jwt_claims(resp["access_token"])
        roles = claims.get("realm_access", {}).get("roles", [])
        return {
            "username": claims.get("preferred_username"),
            "email": claims.get("email", ""),
            "first_name": claims.get("given_name", ""),
            "last_name": claims.get("family_name", ""),
            "role_keys": roles,
        }


SECURITY_MANAGER_CLASS = KeycloakSecurityManager
