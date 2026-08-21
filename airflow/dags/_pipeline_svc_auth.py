"""pipeline-api 호출에 X-Service-Token 을 자동으로 붙인다(P4 인가 강제 대응, 설계서 §7.6).

DAG 들은 bare ``requests.get/post`` 로 pipeline-api 를 호출하는데, 인가 강제(``AUTHZ_ENFORCEMENT_ENABLED``)
가 켜지면 이 호출들이 401/403 으로 죽는다. 각 호출부를 고치는 대신, requests 세션 계층을 한 번
패치해서 pipeline-api 로 가는 요청에만 서비스 토큰 헤더를 얹는다. import 만으로 적용된다.

``PIPELINE_SERVICE_TOKEN`` 이 비어 있으면(=강제 off 환경) 아무 것도 하지 않는다.
"""

import os

import requests

_TOKEN = os.environ.get("PIPELINE_SERVICE_TOKEN", "")
_API_HOST = "pipeline-api:8081"

if _TOKEN and not getattr(requests.sessions.Session, "_cerebro_svc_patched", False):
    _original_request = requests.sessions.Session.request

    def _request_with_service_token(self, method, url, **kwargs):
        if _API_HOST in str(url):
            headers = kwargs.get("headers") or {}
            headers.setdefault("X-Service-Token", _TOKEN)
            kwargs["headers"] = headers
        return _original_request(self, method, url, **kwargs)

    requests.sessions.Session.request = _request_with_service_token
    requests.sessions.Session._cerebro_svc_patched = True
