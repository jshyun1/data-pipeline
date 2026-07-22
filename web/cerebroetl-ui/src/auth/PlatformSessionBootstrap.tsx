import { useCallback, useEffect, useRef, useState, type ReactNode, type SyntheticEvent } from "react";
import { Button, Result, Spin } from "antd";
import { ReloadOutlined } from "@ant-design/icons";

type PlatformKey = "airflow" | "nifi" | "kafka";

const LABELS: Record<PlatformKey, string> = {
  airflow: "Airflow",
  nifi: "NiFi",
  kafka: "Kafka Connect",
};

const CHECK_URLS: Record<PlatformKey, string> = {
  airflow: "/airflow/ui/auth/me",
  // NiFi 2.x에는 /access/config가 없으므로 인증이 필요한 실제 Flow API로 확인한다.
  nifi: "/nifi-api/flow/process-groups/root/status",
  kafka: "/kafka-connect-api/connectors",
};

const EMPTY_STATUS: Record<PlatformKey, boolean> = {
  airflow: false,
  nifi: false,
  kafka: false,
};

async function checkPlatform(key: PlatformKey) {
  try {
    const response = await fetch(CHECK_URLS[key], {
      cache: "no-store",
      credentials: "include",
    });
    return response.ok;
  } catch {
    return false;
  }
}

/**
 * 포털의 Keycloak 로그인이 끝난 직후 Airflow/NiFi 브라우저 세션을 숨김 iframe으로
 * 미리 만든다. 두 도구 모두 통합 웹 reverse proxy 아래의 same-origin 경로를 사용하므로
 * 완료된 세션 쿠키는 이후 실제 관리 iframe에서도 그대로 사용된다.
 *
 * Kafka Connect는 사용자 로그인 세션이 없으므로 REST 연결 가능 여부만 확인한다.
 */
export function PlatformSessionBootstrap({ children }: { children: ReactNode }) {
  const airflowProviderSelected = useRef(false);
  const [attempt, setAttempt] = useState(0);
  const [status, setStatus] = useState<Record<PlatformKey, boolean>>(EMPTY_STATUS);
  const [timedOut, setTimedOut] = useState(false);

  const retry = useCallback(() => {
    airflowProviderSelected.current = false;
    setStatus(EMPTY_STATUS);
    setTimedOut(false);
    setAttempt((value) => value + 1);
  }, []);

  const continueAirflowLogin = useCallback((event: SyntheticEvent<HTMLIFrameElement>) => {
    if (airflowProviderSelected.current) return;

    try {
      const frame = event.currentTarget;
      const document = frame.contentDocument;
      // FAB OAuth 선택 버튼은 href가 없고 inline script가 id에 click handler를 연결한다.
      const keycloakLogin = document?.querySelector<HTMLAnchorElement>("#btn-signin-keycloak");

      if (keycloakLogin) {
        airflowProviderSelected.current = true;
        keycloakLogin.click();
      }
    } catch {
      // Keycloak로 이동해 있는 동안에는 cross-origin 문서이므로 접근할 수 없다.
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    const startedAt = Date.now();
    let current = { ...EMPTY_STATUS };
    let timerId: number | undefined;

    const poll = async () => {
      const keys = Object.keys(CHECK_URLS) as PlatformKey[];
      const results = await Promise.all(keys.map(async (key) => [key, await checkPlatform(key)] as const));
      if (cancelled) return;

      for (const [key, isReady] of results) {
        current[key] = current[key] || isReady;
      }
      setStatus({ ...current });

      const allReady = Object.values(current).every(Boolean);
      if (allReady || Date.now() - startedAt >= 45_000) {
        if (!allReady) setTimedOut(true);
        return;
      }
      timerId = window.setTimeout(poll, 1_000);
    };

    void poll();
    return () => {
      cancelled = true;
      if (timerId) window.clearTimeout(timerId);
    };
  }, [attempt]);

  const ready = Object.values(status).every(Boolean);
  const pending = (Object.keys(status) as PlatformKey[]).filter((key) => !status[key]);

  if (ready) return children;

  return (
    <div className="platform-bootstrap">
      {/* 기존 Keycloak 브라우저 세션으로 각 도구의 세션 쿠키를 선발급한다. */}
      {!status.airflow && (
        <iframe
          key={`airflow-${attempt}`}
          className="platform-bootstrap-frame"
          title="Airflow SSO 준비"
          src="/airflow/api/v2/auth/login?next=/airflow/"
          onLoad={continueAirflowLogin}
        />
      )}
      {!status.nifi && (
        <iframe
          key={`nifi-${attempt}`}
          className="platform-bootstrap-frame"
          title="NiFi SSO 준비"
          src="/nifi/"
        />
      )}

      {timedOut ? (
        <Result
          status="warning"
          title="일부 통합 서비스 연결이 지연되고 있습니다"
          subTitle={`연결 대기: ${pending.map((key) => LABELS[key]).join(", ")}`}
          extra={
            <Button type="primary" icon={<ReloadOutlined />} onClick={retry}>
              다시 연결
            </Button>
          }
        />
      ) : (
        <div className="platform-bootstrap-status">
          <Spin size="large" />
          <h2>통합 서비스 연결 중</h2>
          <p>{pending.map((key) => LABELS[key]).join(", ")} 세션과 연결 상태를 준비하고 있습니다.</p>
        </div>
      )}
    </div>
  );
}
