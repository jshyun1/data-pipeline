import { useEffect, useState, type SyntheticEvent } from "react";
import { Button, Result, Spin } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import { useLocation } from "react-router-dom";

const HIDE_TOOL_LOGO_STYLE_ID = "cerebro-hide-tool-logo";
// NiFi/Airflow 각자의 로고를 감춰서 "따로 노는 느낌" 없이 하나의 Cerebro ETL처럼
// 보이게 한다. 번들 분석으로 실제 렌더링되는 요소를 확인한 선택자:
// - NiFi: 라우트 가드 로딩 중에만 뜨는 스플래시 오버레이(.splash/.splash-img,
//   nifi-drop-splash*.svg 배경) - 툴바 로고가 아니라 부트 스플래시였음.
// - Airflow: <img alt="Logo">는 커스텀 테마 아이콘을 설정했을 때만 렌더링되는
//   코드 경로라 지금 설정에선 절대 매치되지 않음. 실제로는 네브바 홈 링크에
//   인라인 SVG(viewBox="0 0 35 35", 5색 팬휠)로 항상 그려짐 - 번들 전체에서
//   이 viewBox를 쓰는 요소가 그것 하나뿐이라 안전하게 특정 가능.
// svg 자체만 감추면 Chakra가 고정 크기(boxSize)로 감싸둔 링크/버튼 껍데기가
// 그대로 남아 빈 칸으로 보인다(실제로 사용자가 스크린샷으로 확인) - :has()로
// 그 svg를 직접 담고 있는 조상 요소째로 접어서 빈 칸이 안 남게 한다.
const HIDE_TOOL_LOGO_CSS = `
  .splash, .context-logo, img[alt="Logo"] { display: none !important; }
  svg[viewBox="0 0 35 35"] { display: none !important; }
  :has(> svg[viewBox="0 0 35 35"]) { display: none !important; }
`;

function hideToolLogo(event: SyntheticEvent<HTMLIFrameElement>) {
  try {
    const doc = event.currentTarget.contentDocument;
    if (!doc || doc.getElementById(HIDE_TOOL_LOGO_STYLE_ID)) {
      return;
    }
    const style = doc.createElement("style");
    style.id = HIDE_TOOL_LOGO_STYLE_ID;
    style.textContent = HIDE_TOOL_LOGO_CSS;
    doc.head.appendChild(style);
  } catch {
    // Keycloak 로그인 리다이렉트 등 cross-origin 문서인 동안은 접근이 막힌다 -
    // 같은 출처(NiFi/Airflow 자체 화면)로 돌아오면 다음 onLoad에서 다시 시도된다.
  }
}

interface ConsoleFramePageProps {
  title: string;
  src: string;
  healthcheckSrc?: string;
  waitMessage?: string;
}

function withProcessGroupId(url: string, processGroupId: string) {
  // NiFi의 Angular Router는 useHash:true(해시 기반)라 실제 라우트는 서버로
  // 전송되지 않는 URL 프래그먼트(#/...)에 있어야 한다 (canvas 라우팅이
  // router.navigate(["/process-groups", id])로 구현돼 있고, 라우터 모듈이
  // RouterModule.forRoot(routes,{useHash:!0})로 등록된 것을 번들에서 확인).
  // 일반 경로 세그먼트(/nifi/process-groups/{id})로 요청하면 서버가 실제
  // 리소스로 찾다가 없어서 자체 404(#/404)로 리다이렉트해버린다.
  const base = url.endsWith("/") ? url : `${url}/`;
  return `${base}#/process-groups/${encodeURIComponent(processGroupId)}`;
}

export function ConsoleFramePage({ title, src, healthcheckSrc, waitMessage }: ConsoleFramePageProps) {
  const location = useLocation();
  const [isReady, setIsReady] = useState(!healthcheckSrc);
  const [frameKey, setFrameKey] = useState(0);
  const processGroupId = new URLSearchParams(location.search).get("processGroupId");
  const frameSrc = processGroupId ? withProcessGroupId(src, processGroupId) : src;

  useEffect(() => {
    if (!healthcheckSrc) {
      setIsReady(true);
      return;
    }

    let isCancelled = false;
    let timerId: number | undefined;

    const checkReady = async () => {
      try {
        const response = await fetch(healthcheckSrc, {
          cache: "no-store",
          credentials: "include",
        });

        if (!isCancelled && response.status < 500) {
          setIsReady(true);
          return;
        }
      } catch {
        if (!isCancelled) {
          setIsReady(false);
        }
      }

      if (!isCancelled) {
        timerId = window.setTimeout(checkReady, 5000);
      }
    };

    setIsReady(false);
    void checkReady();

    return () => {
      isCancelled = true;
      if (timerId) {
        window.clearTimeout(timerId);
      }
    };
  }, [healthcheckSrc]);

  const reloadFrame = () => {
    setFrameKey((key) => key + 1);
  };

  return (
    <div className="console-page">
      <div className="console-frame-shell">
        {isReady ? (
          <iframe
            key={frameKey}
            className="console-frame"
            title={title}
            src={frameSrc}
            onLoad={hideToolLogo}
          />
        ) : (
          <Result
            className="console-wait"
            icon={<Spin size="large" />}
            title={waitMessage ?? "관리 콘솔을 준비하는 중입니다"}
            extra={
              <Button icon={<ReloadOutlined />} onClick={reloadFrame}>
                다시 확인
              </Button>
            }
          />
        )}
      </div>
    </div>
  );
}
