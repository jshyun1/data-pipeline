import { useEffect, useState } from "react";
import { Button, Result, Spin } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import { useLocation } from "react-router-dom";

interface ConsoleFramePageProps {
  title: string;
  src: string;
  healthcheckSrc?: string;
  waitMessage?: string;
}

function withProcessGroupId(url: string, processGroupId: string) {
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}processGroupId=${encodeURIComponent(processGroupId)}`;
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
          <iframe key={frameKey} className="console-frame" title={title} src={frameSrc} />
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
