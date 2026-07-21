import { useEffect, useState } from "react";
import { Button, Result, Spin } from "antd";
import { ExportOutlined, ReloadOutlined } from "@ant-design/icons";
import { useLocation } from "react-router-dom";

interface ConsoleFramePageProps {
  kicker: string;
  title: string;
  src: string;
  externalSrc?: string;
  healthcheckSrc?: string;
  waitMessage?: string;
}

export function ConsoleFramePage({ kicker, title, src, externalSrc, healthcheckSrc, waitMessage }: ConsoleFramePageProps) {
  const location = useLocation();
  const [isReady, setIsReady] = useState(!healthcheckSrc);
  const [frameKey, setFrameKey] = useState(0);
  const processGroupId = new URLSearchParams(location.search).get("processGroupId");
  const frameSrc = processGroupId ? `${src}?processGroupId=${encodeURIComponent(processGroupId)}` : src;
  const openSrc =
    processGroupId && externalSrc
      ? `${externalSrc}?processGroupId=${encodeURIComponent(processGroupId)}`
      : (externalSrc ?? frameSrc);

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
      <div className="page-toolbar">
        <div>
          <div className="page-kicker">{kicker}</div>
          <h2 className="page-title">{title}</h2>
        </div>
        <Button icon={<ExportOutlined />} href={openSrc} target="_blank" rel="noreferrer">
          새 창
        </Button>
      </div>
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
