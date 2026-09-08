import { AlertOutlined, SettingOutlined } from "@ant-design/icons";
import { useMemo, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { ConsoleFramePage } from "./ConsoleFramePage";

type NifiSettingsTarget = "errors" | "parameters";

const NIFI_SETTINGS_ITEMS: Array<{
  key: NifiSettingsTarget;
  label: string;
  src: string;
  menuTitles: string[];
  icon: ReactNode;
}> = [
  {
    key: "errors",
    label: "오류 확인 창",
    src: "/nifi/",
    menuTitles: ["공지 게시판", "Bulletin Board"],
    icon: <AlertOutlined />,
  },
  {
    key: "parameters",
    label: "전역변수 설정",
    src: "/nifi/",
    menuTitles: ["파라미터 컨텍스트", "Parameter Contexts"],
    icon: <SettingOutlined />,
  },
];

export function NifiSettingsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const targetParam = searchParams.get("target");
  const selectedKey: NifiSettingsTarget | null =
    targetParam === "errors" || targetParam === "parameters" ? targetParam : null;
  const selectedItem = useMemo(
    () => NIFI_SETTINGS_ITEMS.find((item) => item.key === selectedKey) ?? null,
    [selectedKey],
  );

  const selectItem = (key: NifiSettingsTarget) => {
    setSearchParams({ target: key });
  };

  return (
    <div className="nifi-settings-page">
      <aside className="nifi-settings-list" aria-label="NiFi 관리 설정 목록">
        {NIFI_SETTINGS_ITEMS.map((item) => (
          <button
            key={item.key}
            type="button"
            className={item.key === selectedKey ? "nifi-settings-list-item active" : "nifi-settings-list-item"}
            onClick={() => selectItem(item.key)}
          >
            <span className="nifi-settings-list-icon" aria-hidden="true">
              {item.icon}
            </span>
            <span>{item.label}</span>
          </button>
        ))}
      </aside>
      <section className="nifi-settings-frame" aria-label={selectedItem?.label ?? "NiFi 관리 설정"}>
        {selectedItem ? (
          <ConsoleFramePage
            key={selectedItem.key}
            title={selectedItem.label}
            src={selectedItem.src}
            healthcheckSrc="/nifi/"
            waitMessage="NiFi 관리 설정을 준비하는 중입니다"
            nifiAutoOpenMenuTitles={selectedItem.menuTitles}
          />
        ) : (
          <div className="nifi-settings-empty" aria-hidden="true" />
        )}
      </section>
    </div>
  );
}
