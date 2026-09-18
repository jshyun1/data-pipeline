import type { ReactNode } from "react";
import { DoubleLeftOutlined, DoubleRightOutlined } from "@ant-design/icons";
import type { SideDockState } from "./useSideDock";

/**
 * 옆 패널 한 칸: 접기 버튼 · 너비 조절 손잡이 + 패널.
 * 접혀도 패널은 지우지 않고 CSS 로 숨긴다 - 속성창에서 고치던 값 · AI 대화가 접었다 펴도 남아 있어야 한다.
 */
export function SideDock({ side, dock, label, children }: {
  side: "left" | "right";
  dock: SideDockState;
  /** «트리 패널» · «속성 패널» 같은 이름 - 버튼 설명에 쓴다. */
  label: string;
  children: ReactNode;
}) {
  const openIcon = side === "left" ? <DoubleRightOutlined /> : <DoubleLeftOutlined />;
  const closeIcon = side === "left" ? <DoubleLeftOutlined /> : <DoubleRightOutlined />;
  return (
    <div className={`side-dock side-dock--${side}${dock.collapsed ? " collapsed" : ""}`}>
      {!dock.collapsed && (
        <button type="button" className="console-resize-handle" aria-label={`${label} 너비 조절`} title={`${label} 너비 조절`}
                onMouseDown={dock.startResize} />
      )}
      <button type="button" className="console-curtain-toggle"
              aria-label={dock.collapsed ? `${label} 열기` : `${label} 닫기`}
              title={dock.collapsed ? `${label} 열기` : `${label} 닫기`}
              onClick={dock.toggle}>
        {dock.collapsed ? openIcon : closeIcon}
      </button>
      {children}
    </div>
  );
}
