import { useState, type MouseEvent as ReactMouseEvent } from "react";

/** 접힌 옆 패널의 폭. ETL 관리 화면(.console-side.collapsed)과 같다. */
const COLLAPSED_WIDTH = 30;

export interface SideDockState {
  collapsed: boolean;
  toggle: () => void;
  width: number;
  /** 격자 열 폭(접히면 30px). */
  column: string;
  startResize: (event: ReactMouseEvent<HTMLButtonElement>) => void;
}

/**
 * 옆 패널 접기 · 너비 조절 상태. ETL 관리 화면(ConsoleFramePage)의 트리 · 상세 패널과 같은 동작을
 * 격자 화면(워크플로우 설계 · 로그)에서 쓰려고 뺐다(2026-09-18 요청).
 * 너비는 minWidth ~ min(maxWidth, 화면 45%) 사이로 묶어 가운데 영역이 사라지지 않게 한다.
 */
export function useSideDock(side: "left" | "right", defaultWidth: number, minWidth: number, maxWidth: number): SideDockState {
  const [collapsed, setCollapsed] = useState(false);
  const [width, setWidth] = useState(defaultWidth);

  const startResize = (event: ReactMouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = width;
    const resizeTo = (clientX: number) => {
      const delta = side === "left" ? clientX - startX : startX - clientX;
      const limit = Math.max(minWidth, Math.min(maxWidth, Math.floor(window.innerWidth * 0.45)));
      setWidth(Math.min(limit, Math.max(minWidth, startWidth + delta)));
    };
    const handleMouseMove = (moveEvent: MouseEvent) => resizeTo(moveEvent.clientX);
    const handleMouseUp = (upEvent: MouseEvent) => {
      resizeTo(upEvent.clientX);
      document.body.classList.remove("is-resizing-console-panel");
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
    document.body.classList.add("is-resizing-console-panel");
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
  };

  return {
    collapsed,
    toggle: () => setCollapsed((current) => !current),
    width,
    // 브라우저 배율을 올리면 보이는 폭이 좁아진다 - 그때는 끌어 둔 폭 대신 화면 몫(26vw)까지만 쓴다.
    // 그러지 않으면 양옆 패널이 가운데를 밀어내 화면 밖으로 넘친다(2026-10-05 요청).
    column: collapsed ? `${COLLAPSED_WIDTH}px` : `min(${width}px, 26vw)`,
    startResize,
  };
}
