import type { ReactNode } from "react";
import { ConfigProvider } from "antd";
import { cerebroStandardTheme } from "../theme/cerebro";
import { MenuPageHeader, WfIcon, type MenuKey, type WfIconName } from "./WorkflowPageHeader";

/**
 * 표준 화면 틀 — 디자인 작업물(2026-09-17) 규격을 입힌 화면을 감싼다.
 *
 * <ul>
 *   <li>antd 를 표준 테마(cerebroStandardTheme)로: 시그니처 레드 · 표 · 버튼 · 탭 · 페이지 번호 규격</li>
 *   <li>화면 안에서 여는 모달 · 서랍에 std-modal · std-drawer 를 붙인다(포털이라 페이지 클래스가 닿지 않는다)</li>
 *   <li>머리줄: 제목 + 같은 메뉴의 하위 탭 ··· 오른쪽 조작</li>
 * </ul>
 *
 * <p>모양만 바꾼다 - 화면이 쓰는 컴포넌트 · 동작은 그대로 둔다.
 */
export function StdPage({
  menu,
  title,
  icon,
  actions,
  className,
  fill = false,
  children,
}: {
  menu: MenuKey;
  title: string;
  icon: WfIconName;
  actions?: ReactNode;
  className?: string;
  /**
   * 목록 화면 여부. 켜면 화면 높이를 뷰포트에 못 박아, 내역이 0건이어도 표 카드가
   * 바닥까지 내려온다(2026-09-18 요청). 표 카드에는 std-table-card 가 붙어 있어야 한다.
   * 입력 폼 화면(생성·등록)은 내용 길이가 제각각이라 켜지 않는다.
   */
  fill?: boolean;
  children: ReactNode;
}) {
  return (
    <ConfigProvider
      theme={cerebroStandardTheme}
      modal={{ classNames: { root: "std-modal" } }}
      drawer={{ classNames: { root: "std-drawer" } }}
    >
      <div className={`std-page${fill ? " std-page--fill" : ""}${className ? ` ${className}` : ""}`}>
        <MenuPageHeader menu={menu} title={title} icon={<WfIcon name={icon} size={22} strokeWidth={2.2} />} actions={actions} />
        {children}
      </div>
    </ConfigProvider>
  );
}
