import type { ThemeConfig } from "antd";

/**
 * CEREBRO 디자인 시스템(디자인 초안 «CEREBRO ETL_UI DESIGN/css/variables.css»)의 antd 쪽 값.
 * CSS 쪽 값은 index.css 의 :root 토큰에 둔다 - 색·글꼴을 바꿀 때는 두 곳을 함께 바꾼다.
 */

/** 글꼴 파일은 index.css 의 @font-face 가 번들에서 싣는다(폐쇄망 설치도 CDN 없이 뜬다). */
export const CEREBRO_FONT_FAMILY =
  "'Pretendard Variable', Pretendard, -apple-system, BlinkMacSystemFont, system-ui, 'Segoe UI', 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif";

export const CEREBRO_RED = "#ff3b30";

/**
 * 앱 전체에는 글꼴만 입힌다. antd 컴포넌트는 body 글꼴을 물려받지 않고 자기 토큰을 쓰므로
 * 여기서 넣어야 표·버튼까지 같은 글꼴이 된다. 강조색(파랑 → 빨강)은 화면 단위로 입힌다 -
 * 아직 디자인을 적용하지 않은 화면의 버튼 색까지 한꺼번에 바뀌지 않도록.
 */
export const appTheme: ThemeConfig = {
  token: { fontFamily: CEREBRO_FONT_FAMILY },
};

/** 시그니처 레드. 디자인을 적용한 화면을 이 테마의 ConfigProvider 로 감싼다. */
export const cerebroBrandTheme: ThemeConfig = {
  token: {
    colorPrimary: CEREBRO_RED,
    colorPrimaryHover: "#e02b21",
    colorPrimaryActive: "#c9241b",
  },
};

/** 로그인 카드: 48px 입력칸(옅은 하늘색 바탕, 누르면 흰 바탕 + 빨간 테두리)과 빨간 버튼. */
export const loginTheme: ThemeConfig = {
  token: {
    ...cerebroBrandTheme.token,
    borderRadius: 8,
    controlHeightLG: 48,
    fontSizeLG: 14,
  },
  components: {
    Input: {
      colorBgContainer: "#eef2f8",
      hoverBg: "#eef2f8",
      activeBg: "#ffffff",
      colorBorder: "#e2e8f0",
      colorTextPlaceholder: "#94a3b8",
      activeShadow: "0 0 0 3px rgba(255, 59, 48, 0.15)",
      paddingInlineLG: 16,
    },
    Button: {
      primaryShadow: "0 4px 12px rgba(255, 59, 48, 0.2)",
      contentFontSizeLG: 15,
      fontWeight: 600,
    },
  },
};

/**
 * 표준 화면 테마 — 디자인 작업물(CEREBRO_ETL_UI_DESIGN_20260917 «CEREBRO Enterprise UI Standard v2.0»)의
 * 표 · 버튼 · 탭 · 페이지 번호 규격을 antd 토큰으로 옮겼다(README 4절 · css/base.css).
 *
 * <p>CSS 로 antd 를 덮어쓰면 antd 가 올린 선택자와 우선순위 싸움이 되고, 버전이 오르면 조용히 깨진다.
 * 토큰으로 줄 수 있는 것은 토큰으로 주고, 토큰이 없는 것(배지 알약 모양 · 모달 머리/바닥 띠 · 페이지 번호 배치)만
 * index.css 의 «표준 화면» 블록(.std-page · .std-modal)에 둔다. {@code components/StdPage} 가 이 테마로 감싼다.
 *
 * <p>ETL 화면은 ETL 담당 작업자 소관이라 이 테마를 쓰지 않는다.
 */
export const cerebroStandardTheme: ThemeConfig = {
  token: {
    ...cerebroBrandTheme.token,
    fontSize: 13,
    borderRadius: 6,
    borderRadiusSM: 4,
    colorText: "#1e293b",
    colorTextSecondary: "#64748b",
    colorBorder: "#cbd5e1",
    colorBorderSecondary: "#e2e8f0",
    colorError: "#ef4444",
    colorSuccess: "#16a34a",
    colorWarning: "#d97706",
    colorInfo: "#2563eb",
  },
  components: {
    // 표: 머리 12px 굵게 · #fafbfc 바탕, 칸 12.5px · 12px 14px 여백, 줄 경계 #f1f5f9, 머리 칸 사이 세로선 없음.
    Table: {
      headerBg: "#fafbfc",
      headerColor: "#475569",
      headerSplitColor: "transparent",
      headerBorderRadius: 0,
      headerSortActiveBg: "#f1f5f9",
      headerSortHoverBg: "#f1f5f9",
      bodySortBg: "#fcfcfd",
      rowHoverBg: "#f8fafc",
      rowExpandedBg: "#fbfcfd",
      rowSelectedBg: "#fff1f0",
      rowSelectedHoverBg: "#ffe8e6",
      borderColor: "#f1f5f9",
      cellFontSize: 12.5,
      cellFontSizeMD: 12.5,
      cellFontSizeSM: 12,
      cellPaddingBlock: 12,
      cellPaddingInline: 14,
      cellPaddingBlockMD: 12,
      cellPaddingInlineMD: 14,
      cellPaddingBlockSM: 8,
      cellPaddingInlineSM: 12,
    },
    // 버튼: 기본 32px · 12.5px 굵게, 표 안 작은 버튼 24px · 11.5px. 보조 버튼은 회색 테두리 → 올리면 진한 회색.
    Button: {
      fontWeight: 600,
      contentFontSize: 12.5,
      contentFontSizeSM: 11.5,
      paddingInline: 14,
      paddingInlineSM: 8,
      defaultColor: "#475569",
      defaultBorderColor: "#cbd5e1",
      defaultHoverBg: "#f8fafc",
      defaultHoverColor: "#0f172a",
      defaultHoverBorderColor: "#94a3b8",
      defaultActiveBg: "#f1f5f9",
      defaultActiveColor: "#0f172a",
      defaultActiveBorderColor: "#94a3b8",
      defaultShadow: "none",
      dangerShadow: "none",
      primaryShadow: "0 2px 6px rgba(255, 59, 48, 0.22)",
    },
    Tabs: {
      titleFontSize: 13,
      itemColor: "#64748b",
      itemHoverColor: "#0f172a",
      horizontalItemGutter: 28,
    },
    // 페이지 번호: 28px 칸, 지금 페이지는 빨간 바탕 흰 글자.
    Pagination: {
      itemSize: 28,
      itemSizeSM: 26,
      itemActiveBg: CEREBRO_RED,
      itemActiveColor: "#ffffff",
      itemActiveColorHover: "#ffffff",
    },
    Segmented: {
      trackBg: "#f1f5f9",
      itemColor: "#64748b",
      itemHoverColor: "#0f172a",
      itemSelectedBg: "#ffffff",
      itemSelectedColor: "#0f172a",
    },
    Modal: {
      headerBg: "#fafbfc",
      footerBg: "#fafbfc",
      titleFontSize: 15.5,
      titleColor: "#0f172a",
    },
    Card: {
      headerFontSize: 15,
    },
  },
};
