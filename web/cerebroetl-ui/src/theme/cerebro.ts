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
