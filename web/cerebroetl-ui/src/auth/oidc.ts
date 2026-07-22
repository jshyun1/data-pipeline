import { UserManager, WebStorageStateStore, type UserManagerSettings } from "oidc-client-ts";

// Keycloak(cerebro realm)로 로그인하기 위한 OIDC 설정.
// authority는 브라우저·백엔드 공통 canonical URL(host.docker.internal)이라 토큰 iss와 일치한다.
// 빌드 시 주입되지 않으면 로컬 기본값을 쓴다(운영 빌드에서 VITE_* 로 덮어쓸 수 있음).
const AUTHORITY =
  import.meta.env.VITE_OIDC_AUTHORITY ?? "https://host.docker.internal:8543/realms/cerebro";
const CLIENT_ID = import.meta.env.VITE_OIDC_CLIENT_ID ?? "cerebro-portal";

const settings: UserManagerSettings = {
  authority: AUTHORITY,
  client_id: CLIENT_ID,
  redirect_uri: `${window.location.origin}/auth/callback`,
  post_logout_redirect_uri: `${window.location.origin}/login`,
  response_type: "code", // Authorization Code + PKCE
  scope: "openid profile email",
  // 새로고침해도 세션 유지되도록 localStorage에 보관
  userStore: new WebStorageStateStore({ store: window.localStorage }),
  automaticSilentRenew: true,
  monitorSession: false,
};

export const userManager = new UserManager(settings);

export async function getAccessToken(): Promise<string | null> {
  const user = await userManager.getUser();
  return user && !user.expired ? user.access_token : null;
}
