import { apiClient, unwrap, type ApiResponse } from "./client";

// 백엔드 SystemCode enum과 일치.
export type SystemCode = "COMMON" | "NIFI" | "AIRFLOW" | "KAFKA" | "ADMIN";
export type AccessAction = "READ" | "WRITE";

export const SYSTEM_LABELS: Record<SystemCode, string> = {
  COMMON: "공통/대시보드",
  NIFI: "ETL",
  AIRFLOW: "Airflow",
  KAFKA: "CDC",
  ADMIN: "계정/권한",
};

export const SYSTEM_ORDER: SystemCode[] = ["COMMON", "NIFI", "AIRFLOW", "KAFKA", "ADMIN"];

// 비트마스크(설계서 §4.3): READ=1, WRITE=7(변경+실행). 2단계 UI라 저장값은 0/1/7.
export const BIT_READ = 1;
export const BIT_WRITE = 7;

export function bitsFor(action: AccessAction): number {
  return action === "WRITE" ? BIT_WRITE : BIT_READ;
}

/** (granted & required) === required. admin 은 무조건 허용. */
export function can(me: AuthzMe | null, system: SystemCode, action: AccessAction): boolean {
  if (!me) return false;
  if (me.admin) return true;
  const granted = me.systemBits?.[system] ?? 0;
  const required = bitsFor(action);
  return (granted & required) === required;
}

export interface AuthzMe {
  userId: string;
  userNm: string;
  email: string | null;
  admin: boolean;
  pwMustChange: boolean;
  roles: string[];
  systemBits: Record<string, number>;
}

export async function changePassword(currentPassword: string, newPassword: string): Promise<void> {
  unwrap((await apiClient.post<ApiResponse<void>>("/auth/password", { currentPassword, newPassword })).data);
}

export interface MenuNode {
  menuId: string;
  parentId: string | null;
  menuNm: string;
  menuUrl: string | null;
  icon: string | null;
  systemCode: string;
  sortOrd: number;
  children: MenuNode[];
}

export interface RoleView {
  roleId: string;
  roleNm: string;
  roleDesc: string | null;
  builtIn: boolean;
  useYn: string;
  userCount: number;
  systemBits: Record<string, number>;
}

export interface RoleDetail {
  roleId: string;
  roleNm: string;
  roleDesc: string | null;
  builtIn: boolean;
  systemBits: Record<string, number>;
  menuOverrides: Record<string, boolean>;
}

export interface MenuCatalogItem {
  menuId: string;
  parentId: string | null;
  menuNm: string;
  menuUrl: string | null;
  systemCode: string;
  requiredBits: number;
  sortOrd: number;
}

export interface AssignmentView {
  userId: string;
  userNm: string;
  email: string | null;
  roleIds: string[];
  roleNames: string[];
}

export interface AuditView {
  id: number;
  occurredAt: string | null;
  actorId: string;
  action: string;
  targetType: string | null;
  targetId: string | null;
  beforeValue: string | null;
  afterValue: string | null;
  detail: string | null;
  clientIp: string | null;
}

export interface AccountView {
  userId: string;
  userNm: string;
  email: string | null;
  telNo: string | null;
  admin: boolean;
  useYn: string;
  lastLoginDt: string | null;
  locked: boolean;
  lockedUntil: string | null;
  loginFailCount: number;
}

// ----- 본인 권한/메뉴 ----------------------------------------------------

export async function getMe(): Promise<AuthzMe> {
  const res = await apiClient.get<ApiResponse<AuthzMe>>("/authz/me");
  return unwrap(res.data);
}

export async function getMenus(): Promise<MenuNode[]> {
  const res = await apiClient.get<ApiResponse<MenuNode[]>>("/authz/menus");
  return unwrap(res.data);
}

// ----- 역할 -------------------------------------------------------------

export async function listRoles(): Promise<RoleView[]> {
  const res = await apiClient.get<ApiResponse<RoleView[]>>("/admin/roles");
  return unwrap(res.data);
}

export async function getRole(roleId: string): Promise<RoleDetail> {
  const res = await apiClient.get<ApiResponse<RoleDetail>>(`/admin/roles/${encodeURIComponent(roleId)}`);
  return unwrap(res.data);
}

export async function createRole(body: { roleId: string; roleNm: string; roleDesc?: string }): Promise<void> {
  unwrap((await apiClient.post<ApiResponse<void>>("/admin/roles", body)).data);
}

export async function updateRole(roleId: string, body: { roleNm?: string; roleDesc?: string }): Promise<void> {
  unwrap((await apiClient.put<ApiResponse<void>>(`/admin/roles/${encodeURIComponent(roleId)}`, body)).data);
}

export async function deleteRole(roleId: string): Promise<void> {
  unwrap((await apiClient.delete<ApiResponse<void>>(`/admin/roles/${encodeURIComponent(roleId)}`)).data);
}

export async function setRolePermissions(roleId: string, systemBits: Record<string, number>): Promise<void> {
  unwrap((await apiClient.put<ApiResponse<void>>(`/admin/roles/${encodeURIComponent(roleId)}/permissions`, systemBits)).data);
}

export async function setRoleMenuOverrides(roleId: string, overrides: Record<string, boolean>): Promise<void> {
  unwrap((await apiClient.put<ApiResponse<void>>(`/admin/roles/${encodeURIComponent(roleId)}/menus`, overrides)).data);
}

export async function listMenuCatalog(): Promise<MenuCatalogItem[]> {
  const res = await apiClient.get<ApiResponse<MenuCatalogItem[]>>("/admin/menus");
  return unwrap(res.data);
}

// ----- 역할 배정 --------------------------------------------------------

export async function listAssignments(): Promise<AssignmentView[]> {
  const res = await apiClient.get<ApiResponse<AssignmentView[]>>("/admin/assignments");
  return unwrap(res.data);
}

export async function setUserRoles(userId: string, roleIds: string[]): Promise<void> {
  unwrap((await apiClient.put<ApiResponse<void>>(`/admin/assignments/${encodeURIComponent(userId)}`, { roleIds })).data);
}

export interface SyncIdentitiesResult {
  total: number;
  nifiSynced: number;
  airflowSynced: number;
}

/** 전체 사용자를 NiFi/Airflow 개인계정으로 일괄 재조정(P5b 이전 배정 사용자 catch-up). */
export async function syncAllIdentities(): Promise<SyncIdentitiesResult> {
  const res = await apiClient.post<ApiResponse<SyncIdentitiesResult>>("/admin/assignments/sync-identities");
  return unwrap(res.data);
}

// ----- 계정 -------------------------------------------------------------

export async function listAccounts(): Promise<AccountView[]> {
  const res = await apiClient.get<ApiResponse<AccountView[]>>("/admin/accounts");
  return unwrap(res.data);
}

export async function createAccount(body: {
  userId: string;
  userNm: string;
  email?: string;
  telNo?: string;
  password: string;
  admin: boolean;
}): Promise<void> {
  unwrap((await apiClient.post<ApiResponse<AccountView>>("/admin/accounts", body)).data);
}

export async function updateAccount(userId: string, body: { userNm?: string; email?: string; telNo?: string }): Promise<void> {
  unwrap((await apiClient.put<ApiResponse<AccountView>>(`/admin/accounts/${encodeURIComponent(userId)}`, body)).data);
}

export async function resetAccountPassword(userId: string, password: string): Promise<void> {
  unwrap((await apiClient.put<ApiResponse<void>>(`/admin/accounts/${encodeURIComponent(userId)}/password`, { password })).data);
}

export async function disableAccount(userId: string): Promise<void> {
  unwrap((await apiClient.put<ApiResponse<void>>(`/admin/accounts/${encodeURIComponent(userId)}/disable`)).data);
}

export async function enableAccount(userId: string): Promise<void> {
  unwrap((await apiClient.put<ApiResponse<void>>(`/admin/accounts/${encodeURIComponent(userId)}/enable`)).data);
}

export async function unlockAccount(userId: string): Promise<void> {
  unwrap((await apiClient.put<ApiResponse<void>>(`/admin/accounts/${encodeURIComponent(userId)}/unlock`)).data);
}

// ----- 감사 로그 --------------------------------------------------------

export interface AuditQuery {
  /** ISO local datetime (YYYY-MM-DDTHH:mm:ss). 미지정 시 서버가 최근 7일. */
  from?: string;
  to?: string;
  limit?: number;
}

export async function listAudit(query: AuditQuery = {}): Promise<AuditView[]> {
  const res = await apiClient.get<ApiResponse<AuditView[]>>(`/admin/audit`, { params: query });
  return unwrap(res.data);
}
