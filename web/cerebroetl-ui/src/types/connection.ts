// web/backend의 com.company.pipeline.connection.dto.* 와 1:1로 맞춘 타입.
export type DbType = "ORACLE" | "POSTGRESQL";
export type ConnectionStatus = "UNKNOWN" | "TESTING" | "SUCCESS" | "FAILED";

export interface ConnectionResponse {
  id: number;
  name: string;
  dbType: DbType;
  host: string;
  port: number;
  databaseName: string | null;
  serviceName: string | null;
  schemaName: string | null;
  username: string;
  nifiControllerServiceId: string | null;
  nifiControllerServiceName: string | null;
  status: ConnectionStatus;
  lastTestedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ConnectionCreateRequest {
  name: string;
  dbType: DbType;
  host: string;
  port: number;
  databaseName?: string;
  serviceName?: string;
  schemaName?: string;
  username: string;
  password: string;
}

// password를 비우면(undefined/빈 문자열) 백엔드가 기존 값을 그대로 유지한다.
export interface ConnectionUpdateRequest {
  name: string;
  dbType: DbType;
  host: string;
  port: number;
  databaseName?: string;
  serviceName?: string;
  schemaName?: string;
  username: string;
  password?: string;
}

export interface ConnectionTestRequest {
  dbType: DbType;
  host: string;
  port: number;
  databaseName?: string;
  serviceName?: string;
  username: string;
  password: string;
}

export interface ConnectionTestResponse {
  success: boolean;
  testedAt: string;
  latencyMs: number;
  message: string;
}

export interface ConnectionReferenceResponse {
  referenceType: "CDC" | "ETL";
  referenceId: number;
  name: string;
  role: string;
  status: string | null;
}

export interface ConnectionUsageResponse {
  connectionId: number;
  cdcSourceCount: number;
  cdcTargetCount: number;
  etlJobCount: number;
  deletable: boolean;
  references: ConnectionReferenceResponse[];
}

export interface CdcPrerequisiteCheckResponse {
  code: string;
  label: string;
  status: "PASS" | "WARN" | "FAIL" | "UNKNOWN";
  actualValue: string | null;
  guidance: string;
}

export interface CdcPrerequisiteResponse {
  connectionId: number;
  dbType: DbType;
  checkedAt: string;
  overallStatus: "PASS" | "WARN" | "FAIL";
  checks: CdcPrerequisiteCheckResponse[];
}
