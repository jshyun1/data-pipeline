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
