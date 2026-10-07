import {
  CloseOutlined,
  DownOutlined,
  EditOutlined,
  EyeInvisibleOutlined,
  EyeOutlined,
  QuestionCircleOutlined,
  UpOutlined,
  UploadOutlined,
} from "@ant-design/icons";
import { Button, Descriptions, Input, message, Modal, Radio, Result, Select, Tag } from "antd";
import type { DragEvent, KeyboardEvent, ReactNode, UIEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  listConnections,
  listConnectionColumns,
  listConnectionSchemas,
  listConnectionTables,
  validateConnectionSql,
  type ColumnMetadataResponse,
} from "../api/connections";
import {
  createInitialDbToDbFlow,
  createFileLoadFlow,
  createHttpLoadFlow,
  createUnstructuredLoadFlow,
  getNifiProcessGroupTree,
  listAirflowDagCatalog,
  listRootNifiControllerServices,
  refreshNifiProcessGroupTree,
  syncAirflowDagCatalog,
  testHttpApi,
  uploadFileLoadFiles,
  type NifiProcessGroupEntity,
  type NifiControllerServiceEntity,
  type NifiProcessGroupTreeNode,
} from "../api/platform";
import { listEtlJobs, syncEtlJobs } from "../api/etlJobs";
import { validateCalciteSql } from "../api/formulaHelp";
import type { ConnectionResponse } from "../types/connection";
import { useNavigate } from "react-router-dom";

const WIZARD_STEPS = [
  { id: "type", label: "유형 선택" },
  { id: "basic", label: "기본 정보" },
  { id: "connection", label: "연결 설정" },
  { id: "query", label: "적재 쿼리" },
  { id: "target", label: "컬럼 매핑" },
] as const;

type WizardStepId = (typeof WIZARD_STEPS)[number]["id"];
type EtlType = "DB_TO_DB" | "FILE_LOAD" | "HTTP" | "UNSTRUCTURED";
type EtlTypeOption = { value: EtlType; title: string; description: string; disabled?: boolean };
type LoadMode = "INSERT" | "TRUNCATE" | "UPSERT";
type FileExtension = "csv" | "excel" | "log" | "json";
type NifiDatabaseType = "Generic" | "Oracle 12+" | "PostgreSQL" | "MySQL" | "MS SQL 2012+";
type ConnectionDbType = ConnectionResponse["dbType"];
const COLUMN_MAPPING_ROW_HEIGHT = 40;
const LOAD_SQL_SOURCE_TABLE = "__LOAD_SQL__";
const SOURCE_COLUMN_DRAG_TYPE = "application/x-etl-source-column";

const STEP_NUMBER: Record<WizardStepId, number> = {
  type: 1,
  basic: 2,
  connection: 3,
  query: 4,
  target: 5,
};

const TYPE_OPTIONS: EtlTypeOption[] = [
  {
    value: "DB_TO_DB",
    title: "DB -> DB 적재",
    description: "원천 DB에서 읽어 대상에 적재",
  },
  {
    value: "FILE_LOAD",
    title: "파일 적재",
    description: "서버 폴더의 파일을 읽어 적재",
  },
  {
    value: "HTTP",
    title: "HTTP 수집",
    description: "HTTP로 받은 데이터를 적재",
  },
  {
    value: "UNSTRUCTURED",
    title: "비정형 전송",
    description: "이미지, 문서를 받아 저장",
  },
];

const FILE_EXTENSION_OPTIONS: Array<{ value: FileExtension; label: string; disabled?: boolean }> = [
  { value: "csv", label: "csv" },
  { value: "excel", label: "excel" },
  { value: "log", label: "log", disabled: true },
  { value: "json", label: "json", disabled: true },
];

const DATABASE_TYPE_OPTIONS: NifiDatabaseType[] = [
  "PostgreSQL",
  "Oracle 12+",
  "MySQL",
  "MS SQL 2012+",
  "Generic",
];

const CONNECTION_TO_NIFI_DATABASE_TYPE: Record<ConnectionDbType, NifiDatabaseType> = {
  ORACLE: "Oracle 12+",
  POSTGRESQL: "PostgreSQL",
  MYSQL: "MySQL",
};

interface ProcessGroupOption {
  id: string;
  label: string;
}

interface ControllerServiceOption {
  id: string;
  name: string;
  displayName: string;
  type: string;
}

interface BasicInfo {
  jobName: string;
  parentGroupId: string;
  parentGroupPath: string;
  comments: string;
  fileExtension: FileExtension;
  files: File[];
}

interface ConnectionInfo {
  httpMethod: "GET" | "POST";
  apiUrl: string;
  apiKey: string;
  sourceServiceId: string;
  sourceConnectionId?: number | null;
  sourceDatabaseType: NifiDatabaseType;
  sourceSchema: string;
  sourceTable: string;
  targetServiceId: string;
  targetDatabaseType: NifiDatabaseType;
  targetSchema: string;
  targetTable: string;
}

interface SourceColumnMetadata extends ColumnMetadataResponse {
  table: string;
}

interface ColumnMapping {
  id: string;
  targetColumn: string;
  sourceTable: string;
  sourceColumn: string;
  logic: string;
}

interface CompletionSummary {
  processGroupId: string;
  processGroupPath: string;
  processorCount: number;
  loadMode: LoadMode;
  etlType: EtlType;
  canvasReady: boolean;
  dagId?: string | null;
  dagReady: boolean;
}

interface EtlCreateDraft {
  activeStep: WizardStepId;
  etlType: EtlType;
  completedSteps: WizardStepId[];
  basicInfo: Omit<BasicInfo, "files">;
  connectionInfo: ConnectionInfo;
  loadMode: LoadMode;
  showTruncateSql: boolean;
  truncateSql: string;
  loadSql: string;
  queryRecordSql: string;
  sourceColumns: SourceColumnMetadata[];
  loadSqlColumns: SourceColumnMetadata[];
  httpSourceJson: string;
  httpTestPassed: boolean;
  httpTestType?: "JSON_API" | "FILE_DOWNLOAD";
  targetColumns: ColumnMetadataResponse[];
  columnMappings: ColumnMapping[];
}

const ETL_CREATE_DRAFT_STORAGE_KEY = "cerebroetl.etlCreateDraft.v1";

const DEFAULT_BASIC_INFO: BasicInfo = {
  jobName: "",
  parentGroupId: "",
  parentGroupPath: "",
  comments: "",
  fileExtension: "csv",
  files: [],
};

const DEFAULT_CONNECTION_INFO: ConnectionInfo = {
  httpMethod: "GET",
  apiUrl: "",
  apiKey: "",
  sourceServiceId: "",
  sourceConnectionId: null,
  sourceDatabaseType: "PostgreSQL",
  sourceSchema: "",
  sourceTable: "",
  targetServiceId: "",
  targetDatabaseType: "PostgreSQL",
  targetSchema: "",
  targetTable: "",
};

const HTTP_SAMPLE_JSON = `{
  "latitude": 37.0,
  "longitude": 127.9,
  "timezone": "Asia/Seoul",
  "current": {
    "time": "2026-09-25T15:00",
    "temperature_2m": 22.3,
    "relative_humidity_2m": 61,
    "wind_speed_10m": 3.4
  }
}`;

const HTTP_SOURCE_TABLE = "JSON";
const HTTP_SOURCE_COLUMNS: SourceColumnMetadata[] = [
  "latitude",
  "longitude",
  "timezone",
  "time",
  "temperature_2m",
  "relative_humidity_2m",
  "wind_speed_10m",
].map((name) => ({
  table: HTTP_SOURCE_TABLE,
  name,
  dataType: "",
  primaryKey: false,
  maskable: false,
}));

const HTTP_SELECTED_TABLES = [HTTP_SOURCE_TABLE];

function readEtlCreateDraft(): EtlCreateDraft | null {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    const rawValue = window.sessionStorage.getItem(ETL_CREATE_DRAFT_STORAGE_KEY);
    return rawValue ? JSON.parse(rawValue) as EtlCreateDraft : null;
  } catch {
    return null;
  }
}

function writeEtlCreateDraft(draft: EtlCreateDraft) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.sessionStorage.setItem(ETL_CREATE_DRAFT_STORAGE_KEY, JSON.stringify(draft));
  } catch {
    // Draft persistence is best-effort; form editing must keep working if storage is blocked.
  }
}

function restoredEtlType(restoredDraft: EtlCreateDraft | null): EtlType {
  return TYPE_OPTIONS.some((option) => option.value === restoredDraft?.etlType)
    ? restoredDraft!.etlType
    : "DB_TO_DB";
}

function clearEtlCreateDraft() {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.sessionStorage.removeItem(ETL_CREATE_DRAFT_STORAGE_KEY);
  } catch {
    // Ignore storage failures.
  }
}

function flattenProcessGroups(
  node: NifiProcessGroupTreeNode,
  path: string[] = [],
): ProcessGroupOption[] {
  const nextPath = [...path, node.name];
  return [
    { id: node.id, label: nextPath.join(" / ") },
    ...node.children.flatMap((child) => flattenProcessGroups(child, nextPath)),
  ];
}

function processGroupTreeIncludes(node: NifiProcessGroupTreeNode, processGroupId: string): boolean {
  return node.id === processGroupId || node.children.some((child) => processGroupTreeIncludes(child, processGroupId));
}

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function toControllerServiceOption(service: NifiControllerServiceEntity): ControllerServiceOption | null {
  const id = service.component?.id ?? service.id;
  const name = service.component?.name;
  const type = service.component?.type;
  if (!id || !name || !type) {
    return null;
  }
  return {
    id,
    name,
    displayName: stripCdcControllerServicePrefix(name),
    type,
  };
}

function normalizeName(value: string) {
  return value.trim().toLowerCase();
}

function normalizeComparableName(value: string) {
  const normalized = normalizeName(value).replace(/[^a-z0-9가-힣]/g, "");
  const aliases: Record<string, string> = {
    lat: "latitude",
    lon: "longitude",
    lng: "longitude",
    temperature: "temperature2m",
    temp: "temperature2m",
    humidity: "relativehumidity2m",
    windspeed: "windspeed10m",
    wind: "windspeed10m",
  };
  return aliases[normalized] ?? normalized;
}

function stripCdcControllerServicePrefix(value: string) {
  return value.trim().replace(/^cdc-\d+-/i, "");
}

function normalizeControllerServiceName(value: string) {
  return normalizeName(stripCdcControllerServicePrefix(value));
}

function isDbcpConnectionPool(service: ControllerServiceOption) {
  return service.type === "DBCPConnectionPool" || service.type.endsWith(".DBCPConnectionPool");
}

function nifiDatabaseTypeFromConnection(connection: ConnectionResponse | null) {
  return connection ? CONNECTION_TO_NIFI_DATABASE_TYPE[connection.dbType] : null;
}

function matchesSelectOption(input: string, option?: { label?: ReactNode }) {
  return String(option?.label ?? "").toLowerCase().includes(input.toLowerCase());
}

function commaSeparatedValues(value: string) {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function tableAlias(index: number) {
  if (index >= 0 && index < 26) {
    return String.fromCharCode(97 + index);
  }
  return `t${index + 1}`;
}

function sourceColumnKey(table: string, column: string) {
  return `${table}::${column}`;
}

function sourceColumnFromLoadSqlAlias(name: string): SourceColumnMetadata {
  return {
    table: LOAD_SQL_SOURCE_TABLE,
    name,
    dataType: "",
    primaryKey: false,
    maskable: false,
  };
}

function splitSqlSelectItems(value: string) {
  const items: string[] = [];
  let current = "";
  let quote: "'" | "\"" | "`" | null = null;
  let depth = 0;
  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];
    const nextChar = value[index + 1];
    if (quote) {
      current += char;
      if (char === quote) {
        if (quote === "'" && nextChar === "'") {
          current += nextChar;
          index += 1;
        } else {
          quote = null;
        }
      }
      continue;
    }
    if (char === "'" || char === "\"" || char === "`") {
      quote = char;
      current += char;
      continue;
    }
    if (char === "(") {
      depth += 1;
      current += char;
      continue;
    }
    if (char === ")") {
      depth = Math.max(0, depth - 1);
      current += char;
      continue;
    }
    if (char === "," && depth === 0) {
      items.push(current.trim());
      current = "";
      continue;
    }
    current += char;
  }
  if (current.trim()) {
    items.push(current.trim());
  }
  return items;
}

function unquoteSqlIdentifier(value: string) {
  const trimmed = value.trim();
  if (
    (trimmed.startsWith("\"") && trimmed.endsWith("\"")) ||
    (trimmed.startsWith("`") && trimmed.endsWith("`")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"))
  ) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function extractLoadSqlAliasTableMap(loadSql: string) {
  const fromMatch = /\bfrom\b([\s\S]*?)(?:\bwhere\b|\bgroup\s+by\b|\border\s+by\b|\bhaving\b|$)/i.exec(loadSql);
  if (!fromMatch) {
    return new Map<string, string>();
  }
  return splitSqlSelectItems(fromMatch[1]).reduce((aliasMap, item) => {
    const normalizedItem = item.trim().replace(/\s+/g, " ");
    const match = /^([^\s,]+)(?:\s+(?:as\s+)?([a-zA-Z_][\w$]*))?$/i.exec(normalizedItem);
    if (match?.[2]) {
      aliasMap.set(match[2], match[1]);
    }
    return aliasMap;
  }, new Map<string, string>());
}

function extractLoadSqlAliasColumnTableMap(loadSql: string) {
  const selectMatch = loadSql.match(/\bselect\b([\s\S]*?)\bfrom\b/i);
  if (!selectMatch) {
    return new Map<string, string>();
  }
  const tableByAlias = extractLoadSqlAliasTableMap(loadSql);
  return splitSqlSelectItems(selectMatch[1]).reduce((columnTableMap, item) => {
    const columnAlias = loadSqlProjectionAlias(item);
    const tableAliasMatch = /\b([a-zA-Z_][\w$]*)\s*\.\s*("[^"]+"|`[^`]+`|\[[^\]]+\]|[a-zA-Z_][\w$]*)/.exec(item);
    const tableName = tableAliasMatch ? tableByAlias.get(tableAliasMatch[1]) : null;
    if (columnAlias && tableName) {
      columnTableMap.set(normalizeName(columnAlias), tableName);
    }
    return columnTableMap;
  }, new Map<string, string>());
}

function extractLoadSqlAliasColumns(loadSql: string) {
  const selectMatch = loadSql.match(/\bselect\b([\s\S]*?)\bfrom\b/i);
  if (!selectMatch) {
    return [];
  }
  return splitSqlSelectItems(selectMatch[1])
    .map(loadSqlProjectionAlias)
    .filter((alias, index, aliases) => !!alias && aliases.findIndex((item) =>
      normalizeName(item) === normalizeName(alias),
    ) === index)
    .map(sourceColumnFromLoadSqlAlias);
}

function loadSqlProjectionAlias(item: string) {
  const aliasMatch = item.match(/\bas\s+("[^"]+"|`[^`]+`|\[[^\]]+\]|[a-zA-Z_][\w$]*)\s*$/i);
  if (aliasMatch) {
    return unquoteSqlIdentifier(aliasMatch[1]);
  }
  const columnMatch = item.trim().match(/^(?:[a-zA-Z_][\w$]*\s*\.\s*)?("[^"]+"|`[^`]+`|\[[^\]]+\]|[a-zA-Z_][\w$]*)$/);
  return columnMatch ? unquoteSqlIdentifier(columnMatch[1]) : "";
}

function removeLoadSqlAliasColumn(loadSql: string, columnName: string) {
  const selectMatch = /\bselect\b([\s\S]*?)\bfrom\b/i.exec(loadSql);
  const fromMatch = /\bfrom\b/i.exec(loadSql);
  if (!selectMatch || !fromMatch || selectMatch.index === undefined || fromMatch.index === undefined) {
    return loadSql;
  }
  const items = splitSqlSelectItems(selectMatch[1]);
  const nextItems = items.filter((item) =>
    normalizeName(loadSqlProjectionAlias(item)) !== normalizeName(columnName),
  );
  if (nextItems.length === items.length) {
    return loadSql;
  }
  const beforeSelect = loadSql.slice(0, selectMatch.index);
  const afterFrom = loadSql.slice(fromMatch.index);
  const projectionSql = nextItems.map((item, index) => `${index === 0 ? "" : ","}${item}`).join("\n");
  return `${beforeSelect}select \n${projectionSql}\n${afterFrom}`;
}

function buildDefaultColumnMappings(
  targetColumns: ColumnMetadataResponse[],
  sourceColumns: SourceColumnMetadata[],
  previousMappings: ColumnMapping[],
) {
  return targetColumns.map((targetColumn) => {
    const previous = previousMappings.find((mapping) => mapping.targetColumn === targetColumn.name);
    const previousSourceExists = previous && sourceColumns.some((sourceColumn) =>
      sourceColumn.table === previous.sourceTable && sourceColumn.name === previous.sourceColumn,
    );
    const targetName = normalizeComparableName(targetColumn.name);
    const exactMatchedSource = sourceColumns.find((sourceColumn) =>
      normalizeComparableName(sourceColumn.name) === targetName,
    );
    const fuzzyMatchedSource = sourceColumns.find((sourceColumn) => {
      const sourceName = normalizeComparableName(sourceColumn.name);
      return sourceName.includes(targetName) || targetName.includes(sourceName);
    });
    const matchedSource = exactMatchedSource ?? fuzzyMatchedSource;
    const sourceTable = previousSourceExists ? previous.sourceTable : matchedSource?.table || "";
    const sourceColumn = previousSourceExists ? previous.sourceColumn : matchedSource?.name || "";
    return {
      id: previous?.id ?? targetColumn.name,
      targetColumn: targetColumn.name,
      sourceTable,
      sourceColumn,
      logic: previous?.logic ?? "",
    };
  });
}

function buildMappedLoadSql(
  connectionInfo: ConnectionInfo,
  sourceColumns: SourceColumnMetadata[],
  targetColumns: ColumnMetadataResponse[],
  loadMode: LoadMode,
) {
  const selectedTables = commaSeparatedValues(connectionInfo.sourceTable);
  if (selectedTables.length === 0) {
    return defaultLoadSql(
      loadMode,
      connectionInfo.sourceSchema,
      connectionInfo.sourceTable,
      connectionInfo.sourceDatabaseType,
    );
  }

  const aliasByTable = new Map(selectedTables.map((table, index) => [table, tableAlias(index)]));
  const selectedSourceColumns = sourceColumns.filter((sourceColumn) =>
    aliasByTable.has(sourceColumn.table),
  );
  const uniqueSourceColumns = selectedSourceColumns.filter((sourceColumn, index, allColumns) =>
    allColumns.findIndex((item) =>
      item.table === sourceColumn.table && item.name === sourceColumn.name,
    ) === index,
  );
  const targetColumnByName = new Map(targetColumns.map((targetColumn) => [
    normalizeComparableName(targetColumn.name),
    targetColumn.name,
  ]));
  const projections = uniqueSourceColumns.map((sourceColumn, index) => {
    const alias = aliasByTable.get(sourceColumn.table);
    const expression = alias ? `${alias}.${sourceColumn.name}` : sourceColumn.name;
    const targetColumn = targetColumnByName.get(normalizeComparableName(sourceColumn.name)) ?? sourceColumn.name;
    const prefix = index === 0 ? "" : ",";
    return `${prefix}${expression} as ${targetColumn}`;
  });
  const schemaPrefix = connectionInfo.sourceSchema.trim() ? `${connectionInfo.sourceSchema.trim()}.` : "";
  const fromClause = selectedTables
    .map((table, index) => `${schemaPrefix}${table} ${tableAlias(index)}`)
    .join(", ");

  const projectionSql = projections.length > 0 ? projections.join("\n") : "*";
  const baseSql = `select \n${projectionSql}\nfrom ${fromClause}`;
  if (loadMode !== "UPSERT") {
    return baseSql;
  }
  return `${baseSql}\nwhere {update컬럼} ${defaultUpdateWhereExpression(connectionInfo.sourceDatabaseType)}`;
}

function stripSourceAliases(value: string, selectedTables: string[]) {
  return selectedTables.reduce((currentValue, _table, index) => {
    const alias = tableAlias(index);
    return currentValue.replace(new RegExp(`\\b${alias}\\.`, "gi"), "");
  }, value);
}

function buildQueryRecordSql(
  mappings: ColumnMapping[],
  selectedTables: string[],
) {
  const projections = mappings.map((mapping, index) => {
    const expression = stripSourceAliases(
      mapping.logic.trim() || mapping.sourceColumn || "NULL",
      selectedTables,
    );
    const prefix = index === 0 ? "" : ",";
    return `${prefix}${expression} as ${mapping.targetColumn}`;
  });
  return `SELECT\n${projections.join("\n")}\nFROM FLOWFILE`;
}

function formatLoadSqlIndent(sql: string) {
  const lines = sql.replace(/\r\n?/g, "\n").split("\n");
  let inSelectList = false;
  return lines.map((line) => {
    const trimmed = line.trim();
    if (!trimmed) {
      return "";
    }
    if (/^select\b/i.test(trimmed)) {
      inSelectList = true;
      return trimmed;
    }
    if (/^from\b/i.test(trimmed)) {
      inSelectList = false;
      return trimmed;
    }
    if (/^(where|group\s+by|order\s+by|having|union)\b/i.test(trimmed)) {
      inSelectList = false;
      return trimmed;
    }
    if (inSelectList || /^(,|and\b|or\b)/i.test(trimmed)) {
      return `    ${trimmed}`;
    }
    return trimmed;
  }).join("\n");
}

function renderSqlText(sql: string) {
  if (!sql) {
    return "\n";
  }
  return sql.split(/(#\{[^}\n]+\})/g).map((part, index) =>
    /^#\{[^}\n]+\}$/.test(part) ? (
      <span key={index} className="etl-sql-token">
        {part}
      </span>
    ) : (
      part
    ),
  );
}

function formatJsonPreview(value: string) {
  if (!value.trim()) {
    return "";
  }
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

function displayMappingExpression(mapping: ColumnMapping, selectedTables: string[]) {
  return stripSourceAliases(mapping.logic.trim() || mapping.sourceColumn || "", selectedTables);
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function sourceColumnReferencePattern(columnName: string, flags = "i") {
  return new RegExp(`(?:\\b[a-zA-Z_][\\w$]*\\s*\\.\\s*)?\\b${escapeRegExp(columnName)}\\b`, flags);
}

type ValidationSeverity = "error" | "warning";

interface MappingValidationIssue {
  severity: ValidationSeverity;
  message: string;
}

interface MappingValidationDialog {
  errorCount: number;
  warningCount: number;
  calciteMessage: string;
  issues: MappingValidationIssue[];
}

const SQL_IDENTIFIER_IGNORE_WORDS = new Set([
  "and",
  "as",
  "between",
  "case",
  "cast",
  "coalesce",
  "current_date",
  "current_timestamp",
  "date",
  "else",
  "end",
  "false",
  "from",
  "in",
  "is",
  "like",
  "not",
  "null",
  "nvl",
  "or",
  "select",
  "then",
  "to_char",
  "to_date",
  "true",
  "when",
]);

function normalizeDataType(value: string) {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function dataTypeCategory(value: string) {
  const type = normalizeDataType(value);
  if (!type) {
    return "unknown";
  }
  if (/(char|clob|text|string|varchar|uuid|json|xml)/.test(type)) {
    return "text";
  }
  if (/(number|numeric|decimal|int|integer|bigint|smallint|float|double|real|serial)/.test(type)) {
    return "number";
  }
  if (/(date|time|timestamp)/.test(type)) {
    return "datetime";
  }
  if (/(bool|bit)/.test(type)) {
    return "boolean";
  }
  if (/(blob|binary|bytea|raw)/.test(type)) {
    return "binary";
  }
  return "unknown";
}

function isSimpleCompatibleType(sourceType: string, targetType: string) {
  const sourceCategory = dataTypeCategory(sourceType);
  const targetCategory = dataTypeCategory(targetType);
  if (sourceCategory === "unknown" || targetCategory === "unknown") {
    return true;
  }
  if (sourceCategory === targetCategory) {
    return true;
  }
  return targetCategory === "text";
}

function stripSqlNoise(value: string) {
  return value
    .replace(/'([^']|'')*'/g, " ")
    .replace(/"[^"]*"/g, " ")
    .replace(/`[^`]*`/g, " ")
    .replace(/#\{[^}\n]+\}/g, " ")
    .replace(/\b\d+(?:\.\d+)?\b/g, " ");
}

function extractQualifiedReferences(expression: string) {
  const cleanExpression = stripSqlNoise(expression);
  const refs: Array<{ alias: string; column: string }> = [];
  const pattern = /\b([a-zA-Z_][\w$]*)\s*\.\s*([a-zA-Z_][\w$]*)\b/g;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(cleanExpression)) !== null) {
    refs.push({ alias: match[1], column: match[2] });
  }
  return refs;
}

function extractUnqualifiedReferences(expression: string) {
  let cleanExpression = stripSqlNoise(expression);
  cleanExpression = cleanExpression.replace(/\b[a-zA-Z_][\w$]*\s*\.\s*[a-zA-Z_][\w$]*\b/g, " ");
  cleanExpression = cleanExpression.replace(/\b[a-zA-Z_][\w$]*\s*\(/g, " ");
  const refs = new Set<string>();
  const pattern = /\b[a-zA-Z_][\w$]*\b/g;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(cleanExpression)) !== null) {
    const normalized = normalizeName(match[0]);
    if (!SQL_IDENTIFIER_IGNORE_WORDS.has(normalized)) {
      refs.add(match[0]);
    }
  }
  return [...refs];
}

function sourceColumnLabel(sourceColumn: SourceColumnMetadata) {
  return sourceColumn.table ? `${sourceColumn.table}.${sourceColumn.name}` : sourceColumn.name;
}

function buildMappingValidationInterpretation(result: MappingValidationDialog) {
  const lines: string[] = [];
  if (result.errorCount > 0) {
    lines.push(`매핑을 저장하기 전에 오류 ${result.errorCount}건을 먼저 수정해야 합니다.`);
  } else if (result.warningCount > 0) {
    lines.push(`문법 오류는 없지만 적재 전에 확인할 경고 ${result.warningCount}건이 있습니다.`);
  }
  if (!result.calciteMessage.startsWith("Calcite 문법 검사: 성공")) {
    lines.push("처리 로직 SQL이 Calcite 문법으로 해석되지 않습니다. 괄호, 함수명, AS 별칭, 쉼표 위치를 확인하세요.");
  }
  const missingMapping = result.issues.some((issue) => issue.message.includes("매핑이 없습니다") || issue.message.includes("비어 있습니다"));
  if (missingMapping) {
    lines.push("타깃 컬럼 중 소스 컬럼이나 처리 로직이 연결되지 않은 항목이 있습니다.");
  }
  const missingSource = result.issues.some((issue) => issue.message.includes("소스 컬럼") && issue.message.includes("찾을 수 없습니다"));
  if (missingSource) {
    lines.push("처리 로직이 현재 소스 목록에 없는 컬럼명을 참조하고 있습니다. 삭제된 컬럼이나 alias를 확인하세요.");
  }
  const duplicateTarget = result.issues.some((issue) => issue.message.includes("중복 매핑"));
  if (duplicateTarget) {
    lines.push("동일한 타깃 컬럼으로 여러 매핑이 잡혀 있습니다. 하나의 타깃 컬럼에는 하나의 처리 로직만 남겨 주세요.");
  }
  const typeWarning = result.issues.some((issue) => issue.message.includes("타입"));
  if (typeWarning) {
    lines.push("소스 타입과 타깃 타입이 다를 수 있습니다. 필요하면 CAST/TO_CHAR/TO_DATE 같은 명시 변환을 추가하세요.");
  }
  return lines.length > 0 ? lines : ["아래 에러 내용을 확인해 매핑 조건을 조정하세요."];
}

function validateMappingConsistency(
  columnMappings: ColumnMapping[],
  sourceColumns: SourceColumnMetadata[],
  targetColumns: ColumnMetadataResponse[],
  selectedSourceTables: string[],
) {
  const issues: MappingValidationIssue[] = [];
  const sourceColumnsByName = new Map<string, SourceColumnMetadata[]>();
  sourceColumns.forEach((sourceColumn) => {
    const key = normalizeName(sourceColumn.name);
    sourceColumnsByName.set(key, [...(sourceColumnsByName.get(key) ?? []), sourceColumn]);
  });
  const sourceColumnByTableAndName = new Map(sourceColumns.map((sourceColumn) => [
    sourceColumnKey(normalizeName(sourceColumn.table), normalizeName(sourceColumn.name)),
    sourceColumn,
  ]));
  const aliasToTable = new Map(selectedSourceTables.map((table, index) => [tableAlias(index), table]));
  const targetColumnByName = new Map(targetColumns.map((targetColumn) => [
    normalizeName(targetColumn.name),
    targetColumn,
  ]));
  const mappingsByTarget = new Map<string, ColumnMapping[]>();
  columnMappings.forEach((mapping) => {
    const key = normalizeName(mapping.targetColumn);
    mappingsByTarget.set(key, [...(mappingsByTarget.get(key) ?? []), mapping]);
  });

  targetColumns.forEach((targetColumn) => {
    const mappings = mappingsByTarget.get(normalizeName(targetColumn.name)) ?? [];
    if (mappings.length === 0) {
      issues.push({ severity: "error", message: `타깃 컬럼 ${targetColumn.name}의 매핑이 없습니다.` });
    }
    if (mappings.length > 1) {
      issues.push({ severity: "error", message: `타깃 컬럼 ${targetColumn.name}이 ${mappings.length}번 중복 매핑되었습니다.` });
    }
  });
  columnMappings.forEach((mapping) => {
    const targetColumn = targetColumnByName.get(normalizeName(mapping.targetColumn));
    const expression = mapping.logic.trim() || mapping.sourceColumn.trim();
    if (!targetColumn) {
      issues.push({ severity: "error", message: `타깃 컬럼 ${mapping.targetColumn}은 현재 타깃 테이블에 없습니다.` });
      return;
    }
    if (!expression) {
      issues.push({ severity: "error", message: `타깃 컬럼 ${mapping.targetColumn}의 처리 로직이 비어 있습니다.` });
      return;
    }

    extractQualifiedReferences(expression).forEach((ref) => {
      const tableName = aliasToTable.get(ref.alias);
      if (!tableName) {
        issues.push({ severity: "error", message: `${mapping.targetColumn}: 소스 alias ${ref.alias}를 찾을 수 없습니다.` });
        return;
      }
      const sourceColumn = sourceColumnByTableAndName.get(sourceColumnKey(normalizeName(tableName), normalizeName(ref.column)));
      if (!sourceColumn) {
        issues.push({ severity: "error", message: `${mapping.targetColumn}: 소스 컬럼 ${ref.alias}.${ref.column}을 찾을 수 없습니다.` });
      }
    });
    extractUnqualifiedReferences(expression).forEach((columnName) => {
      const matches = sourceColumnsByName.get(normalizeName(columnName)) ?? [];
      if (matches.length === 0) {
        issues.push({ severity: "error", message: `${mapping.targetColumn}: 소스 컬럼 ${columnName}을 찾을 수 없습니다.` });
      } else if (matches.length > 1) {
        issues.push({
          severity: "warning",
          message: `${mapping.targetColumn}: 소스 컬럼 ${columnName}이 여러 테이블에 있습니다. ${matches.map(sourceColumnLabel).join(", ")}`,
        });
      }
    });

    const referencedColumns = mappingReferencedSourceColumns(mapping, sourceColumns, selectedSourceTables);
    if (referencedColumns.length === 0 && extractUnqualifiedReferences(expression).length > 0) {
      issues.push({ severity: "warning", message: `${mapping.targetColumn}: 처리 로직에서 확인 가능한 소스 컬럼 참조가 없습니다.` });
    }
    if (referencedColumns.length === 1 && targetColumn.dataType) {
      const sourceColumn = referencedColumns[0];
      if (sourceColumn.dataType && !isSimpleCompatibleType(sourceColumn.dataType, targetColumn.dataType)) {
        issues.push({
          severity: "warning",
          message: `${mapping.targetColumn}: ${sourceColumnLabel(sourceColumn)}(${sourceColumn.dataType}) -> ${targetColumn.name}(${targetColumn.dataType}) 타입 변환 확인이 필요합니다.`,
        });
      }
    } else if (referencedColumns.length > 1 && targetColumn.dataType && dataTypeCategory(targetColumn.dataType) !== "text") {
      issues.push({
        severity: "warning",
        message: `${mapping.targetColumn}: 여러 소스 컬럼/함수식 결과를 ${targetColumn.dataType}에 적재합니다. 타입 변환 확인이 필요합니다.`,
      });
    }
  });

  return issues;
}

function findClosingParen(value: string, openIndex: number) {
  let depth = 0;
  let quote: "'" | "\"" | "`" | null = null;
  for (let index = openIndex; index < value.length; index += 1) {
    const char = value[index];
    const nextChar = value[index + 1];
    if (quote) {
      if (char === quote) {
        if (quote === "'" && nextChar === "'") {
          index += 1;
        } else {
          quote = null;
        }
      }
      continue;
    }
    if (char === "'" || char === "\"" || char === "`") {
      quote = char;
      continue;
    }
    if (char === "(") {
      depth += 1;
    } else if (char === ")") {
      depth -= 1;
      if (depth === 0) {
        return index;
      }
    }
  }
  return -1;
}

function removeSourceColumnFromLogic(logic: string, columnName: string) {
  const sourcePattern = sourceColumnReferencePattern(columnName);
  const removalRanges: Array<[number, number]> = [];
  const functionPattern = /\b[a-zA-Z_][\w$]*\s*\(/g;
  let functionMatch: RegExpExecArray | null;
  while ((functionMatch = functionPattern.exec(logic)) !== null) {
    const openIndex = logic.indexOf("(", functionMatch.index);
    const closeIndex = findClosingParen(logic, openIndex);
    if (closeIndex < 0) {
      break;
    }
    const callText = logic.slice(functionMatch.index, closeIndex + 1);
    if (sourcePattern.test(callText)) {
      removalRanges.push([functionMatch.index, closeIndex + 1]);
    }
    functionPattern.lastIndex = closeIndex + 1;
  }

  let nextLogic = logic;
  for (const [start, end] of removalRanges.reverse()) {
    nextLogic = `${nextLogic.slice(0, start)}${nextLogic.slice(end)}`;
  }
  nextLogic = nextLogic
    .replace(sourceColumnReferencePattern(columnName, "gi"), "")
    .replace(/\s*(\|\||[+\-*/])\s*(?=(\)|,|$))/g, "")
    .replace(/(^|[(,])\s*(\|\||[+\-*/])\s*/g, "$1")
    .replace(/\s*,\s*,\s*/g, ", ")
    .replace(/\(\s*,\s*/g, "(")
    .replace(/\s*,\s*\)/g, ")")
    .replace(/\(\s*\)/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();
  return nextLogic;
}

function appendSourceColumnToMapping(mapping: ColumnMapping, sourceColumn: SourceColumnMetadata) {
  if (mappingReferencedSourceColumns(mapping, [sourceColumn], []).length > 0) {
    return mapping;
  }
  const sourceExpression = sourceColumn.name;
  const logic = mapping.logic.trim();
  if (!logic && !mapping.sourceColumn) {
    return {
      ...mapping,
      sourceTable: sourceColumn.table,
      sourceColumn: sourceColumn.name,
      logic: sourceExpression,
    };
  }
  if (!logic) {
    return {
      ...mapping,
      logic: `${mapping.sourceColumn} || ${sourceExpression}`,
    };
  }
  return {
    ...mapping,
    logic: `${logic} || ${sourceExpression}`,
  };
}

function mappingReferencedSourceColumns(
  mapping: ColumnMapping,
  sourceColumns: SourceColumnMetadata[],
  selectedTables: string[],
) {
  const logic = mapping.logic.trim();
  if (!logic) {
    return sourceColumns.filter((sourceColumn) =>
      sourceColumn.table === mapping.sourceTable && sourceColumn.name === mapping.sourceColumn,
    );
  }
  const unqualifiedExpression = selectedTables.reduce((currentValue, _table, index) => {
    const alias = tableAlias(index);
    return currentValue.replace(new RegExp(`\\b${alias}\\.[a-z0-9_]+\\b`, "gi"), " ");
  }, logic);
  return sourceColumns.filter((sourceColumn) => {
    const tableIndex = selectedTables.indexOf(sourceColumn.table);
    const alias = tableIndex >= 0 ? tableAlias(tableIndex) : "";
    const qualifiedPattern = alias
      ? new RegExp(`\\b${alias}\\.${escapeRegExp(sourceColumn.name)}\\b`, "i")
      : null;
    if (qualifiedPattern?.test(logic)) {
      return true;
    }
    const unqualifiedPattern = sourceColumnReferencePattern(sourceColumn.name);
    return unqualifiedPattern.test(unqualifiedExpression);
  });
}

function defaultLoadSql(
  loadMode: LoadMode,
  sourceSchema: string,
  sourceTable: string,
  databaseType: NifiDatabaseType,
) {
  const schema = sourceSchema.trim() || "소스스키마";
  const tables = commaSeparatedValues(sourceTable);
  const fromClause = tables.length > 0
    ? tables.map((table) => `${schema}.${table}`).join(",\n     ")
    : `${schema}.소스테이블`;
  const baseSql = `SELECT *\nFROM ${fromClause}`;
  if (loadMode !== "UPSERT") {
    return baseSql;
  }

  return `${baseSql}\nWHERE [UPDATE기준컬럼] ${defaultUpdateWhereExpression(databaseType)}`;
}

function defaultUpdateWhereExpression(databaseType: NifiDatabaseType) {
  if (databaseType === "MySQL") {
    return "BETWEEN CONCAT(DATE_FORMAT(CURDATE() - INTERVAL #{Day} DAY, '%Y%m%d'), '000000')\n"
      + "      AND CONCAT(DATE_FORMAT(CURDATE() - INTERVAL #{Day} DAY, '%Y%m%d'), '235959')";
  }
  if (databaseType === "Oracle 12+") {
    return "BETWEEN TO_CHAR(SYSDATE - #{Day}, 'YYYYMMDD') || '000000'\n"
      + "      AND TO_CHAR(SYSDATE - #{Day}, 'YYYYMMDD') || '235959'";
  }
  return "BETWEEN TO_CHAR(CURRENT_DATE - #{Day}, 'YYYYMMDD') || '000000'\n"
    + "      AND TO_CHAR(CURRENT_DATE - #{Day}, 'YYYYMMDD') || '235959'";
}

function defaultTruncateSql(targetSchema: string, targetTable: string) {
  return `TRUNCATE TABLE ${targetSchema.trim()}.${targetTable.trim()}`;
}

function stepState(step: WizardStepId, activeStep: WizardStepId, completedSteps: WizardStepId[]) {
  if (step === activeStep) {
    return "active";
  }
  if (completedSteps.includes(step)) {
    return "completed";
  }
  return "pending";
}

function selectedTypeLabel(type: EtlType) {
  return TYPE_OPTIONS.find((option) => option.value === type)?.title ?? "";
}

function stepSummary(step: WizardStepId, options: {
  etlType: EtlType;
  basicInfo: BasicInfo;
  connectionInfo: ConnectionInfo;
  loadMode: LoadMode;
}) {
  if (step === "type") {
    return `[선택됨: ${selectedTypeLabel(options.etlType)}]`;
  }
  if (step === "basic" && options.basicInfo.jobName.trim()) {
    return `[${options.basicInfo.jobName.trim()}]`;
  }
  if (step === "connection" && options.connectionInfo.targetDatabaseType) {
    return `[타깃: ${options.connectionInfo.targetDatabaseType}]`;
  }
  if (step === "query") {
    return `[${options.loadMode}]`;
  }
  return "";
}

function CreateStepper({
  activeStep,
  completedSteps,
  steps = WIZARD_STEPS,
  onStepClick,
}: {
  activeStep: WizardStepId;
  completedSteps: WizardStepId[];
  steps?: ReadonlyArray<(typeof WIZARD_STEPS)[number]>;
  onStepClick: (step: WizardStepId) => void;
}) {
  const activeIndex = steps.findIndex((item) => item.id === activeStep);
  const progressWidth = `${(Math.max(0, activeIndex) / Math.max(1, steps.length - 1)) * 100}%`;

  return (
    <div className="etl-stepper-wrap">
      <div className="etl-stepper">
        <div className="etl-stepper-line" />
        <div className="etl-stepper-progress" style={{ width: progressWidth }} />
        {steps.map((step) => {
          const state = stepState(step.id, activeStep, completedSteps);
          return (
            <button
              key={step.id}
              type="button"
              className={`stepper-item ${state === "active" ? "active" : state === "completed" ? "completed" : ""}`}
              onClick={() => onStepClick(step.id)}
            >
              <span className="stepper-circle">{STEP_NUMBER[step.id]}</span>
              <span className="stepper-label">{step.label}</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function WizardStepCard({
  step,
  title,
  activeStep,
  completedSteps,
  summary,
  onOpen,
  children,
}: {
  step: WizardStepId;
  title: string;
  activeStep: WizardStepId;
  completedSteps: WizardStepId[];
  summary?: string;
  onOpen: (step: WizardStepId) => void;
  children: ReactNode;
}) {
  const state = stepState(step, activeStep, completedSteps);
  const active = state === "active";
  const completed = state === "completed";

  return (
    <section
      className={`wizard-step-card${active ? " active open" : ""}${completed ? " completed" : ""}`}
      data-step={STEP_NUMBER[step]}
    >
      <button type="button" className="wizard-step-header" onClick={() => onOpen(step)}>
        <span className="header-left">
          <span className="step-number-badge">{STEP_NUMBER[step]}</span>
          <span className="step-title">{title}</span>
          {summary ? <span className="step-selected-summary">{summary}</span> : null}
        </span>
        <span className="header-right">
          <span className={`step-status-tag${active ? " active" : completed ? " done" : ""}`}>
            {active ? "작성 중" : completed ? "완료" : "대기"}
          </span>
          <span className="header-chevron" aria-hidden="true">
            <DownOutlined />
          </span>
        </span>
      </button>
      <div className="wizard-step-body">{children}</div>
    </section>
  );
}

function TypeStep({
  selectedType,
  onTypeChange,
  onComplete,
}: {
  selectedType: EtlType;
  onTypeChange: (type: EtlType) => void;
  onComplete: () => void;
}) {
  return (
    <>
      <p className="step-guide-text">수집 및 적재할 데이터 소스와 대상 환경에 최적화된 ETL 전송 유형을 선택해 주세요.</p>
      <div className="type-card-grid">
        {TYPE_OPTIONS.map((option) => (
          <button
            key={option.value}
            type="button"
            disabled={option.disabled}
            className={`type-card${selectedType === option.value ? " selected" : ""}${option.disabled ? " disabled" : ""}`}
            onClick={() => onTypeChange(option.value)}
          >
            <span className="type-card-top">
              <span className="type-icon-box" aria-hidden="true">
                {option.value === "DB_TO_DB" ? (
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                    <ellipse cx="12" cy="5" rx="9" ry="3" />
                    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
                    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
                  </svg>
                ) : option.value === "FILE_LOAD" ? (
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                    <polyline points="14 2 14 8 20 8" />
                    <line x1="16" y1="13" x2="8" y2="13" />
                    <line x1="16" y1="17" x2="8" y2="17" />
                    <polyline points="10 9 9 9 8 9" />
                  </svg>
                ) : option.value === "HTTP" ? (
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                    <path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z" />
                  </svg>
                ) : option.value === "UNSTRUCTURED" ? (
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                    <rect x="3" y="3" width="18" height="18" rx="2" ry="2" />
                    <circle cx="8.5" cy="8.5" r="1.5" />
                    <polyline points="21 15 16 10 5 21" />
                  </svg>
                ) : (
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                    <polyline points="16 18 22 12 16 6" />
                    <polyline points="8 6 2 12 8 18" />
                  </svg>
                )}
              </span>
              <span className="type-check-badge" aria-hidden="true">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                  <polyline points="20 6 9 17 4 12" />
                </svg>
              </span>
            </span>
            <span className="type-card-body">
              <strong>{option.title}</strong>
              <span className="type-subtext">{option.description}</span>
            </span>
          </button>
        ))}
      </div>
      <div className="step-actions-bar">
        <div className="actions-left" />
        <div className="actions-right">
          <button type="button" className="btn-step-next" onClick={onComplete}>
            다음
          </button>
        </div>
      </div>
    </>
  );
}

function BasicStep({
  value,
  etlType,
  onChange,
  onComplete,
}: {
  value: BasicInfo;
  etlType: EtlType;
  onChange: (nextValue: Partial<BasicInfo>) => void;
  onComplete: () => void;
}) {
  const [groupTree, setGroupTree] = useState<NifiProcessGroupTreeNode | null>(null);
  const [groupError, setGroupError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    getNifiProcessGroupTree()
      .then((tree) => {
        if (!cancelled) {
          setGroupTree(tree);
          setGroupError(null);
        }
      })
      .catch((ex) => {
        if (!cancelled) {
          setGroupError(ex instanceof Error ? ex.message : "Processor Group을 불러오지 못했습니다.");
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const processGroupOptions = useMemo(
    () => (groupTree ? flattenProcessGroups(groupTree) : []),
    [groupTree],
  );
  const isFileLoad = etlType === "FILE_LOAD";
  const canComplete = value.jobName.trim().length > 0 &&
    value.parentGroupId.length > 0 &&
    (!isFileLoad || (["csv", "excel"].includes(value.fileExtension) && value.files.length > 0));

  return (
    <>
      <table className="etl-spec-table">
        <tbody>
          <tr>
            <th>작업명</th>
            <td>
              <input
                className="spec-input"
                value={value.jobName}
                onChange={(event) => onChange({ jobName: event.target.value })}
                placeholder="작업명을 입력하세요"
              />
            </td>
          </tr>
          <tr>
            <th>상위 그룹</th>
            <td>
              <select
                className="spec-select"
                value={value.parentGroupId}
                disabled={!groupTree || !!groupError}
                onChange={(event) => {
                  const selectedGroup = processGroupOptions.find((group) => group.id === event.target.value);
                  onChange({
                    parentGroupId: event.target.value,
                    parentGroupPath: selectedGroup?.label ?? "",
                  });
                }}
              >
                <option value="" disabled>
                  {groupError ? "Processor Group 조회 실패" : groupTree ? "Processor Group을 선택하세요" : "Processor Group 조회 중"}
                </option>
                {processGroupOptions.map((group) => (
                  <option key={group.id} value={group.id}>
                    {group.label}
                  </option>
                ))}
              </select>
            </td>
          </tr>
          <tr>
            <th>설명</th>
            <td>
              <input
                className="spec-input"
                value={value.comments}
                onChange={(event) => onChange({ comments: event.target.value })}
                placeholder="설명을 입력하세요"
              />
            </td>
          </tr>
          {isFileLoad ? (
            <>
              <tr>
                <th>파일확장자</th>
                <td>
                  <div className="spec-radio-group">
                    <Radio.Group
                      value={value.fileExtension}
                      options={FILE_EXTENSION_OPTIONS}
                      onChange={(event) => onChange({ fileExtension: event.target.value, files: [] })}
                    />
                  </div>
                </td>
              </tr>
              <tr>
                <th>파일 추가</th>
                <td>
                  <div className="spec-file-upload">
                    <button
                      type="button"
                      className="btn-file-select"
                      onClick={() => document.getElementById("etl-file-load-input")?.click()}
                    >
                      <UploadOutlined /> 파일선택
                    </button>
                    <input
                      id="etl-file-load-input"
                      type="file"
                      multiple
                      accept={value.fileExtension === "csv" ? ".csv" : value.fileExtension === "excel" ? ".xlsx,.xls" : undefined}
                      onChange={(event) => onChange({ files: Array.from(event.target.files ?? []) })}
                    />
                    <span className="file-chosen-text">
                      {value.files.length > 0 ? `${value.files.length}개 파일 선택됨` : "선택된 파일 없음"}
                    </span>
                  </div>
                </td>
              </tr>
            </>
          ) : null}
        </tbody>
      </table>

      <div className="step-actions-bar etl-actions-end">
        <div className="actions-right">
          <button type="button" className="btn-step-next" disabled={!canComplete} onClick={onComplete}>
            다음
          </button>
        </div>
      </div>
    </>
  );
}

function ConnectionStep({
  value,
  etlType,
  httpSourceJson,
  httpTestPassed,
  httpTestType,
  onChange,
  onHttpTestSuccess,
  onSourceColumnsChange,
  onTargetColumnsChange,
}: {
  value: ConnectionInfo;
  etlType: EtlType;
  httpSourceJson: string;
  httpTestPassed: boolean;
  httpTestType?: "JSON_API" | "FILE_DOWNLOAD";
  onChange: (nextValue: Partial<ConnectionInfo>) => void;
  onHttpTestSuccess: (testType: "JSON_API" | "FILE_DOWNLOAD", responseBody: string) => void;
  onSourceColumnsChange: (columns: SourceColumnMetadata[]) => void;
  onTargetColumnsChange: (columns: ColumnMetadataResponse[]) => void;
}) {
  const [services, setServices] = useState<ControllerServiceOption[]>([]);
  const [connections, setConnections] = useState<ConnectionResponse[]>([]);
  const [serviceError, setServiceError] = useState<string | null>(null);
  const [sourceSchemas, setSourceSchemas] = useState<string[]>([]);
  const [sourceTables, setSourceTables] = useState<string[]>([]);
  const [sourceSchemaLoading, setSourceSchemaLoading] = useState(false);
  const [sourceTableLoading, setSourceTableLoading] = useState(false);
  const [sourceSchemaError, setSourceSchemaError] = useState<string | null>(null);
  const [sourceTableError, setSourceTableError] = useState<string | null>(null);
  const [targetSchemas, setTargetSchemas] = useState<string[]>([]);
  const [targetTables, setTargetTables] = useState<string[]>([]);
  const [targetSchemaLoading, setTargetSchemaLoading] = useState(false);
  const [targetTableLoading, setTargetTableLoading] = useState(false);
  const [targetSchemaError, setTargetSchemaError] = useState<string | null>(null);
  const [targetTableError, setTargetTableError] = useState<string | null>(null);
  const [showApiKey, setShowApiKey] = useState(false);
  const [testingApi, setTestingApi] = useState<"JSON_API" | "FILE_DOWNLOAD" | null>(null);
  const sourceEnabled = etlType === "DB_TO_DB";
  const httpEnabled = etlType === "HTTP";
  useEffect(() => {
    let cancelled = false;

    listRootNifiControllerServices()
      .then((result) => {
        if (cancelled) {
          return;
        }
        setServices(
          result
            .map(toControllerServiceOption)
            .filter((service): service is ControllerServiceOption => service !== null)
            .filter(isDbcpConnectionPool),
        );
        setServiceError(null);
      })
      .catch((ex) => {
        if (!cancelled) {
          setServiceError(ex instanceof Error ? ex.message : "Controller Service를 불러오지 못했습니다.");
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    listConnections()
      .then((result) => {
        if (!cancelled) {
          setConnections(result);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setConnections([]);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const servicePlaceholder = serviceError
    ? "Controller Service 조회 실패"
    : services.length > 0
      ? "Controller Service를 선택하세요"
      : "DBCPConnectionPool Controller Service가 없습니다";

  const connectionByServiceId = (serviceId: string) => {
    const service = services.find((entry) => entry.id === serviceId);
    if (!service) {
      return null;
    }
    return connections.find(
      (connection) => normalizeControllerServiceName(connection.name) === normalizeControllerServiceName(service.name),
    ) ?? null;
  };

  const sourceConnection = connectionByServiceId(value.sourceServiceId);
  const sourceConnectionReady = !!sourceConnection;
  const sourceDatabaseType = nifiDatabaseTypeFromConnection(sourceConnection);
  const targetConnection = connectionByServiceId(value.targetServiceId);
  const targetConnectionReady = !!targetConnection;
  const targetDatabaseType = nifiDatabaseTypeFromConnection(targetConnection);

  useEffect(() => {
    if (sourceConnection && (value.sourceDatabaseType !== sourceDatabaseType || value.sourceConnectionId !== sourceConnection.id)) {
      onChange({ sourceDatabaseType: sourceDatabaseType ?? value.sourceDatabaseType, sourceConnectionId: sourceConnection.id });
    }
  }, [sourceConnection, sourceDatabaseType, value.sourceConnectionId, value.sourceDatabaseType, onChange]);

  useEffect(() => {
    if (targetDatabaseType && value.targetDatabaseType !== targetDatabaseType) {
      onChange({ targetDatabaseType });
    }
  }, [targetDatabaseType, value.targetDatabaseType, onChange]);

  useEffect(() => {
    let cancelled = false;
    setSourceSchemas([]);
    setSourceTables([]);
    setSourceSchemaError(null);
    setSourceTableError(null);

    if (!sourceEnabled || !sourceConnection) {
      setSourceSchemaLoading(false);
      setSourceTableLoading(false);
      return () => {
        cancelled = true;
      };
    }

    setSourceSchemaLoading(true);
    listConnectionSchemas(sourceConnection.id)
      .then((schemas) => {
        if (!cancelled) {
          setSourceSchemas(schemas);
        }
      })
      .catch((ex) => {
        if (!cancelled) {
          setSourceSchemaError(ex instanceof Error ? ex.message : "소스 스키마를 불러오지 못했습니다.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setSourceSchemaLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [sourceEnabled, sourceConnection?.id]);

  useEffect(() => {
    let cancelled = false;
    setSourceTables([]);
    setSourceTableError(null);
    onSourceColumnsChange([]);

    if (!sourceEnabled || !sourceConnection || !value.sourceSchema.trim()) {
      setSourceTableLoading(false);
      return () => {
        cancelled = true;
      };
    }

    setSourceTableLoading(true);
    listConnectionTables(sourceConnection.id, value.sourceSchema.trim())
      .then((tables) => {
        if (!cancelled) {
          setSourceTables(tables);
        }
      })
      .catch((ex) => {
        if (!cancelled) {
          setSourceTableError(ex instanceof Error ? ex.message : "소스 테이블을 불러오지 못했습니다.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setSourceTableLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [sourceEnabled, sourceConnection?.id, value.sourceSchema, onSourceColumnsChange]);

  useEffect(() => {
    let cancelled = false;
    setTargetSchemas([]);
    setTargetTables([]);
    setTargetSchemaError(null);
    setTargetTableError(null);

    if (!targetConnection) {
      setTargetSchemaLoading(false);
      setTargetTableLoading(false);
      return () => {
        cancelled = true;
      };
    }

    setTargetSchemaLoading(true);
    listConnectionSchemas(targetConnection.id)
      .then((schemas) => {
        if (!cancelled) {
          setTargetSchemas(schemas);
        }
      })
      .catch((ex) => {
        if (!cancelled) {
          setTargetSchemaError(ex instanceof Error ? ex.message : "타깃 스키마를 불러오지 못했습니다.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setTargetSchemaLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [targetConnection?.id]);

  useEffect(() => {
    let cancelled = false;
    setTargetTables([]);
    setTargetTableError(null);
    onTargetColumnsChange([]);

    if (!targetConnection || !value.targetSchema.trim()) {
      setTargetTableLoading(false);
      return () => {
        cancelled = true;
      };
    }

    setTargetTableLoading(true);
    listConnectionTables(targetConnection.id, value.targetSchema.trim())
      .then((tables) => {
        if (!cancelled) {
          setTargetTables(tables);
        }
      })
      .catch((ex) => {
        if (!cancelled) {
          setTargetTableError(ex instanceof Error ? ex.message : "타깃 테이블을 불러오지 못했습니다.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setTargetTableLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [targetConnection?.id, value.targetSchema, onTargetColumnsChange]);

  useEffect(() => {
    let cancelled = false;
    onSourceColumnsChange([]);
    const selectedSourceTables = commaSeparatedValues(value.sourceTable);

    if (!sourceEnabled || !sourceConnection || !value.sourceSchema.trim() || selectedSourceTables.length === 0) {
      return () => {
        cancelled = true;
      };
    }

    Promise.all(
      selectedSourceTables.map((table) =>
        listConnectionColumns(sourceConnection.id, value.sourceSchema.trim(), table)
          .then((columns) => columns.map((column) => ({ ...column, table }))),
      ),
    )
      .then((columnGroups) => {
        if (!cancelled) {
          onSourceColumnsChange(columnGroups.flat());
        }
      })
      .catch(() => {
        if (!cancelled) {
          onSourceColumnsChange([]);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [sourceEnabled, sourceConnection?.id, value.sourceSchema, value.sourceTable, onSourceColumnsChange]);

  useEffect(() => {
    let cancelled = false;
    onTargetColumnsChange([]);

    if (!targetConnection || !value.targetSchema.trim() || !value.targetTable.trim()) {
      return () => {
        cancelled = true;
      };
    }

    listConnectionColumns(targetConnection.id, value.targetSchema.trim(), value.targetTable.trim())
      .then((columns) => {
        if (!cancelled) {
          onTargetColumnsChange(columns);
        }
      })
      .catch(() => {
        if (!cancelled) {
          onTargetColumnsChange([]);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [targetConnection?.id, value.targetSchema, value.targetTable, onTargetColumnsChange]);

  const updateValue = (nextValue: Partial<ConnectionInfo>) => {
    onChange(nextValue);
  };

  const handleHttpApiTest = async (testType: "JSON_API" | "FILE_DOWNLOAD") => {
    if (!value.apiUrl.trim()) {
      message.warning("API URL을 입력하세요.");
      return;
    }
    setTestingApi(testType);
    try {
      const result = await testHttpApi({
        httpMethod: value.httpMethod,
        testType,
        apiUrl: value.apiUrl.trim(),
        apiKey: value.apiKey.trim() || undefined,
      });
      if (result.statusCode < 200 || result.statusCode >= 300) {
        message.error(`${testType === "JSON_API" ? "JSON API" : "파일 다운로드"} 테스트 실패 (HTTP ${result.statusCode})`);
        return;
      }
      const preview = testType === "JSON_API" ? formatJsonPreview(result.responseBody) : "";
      onHttpTestSuccess(testType, preview);
      message.success(`${testType === "JSON_API" ? "JSON API" : "파일 다운로드"} 테스트 성공 (HTTP ${result.statusCode})`);
    } catch (ex) {
      message.error(ex instanceof Error ? ex.message : "HTTP 연결 테스트에 실패했습니다.");
    } finally {
      setTestingApi(null);
    }
  };

  const handleSourceServiceChange = (serviceId: string) => {
    const nextConnection = connectionByServiceId(serviceId);
    const nextDatabaseType = nifiDatabaseTypeFromConnection(nextConnection);
    updateValue({
      sourceServiceId: serviceId,
      sourceConnectionId: nextConnection?.id ?? null,
      sourceDatabaseType: nextDatabaseType ?? value.sourceDatabaseType,
      sourceSchema: "",
      sourceTable: "",
    });
  };

  const handleTargetServiceChange = (serviceId: string) => {
    const nextDatabaseType = nifiDatabaseTypeFromConnection(connectionByServiceId(serviceId));
    updateValue({
      targetServiceId: serviceId,
      targetDatabaseType: nextDatabaseType ?? value.targetDatabaseType,
      targetSchema: "",
      targetTable: "",
    });
  };

  const sourceSchemaPlaceholder = !sourceConnectionReady
    ? "소스 연결을 선택하세요"
    : sourceSchemaLoading
      ? "소스 스키마 조회 중"
      : sourceSchemaError
        ? "소스 스키마 조회 실패"
        : sourceSchemas.length > 0
          ? "소스 스키마를 선택하세요"
          : "조회된 소스 스키마가 없습니다";
  const targetSchemaPlaceholder = !targetConnectionReady
    ? "타깃 연결을 선택하세요"
    : targetSchemaLoading
      ? "타깃 스키마 조회 중"
      : targetSchemaError
        ? "타깃 스키마 조회 실패"
        : targetSchemas.length > 0
          ? "타깃 스키마를 선택하세요"
          : "조회된 타깃 스키마가 없습니다";
  const sourceTablePlaceholder = !value.sourceSchema.trim()
    ? "소스 스키마를 먼저 선택하세요"
    : sourceTableLoading
      ? "소스 테이블 조회 중"
      : sourceTableError
        ? "소스 테이블 조회 실패"
        : sourceTables.length > 0
          ? "소스 테이블을 선택하세요"
          : "조회된 소스 테이블이 없습니다";
  const targetTablePlaceholder = !value.targetSchema.trim()
    ? "타깃 스키마를 먼저 선택하세요"
    : targetTableLoading
      ? "타깃 테이블 조회 중"
      : targetTableError
        ? "타깃 테이블 조회 실패"
        : targetTables.length > 0
          ? "타깃 테이블을 선택하세요"
          : "조회된 타깃 테이블이 없습니다";
  const sourceSchemaOptions = sourceSchemas.map((schema) => ({ label: schema, value: schema }));
  const targetSchemaOptions = targetSchemas.map((schema) => ({ label: schema, value: schema }));
  const sourceTableOptions = sourceTables.map((table) => ({ label: table, value: table }));
  const targetTableOptions = targetTables.map((table) => ({ label: table, value: table }));

  return (
    <table className="etl-spec-table">
      <tbody>
        {httpEnabled ? (
          <>
            <tr>
              <th>HTTP 방법</th>
              <td>
                <div className="spec-radio-group">
                  <label className="spec-radio-label">
                    <input
                      type="radio"
                      name="etl-http-method"
                      checked={value.httpMethod === "GET"}
                      onChange={() => updateValue({ httpMethod: "GET" })}
                    />
                    <span>GET</span>
                  </label>
                  <label className="spec-radio-label">
                    <input
                      type="radio"
                      name="etl-http-method"
                      checked={value.httpMethod === "POST"}
                      onChange={() => updateValue({ httpMethod: "POST" })}
                    />
                    <span>POST</span>
                  </label>
                </div>
              </td>
            </tr>
            <tr>
              <th>API URL</th>
              <td>
                <input
                  className="spec-input"
                  value={value.apiUrl}
                  onChange={(event) => updateValue({ apiUrl: event.target.value })}
                  placeholder="https://api.example.com/weather"
                />
              </td>
            </tr>
            <tr>
              <th>API KEY</th>
              <td>
                <div className="etl-secret-input">
                  <input
                    className="spec-input"
                    type={showApiKey ? "text" : "password"}
                    value={value.apiKey}
                    onChange={(event) => updateValue({ apiKey: event.target.value })}
                    placeholder="API KEY를 입력하세요"
                    autoComplete="off"
                  />
                  <button
                    type="button"
                    className="etl-secret-toggle"
                    title={showApiKey ? "API KEY 숨기기" : "API KEY 보기"}
                    onClick={() => setShowApiKey((current) => !current)}
                  >
                    {showApiKey ? <EyeInvisibleOutlined /> : <EyeOutlined />}
                  </button>
                </div>
              </td>
            </tr>
            <tr>
              <th>연결 테스트</th>
              <td>
                <div className="etl-http-test-row">
                  <Button
                    size="small"
                    loading={testingApi === "JSON_API"}
                    disabled={!value.apiUrl.trim()}
                    onClick={() => handleHttpApiTest("JSON_API")}
                  >
                    JSON API 테스트
                  </Button>
                  <Button
                    size="small"
                    loading={testingApi === "FILE_DOWNLOAD"}
                    disabled={!value.apiUrl.trim()}
                    onClick={() => handleHttpApiTest("FILE_DOWNLOAD")}
                  >
                    파일 다운로드 테스트
                  </Button>
                  <span>
                    {httpTestPassed
                      ? `${httpTestType === "FILE_DOWNLOAD" ? "파일 다운로드" : "JSON API"} 테스트 통과`
                      : "둘 중 하나의 테스트를 통과하면 다음 단계로 이동할 수 있습니다."}
                    {httpSourceJson.trim() ? " · 성공한 JSON 응답을 4번 소스 JSON에 표시합니다." : ""}
                  </span>
                </div>
              </td>
            </tr>
          </>
        ) : null}
        {sourceEnabled ? (
          <>
            <tr>
              <th>소스 연결</th>
              <td>
                <select
                  className="spec-select"
                  value={value.sourceServiceId}
                  disabled={services.length === 0 || !!serviceError}
                  onChange={(event) => handleSourceServiceChange(event.target.value)}
                >
                  <option value="" disabled>
                    {servicePlaceholder}
                  </option>
                  {services.map((service) => (
                    <option key={service.id} value={service.id}>
                      {service.displayName}
                    </option>
                  ))}
                </select>
              </td>
            </tr>
            <tr>
              <th>소스 데이터베이스</th>
              <td>
                <select
                  className="spec-select"
                  value={value.sourceDatabaseType}
                  disabled={!!sourceDatabaseType}
                  onChange={(event) => updateValue({ sourceDatabaseType: event.target.value as NifiDatabaseType })}
                >
                  {DATABASE_TYPE_OPTIONS.map((databaseType) => (
                    <option key={databaseType} value={databaseType}>
                      {databaseType}
                    </option>
                  ))}
                </select>
              </td>
            </tr>
            <tr>
              <th>소스 스키마</th>
              <td>
                <Select
                  className="spec-ant-select"
                  showSearch
                  value={value.sourceSchema || undefined}
                  placeholder={sourceSchemaPlaceholder}
                  disabled={!sourceConnectionReady || sourceSchemaLoading || !!sourceSchemaError || sourceSchemas.length === 0}
                  loading={sourceSchemaLoading}
                  options={sourceSchemaOptions}
                  filterOption={matchesSelectOption}
                  onChange={(nextSchema) => updateValue({ sourceSchema: nextSchema, sourceTable: "" })}
                />
              </td>
            </tr>
            <tr>
              <th>소스 테이블</th>
              <td>
                <Select
                  className="spec-ant-select"
                  mode="multiple"
                  showSearch
                  value={commaSeparatedValues(value.sourceTable)}
                  placeholder={sourceTablePlaceholder}
                  disabled={!value.sourceSchema.trim() || sourceTableLoading || !!sourceTableError || sourceTables.length === 0}
                  loading={sourceTableLoading}
                  options={sourceTableOptions}
                  filterOption={matchesSelectOption}
                  onChange={(nextTables) => updateValue({ sourceTable: nextTables.join(",") })}
                />
              </td>
            </tr>
          </>
        ) : null}
        <tr>
          <th>타깃 연결</th>
          <td>
            <select
              className="spec-select"
              value={value.targetServiceId}
              disabled={services.length === 0 || !!serviceError}
              onChange={(event) => handleTargetServiceChange(event.target.value)}
            >
              <option value="" disabled>
                {servicePlaceholder}
              </option>
              {services.map((service) => (
                <option key={service.id} value={service.id}>
                  {service.displayName}
                </option>
              ))}
            </select>
          </td>
        </tr>
        <tr>
          <th>타깃 데이터베이스</th>
          <td>
            <select
              className="spec-select"
              value={value.targetDatabaseType}
              disabled={!!targetDatabaseType}
              onChange={(event) => updateValue({ targetDatabaseType: event.target.value as NifiDatabaseType })}
            >
              {DATABASE_TYPE_OPTIONS.map((databaseType) => (
                <option key={databaseType} value={databaseType}>
                  {databaseType}
                </option>
              ))}
            </select>
          </td>
        </tr>
        <tr>
          <th>타깃 스키마</th>
          <td>
            <Select
              className="spec-ant-select"
              showSearch
              value={value.targetSchema || undefined}
              placeholder={targetSchemaPlaceholder}
              disabled={!targetConnectionReady || targetSchemaLoading || !!targetSchemaError || targetSchemas.length === 0}
              loading={targetSchemaLoading}
              options={targetSchemaOptions}
              filterOption={matchesSelectOption}
              onChange={(nextSchema) => updateValue({ targetSchema: nextSchema, targetTable: "" })}
            />
          </td>
        </tr>
        <tr>
          <th>타깃 테이블</th>
          <td>
            <Select
              className="spec-ant-select"
              showSearch
              value={value.targetTable || undefined}
              placeholder={targetTablePlaceholder}
              disabled={!value.targetSchema.trim() || targetTableLoading || !!targetTableError || targetTables.length === 0}
              loading={targetTableLoading}
              options={targetTableOptions}
              filterOption={matchesSelectOption}
              onChange={(nextTable) => updateValue({ targetTable: nextTable })}
            />
          </td>
        </tr>
      </tbody>
    </table>
  );
}

function QueryStep({
  etlType,
  httpSourceJson,
  connectionInfo,
  loadMode,
  truncateSql,
  loadSql,
  showTruncateSql,
  onLoadModeChange,
  onTruncateSqlChange,
  onLoadSqlChange,
  onShowTruncateSqlChange,
}: {
  etlType: EtlType;
  httpSourceJson: string;
  connectionInfo: ConnectionInfo;
  loadMode: LoadMode;
  truncateSql: string;
  loadSql: string;
  showTruncateSql: boolean;
  onLoadModeChange: (loadMode: LoadMode) => void;
  onTruncateSqlChange: (truncateSql: string) => void;
  onLoadSqlChange: (loadSql: string) => void;
  onShowTruncateSqlChange: (show: boolean) => void;
}) {
  const sqlPreviewRef = useRef<HTMLPreElement | null>(null);
  const [validatingSql, setValidatingSql] = useState(false);
  const isHttpLoad = etlType === "HTTP";
  const sourceJsonPreview = httpSourceJson.trim() || HTTP_SAMPLE_JSON;

  const syncSqlScroll = (event: UIEvent<HTMLTextAreaElement>) => {
    if (!sqlPreviewRef.current) {
      return;
    }
    sqlPreviewRef.current.scrollTop = event.currentTarget.scrollTop;
    sqlPreviewRef.current.scrollLeft = event.currentTarget.scrollLeft;
  };

  const handleSqlKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key !== "Tab") {
      return;
    }
    event.preventDefault();
    const target = event.currentTarget;
    const start = target.selectionStart;
    const end = target.selectionEnd;
    const nextValue = `${loadSql.slice(0, start)}    ${loadSql.slice(end)}`;
    onLoadSqlChange(nextValue);
    requestAnimationFrame(() => {
      target.selectionStart = start + 4;
      target.selectionEnd = start + 4;
    });
  };

  const formatSqlOnBlur = () => {
    const formattedSql = formatLoadSqlIndent(loadSql);
    if (formattedSql !== loadSql) {
      onLoadSqlChange(formattedSql);
    }
  };

  const validateSql = async () => {
    if (!connectionInfo.sourceConnectionId) {
      message.warning("3번 연결 단계에서 소스 연결을 먼저 선택하세요.");
      return;
    }
    if (!loadSql.trim()) {
      message.warning("적재로직 SQL문을 입력하세요.");
      return;
    }
    setValidatingSql(true);
    try {
      const result = await validateConnectionSql(connectionInfo.sourceConnectionId, loadSql);
      if (result.success) {
        message.success(`${connectionInfo.sourceDatabaseType} 유효성 체크 성공 (${result.latencyMs}ms)`);
      } else {
        message.error(result.message || "유효성 체크에 실패했습니다.");
      }
    } catch (ex) {
      message.error(ex instanceof Error ? ex.message : "유효성 체크에 실패했습니다.");
    } finally {
      setValidatingSql(false);
    }
  };

  return (
    <div className="etl-target-preview">
      <table className="etl-spec-table">
        <tbody>
          <tr>
            <th>적재 방식</th>
            <td>
              <div className="spec-radio-group">
                <label className="spec-radio-label">
                  <input
                    type="radio"
                    name="etl-load-mode"
                    checked={loadMode === "INSERT"}
                    onChange={() => onLoadModeChange("INSERT")}
                  />
                  <span>INSERT</span>
                </label>
                {!isHttpLoad ? (
                  <label className="spec-radio-label">
                    <input
                      type="radio"
                      name="etl-load-mode"
                      checked={loadMode === "TRUNCATE"}
                      onChange={() => onLoadModeChange("TRUNCATE")}
                    />
                    <span>TRUNCATE 후 적재</span>
                  </label>
                ) : null}
                <label className="spec-radio-label">
                  <input
                    type="radio"
                    name="etl-load-mode"
                    checked={loadMode === "UPSERT"}
                    onChange={() => onLoadModeChange("UPSERT")}
                  />
                  <span>UPSERT</span>
                </label>
              </div>
            </td>
          </tr>
          {isHttpLoad ? (
            <tr>
              <th>소스 JSON</th>
              <td>
                <pre className="etl-source-json-preview">{sourceJsonPreview}</pre>
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
      {!isHttpLoad && loadMode === "TRUNCATE" ? (
        <div className="etl-truncate-sql">
          <button
            type="button"
            className="etl-truncate-sql-toggle"
            onClick={() => onShowTruncateSqlChange(!showTruncateSql)}
          >
            TRUNCATE 문 직접 작성
          </button>
          {showTruncateSql ? (
            <textarea
              value={truncateSql}
              onChange={(event) => onTruncateSqlChange(event.target.value)}
              placeholder="예: TRUNCATE TABLE public.target_table"
            />
          ) : null}
        </div>
      ) : null}
      {!isHttpLoad ? (
      <div className="etl-change-key-column">
        <label>적재로직 SQL문</label>
        <div className="etl-sql-cell">
          <div className="etl-sql-actions">
            <Button
              size="small"
              onClick={validateSql}
              loading={validatingSql}
              disabled={!connectionInfo.sourceConnectionId || !loadSql.trim()}
            >
              유효성 체크
            </Button>
          </div>
          <div className="etl-sql-editor">
            <pre ref={sqlPreviewRef} aria-hidden="true">
              {renderSqlText(loadSql)}
            </pre>
            <textarea
              value={loadSql}
              spellCheck={false}
              onChange={(event) => onLoadSqlChange(event.target.value)}
              onBlur={formatSqlOnBlur}
              onKeyDown={handleSqlKeyDown}
              onScroll={syncSqlScroll}
              placeholder={`select\n     as 타겟컬럼1\n    , as 타겟컬럼2\nfrom ${connectionInfo.sourceSchema || "소스스키마"}.소스테이블`}
            />
          </div>
        </div>
        <small className="etl-sql-hint">테이블 간 조인 조건을 추가 바랍니다.</small>
      </div>
      ) : null}
    </div>
  );
}

function TargetStep({
  etlType,
  connectionInfo,
  sourceColumns,
  targetColumns,
  columnMappings,
  loadSql,
  onQueryRecordSqlChange,
  onColumnMappingsChange,
  onSourceColumnsChange,
  onLoadSqlColumnsChange,
  onLoadSqlChange,
}: {
  etlType: EtlType;
  connectionInfo: ConnectionInfo;
  sourceColumns: SourceColumnMetadata[];
  targetColumns: ColumnMetadataResponse[];
  columnMappings: ColumnMapping[];
  loadSql: string;
  onQueryRecordSqlChange: (queryRecordSql: string) => void;
  onColumnMappingsChange: (mappings: ColumnMapping[]) => void;
  onSourceColumnsChange: (columns: SourceColumnMetadata[]) => void;
  onLoadSqlColumnsChange: (columns: SourceColumnMetadata[]) => void;
  onLoadSqlChange: (loadSql: string) => void;
}) {
  const [editingMappingId, setEditingMappingId] = useState<string | null>(null);
  const [editingLogic, setEditingLogic] = useState("");
  const [sourceOrderIds, setSourceOrderIds] = useState<string[]>([]);
  const [draggingSourceKey, setDraggingSourceKey] = useState<string | null>(null);
  const [draggingMappingId, setDraggingMappingId] = useState<string | null>(null);
  const [dragOverTargetMappingId, setDragOverTargetMappingId] = useState<string | null>(null);
  const [validatingCalcite, setValidatingCalcite] = useState(false);
  const [mappingValidationDialog, setMappingValidationDialog] = useState<MappingValidationDialog | null>(null);
  const selectedSourceTables = etlType === "HTTP" ? HTTP_SELECTED_TABLES : commaSeparatedValues(connectionInfo.sourceTable);
  const editingMapping = columnMappings.find((mapping) => mapping.id === editingMappingId) ?? null;
  const mappingDisplayExpression = (mapping: ColumnMapping) => displayMappingExpression(mapping, selectedSourceTables);
  const sourceProjectionColumns = sourceColumns
    .filter((sourceColumn, index, columns) =>
      columns.findIndex((item) =>
        sourceColumnKey(item.table, item.name) === sourceColumnKey(sourceColumn.table, sourceColumn.name),
      ) === index,
    );
  const orderedSourceMappings = [
    ...sourceOrderIds
      .map((sourceKey) => sourceProjectionColumns.find((sourceColumn) =>
        sourceColumnKey(sourceColumn.table, sourceColumn.name) === sourceKey,
      ))
      .filter((sourceColumn): sourceColumn is SourceColumnMetadata => !!sourceColumn),
    ...sourceProjectionColumns.filter((sourceColumn) =>
      !sourceOrderIds.includes(sourceColumnKey(sourceColumn.table, sourceColumn.name)),
    ),
  ];
  const sourceIndexByKey = new Map(orderedSourceMappings.map((sourceColumn, index) => [
    sourceColumnKey(sourceColumn.table, sourceColumn.name),
    index,
  ]));
  const loadSqlTableByColumn = useMemo(() => extractLoadSqlAliasColumnTableMap(loadSql), [loadSql]);
  const targetIndexByMappingId = new Map(columnMappings.map((mapping, index) => [mapping.id, index]));
  const flowConnections = columnMappings.flatMap((mapping) =>
    mappingReferencedSourceColumns(mapping, sourceColumns, selectedSourceTables).map((sourceColumn) => ({
      sourceKey: sourceColumnKey(sourceColumn.table, sourceColumn.name),
      targetMappingId: mapping.id,
    })),
  );
  const flowRowCount = Math.max(orderedSourceMappings.length, columnMappings.length);
  const flowHeight = Math.max(flowRowCount * COLUMN_MAPPING_ROW_HEIGHT, COLUMN_MAPPING_ROW_HEIGHT);

  useEffect(() => {
    setSourceOrderIds((previousIds) => {
      const projectionIds = sourceProjectionColumns.map((sourceColumn) =>
        sourceColumnKey(sourceColumn.table, sourceColumn.name),
      );
      return [
        ...previousIds.filter((sourceKey) => projectionIds.includes(sourceKey)),
        ...projectionIds.filter((sourceKey) => !previousIds.includes(sourceKey)),
      ];
    });
  }, [columnMappings, connectionInfo.sourceTable, sourceColumns]);

  const applyColumnMappings = (nextMappings: ColumnMapping[]) => {
    onColumnMappingsChange(nextMappings);
    onQueryRecordSqlChange(buildQueryRecordSql(nextMappings, selectedSourceTables));
  };

  const updateMapping = (mappingId: string, updates: Partial<ColumnMapping>) => {
    applyColumnMappings(columnMappings.map((mapping) =>
      mapping.id === mappingId ? { ...mapping, ...updates } : mapping,
    ));
  };

  const moveMapping = (mappingId: string, direction: -1 | 1) => {
    const currentIndex = columnMappings.findIndex((mapping) => mapping.id === mappingId);
    const nextIndex = currentIndex + direction;
    if (currentIndex < 0 || nextIndex < 0 || nextIndex >= columnMappings.length) {
      return;
    }
    const nextMappings = [...columnMappings];
    const [removed] = nextMappings.splice(currentIndex, 1);
    nextMappings.splice(nextIndex, 0, removed);
    applyColumnMappings(nextMappings);
  };

  const reorderMapping = (mappingId: string, targetMappingId: string) => {
    const currentIndex = columnMappings.findIndex((mapping) => mapping.id === mappingId);
    const targetIndex = columnMappings.findIndex((mapping) => mapping.id === targetMappingId);
    if (currentIndex < 0 || targetIndex < 0 || currentIndex === targetIndex) {
      return;
    }
    const nextMappings = [...columnMappings];
    const [removed] = nextMappings.splice(currentIndex, 1);
    nextMappings.splice(targetIndex, 0, removed);
    applyColumnMappings(nextMappings);
  };

  const addSourceColumnToTarget = (sourceKey: string, mappingId: string) => {
    const sourceColumn = sourceProjectionColumns.find((column) =>
      sourceColumnKey(column.table, column.name) === sourceKey,
    );
    if (!sourceColumn) {
      return;
    }
    const nextMappings = columnMappings.map((mapping) =>
      mapping.id === mappingId ? appendSourceColumnToMapping(mapping, sourceColumn) : mapping,
    );
    applyColumnMappings(nextMappings);
  };

  const removeFlowConnection = (sourceKey: string, mappingId: string) => {
    const sourceColumn = sourceProjectionColumns.find((column) =>
      sourceColumnKey(column.table, column.name) === sourceKey,
    );
    if (!sourceColumn) {
      return;
    }
    const nextMappings = columnMappings.map((mapping) => {
      if (mapping.id !== mappingId) {
        return mapping;
      }
      const directSourceRemoved = sourceColumnKey(mapping.sourceTable, mapping.sourceColumn) === sourceKey;
      const nextLogic = mapping.logic.trim()
        ? removeSourceColumnFromLogic(mapping.logic, sourceColumn.name)
        : "";
      return {
        ...mapping,
        sourceTable: directSourceRemoved ? "" : mapping.sourceTable,
        sourceColumn: directSourceRemoved ? "" : mapping.sourceColumn,
        logic: nextLogic,
      };
    });
    if (editingMappingId === mappingId) {
      const editingLogicNext = removeSourceColumnFromLogic(editingLogic, sourceColumn.name);
      setEditingLogic(editingLogicNext);
    }
    applyColumnMappings(nextMappings);
  };

  const moveSourceMapping = (sourceKey: string, direction: -1 | 1) => {
    const orderedIds = orderedSourceMappings.map((sourceColumn) =>
      sourceColumnKey(sourceColumn.table, sourceColumn.name),
    );
    const currentIndex = orderedIds.indexOf(sourceKey);
    const nextIndex = currentIndex + direction;
    if (currentIndex < 0 || nextIndex < 0 || nextIndex >= orderedIds.length) {
      return;
    }
    const nextIds = [...orderedIds];
    const [removed] = nextIds.splice(currentIndex, 1);
    nextIds.splice(nextIndex, 0, removed);
    setSourceOrderIds(nextIds);
  };

  const reorderSourceMapping = (sourceKey: string, targetSourceKey: string) => {
    const orderedIds = orderedSourceMappings.map((sourceColumn) =>
      sourceColumnKey(sourceColumn.table, sourceColumn.name),
    );
    const currentIndex = orderedIds.indexOf(sourceKey);
    const targetIndex = orderedIds.indexOf(targetSourceKey);
    if (currentIndex < 0 || targetIndex < 0 || currentIndex === targetIndex) {
      return;
    }
    const nextIds = [...orderedIds];
    const [removed] = nextIds.splice(currentIndex, 1);
    nextIds.splice(targetIndex, 0, removed);
    setSourceOrderIds(nextIds);
  };

  const startSourceDrag = (event: DragEvent<HTMLDivElement>, sourceKey: string) => {
    setDraggingSourceKey(sourceKey);
    event.dataTransfer.effectAllowed = "copyMove";
    event.dataTransfer.setData(SOURCE_COLUMN_DRAG_TYPE, sourceKey);
    event.dataTransfer.setData("text/plain", sourceKey);
  };

  const startMappingDrag = (event: DragEvent<HTMLDivElement>, mappingId: string) => {
    setDraggingMappingId(mappingId);
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", mappingId);
  };

  const deleteSourceColumn = (column: SourceColumnMetadata) => {
    const removedKey = sourceColumnKey(column.table, column.name);
    const nextColumns = sourceColumns.filter(
      (sourceColumn) => sourceColumnKey(sourceColumn.table, sourceColumn.name) !== removedKey,
    );
    if (column.table === LOAD_SQL_SOURCE_TABLE) {
      onLoadSqlColumnsChange(nextColumns);
      onLoadSqlChange(removeLoadSqlAliasColumn(loadSql, column.name));
    } else {
      onSourceColumnsChange(nextColumns);
    }
    applyColumnMappings(columnMappings.map((mapping) => {
      const directSourceRemoved = sourceColumnKey(mapping.sourceTable, mapping.sourceColumn) === removedKey;
      const nextLogic = mapping.logic.trim()
        ? removeSourceColumnFromLogic(mapping.logic, column.name)
        : "";
      return {
        ...mapping,
        sourceTable: directSourceRemoved ? "" : mapping.sourceTable,
        sourceColumn: directSourceRemoved ? "" : mapping.sourceColumn,
        logic: nextLogic,
      };
    }));
    if (editingMapping) {
      setEditingLogic(removeSourceColumnFromLogic(editingLogic, column.name));
    }
  };

  const openLogicEditor = (mapping: ColumnMapping) => {
    setEditingMappingId(mapping.id);
    setEditingLogic(mappingDisplayExpression(mapping));
  };

  const saveLogicEditor = () => {
    if (!editingMapping) {
      return;
    }
    updateMapping(editingMapping.id, { logic: editingLogic });
    setEditingMappingId(null);
    setEditingLogic("");
  };

  const validateCurrentLogic = async () => {
    if (!editingMapping) {
      return;
    }
    setValidatingCalcite(true);
    try {
      const sql = buildQueryRecordSql([{ ...editingMapping, logic: editingLogic }], selectedSourceTables);
      const result = await validateCalciteSql(sql);
      if (result.success) {
        message.success(`Calcite 문법 검증 성공 (${result.latencyMs}ms)`);
      } else {
        message.error(result.message || "Calcite 문법 검증에 실패했습니다.");
      }
    } catch (ex) {
      message.error(ex instanceof Error ? ex.message : "Calcite 문법 검증에 실패했습니다.");
    } finally {
      setValidatingCalcite(false);
    }
  };

  const validateAllMappings = async () => {
    setValidatingCalcite(true);
    try {
      const querySql = buildQueryRecordSql(columnMappings, selectedSourceTables);
      const calciteResult = await validateCalciteSql(querySql);
      const consistencyIssues = validateMappingConsistency(
        columnMappings,
        sourceColumns,
        targetColumns,
        selectedSourceTables,
      );
      const issues: MappingValidationIssue[] = [
        ...(calciteResult.success
          ? []
          : [{ severity: "error" as const, message: `Calcite 문법 검사 실패: ${calciteResult.message}` }]),
        ...consistencyIssues,
      ];
      const errorCount = issues.filter((issue) => issue.severity === "error").length;
      const warningCount = issues.filter((issue) => issue.severity === "warning").length;
      if (issues.length === 0) {
        Modal.success({
          title: "Calcite/정합성 체크 성공",
          content: `Calcite 문법 검사와 매핑 정합성 검사를 통과했습니다. (${calciteResult.latencyMs}ms)`,
        });
      } else {
        setMappingValidationDialog({
          errorCount,
          warningCount,
          calciteMessage: calciteResult.success
            ? `Calcite 문법 검사: 성공 (${calciteResult.latencyMs}ms)`
            : `Calcite 문법 검사: 실패 - ${calciteResult.message}`,
          issues,
        });
      }
    } catch (ex) {
      message.error(ex instanceof Error ? ex.message : "Calcite/정합성 체크에 실패했습니다.");
    } finally {
      setValidatingCalcite(false);
    }
  };

  const openFormulaHelp = () => {
    const width = 640;
    const height = 880;
    const left = Math.max(0, Math.round(window.screenX + (window.outerWidth - width) / 2));
    const top = Math.max(0, Math.round(window.screenY + (window.outerHeight - height) / 2));
    window.open(
      "/etl/formula-help",
      "cerebroEtlFormulaHelp",
      `popup=yes,width=${width},height=${height},left=${left},top=${top},resizable=yes,scrollbars=yes`,
    );
  };

  return (
    <div className="etl-target-preview">
      <div className="etl-column-mapping-panel">
        <div className="etl-column-mapping-toolbar">
          <strong>컬럼 매핑</strong>
          <Button
            size="small"
            onClick={validateAllMappings}
            loading={validatingCalcite}
            disabled={columnMappings.length === 0}
          >
            Calcite 체크
          </Button>
        </div>
          {selectedSourceTables.length > 1 ? (
            <div className="etl-mapping-alias-hint">
              {selectedSourceTables.map((table, index) => (
                <Tag key={table}>{table} = {tableAlias(index)}</Tag>
              ))}
            </div>
          ) : null}
          <div className="etl-column-mapping-layout">
            <div className="etl-source-column-list">
              <div className="etl-column-list-heading etl-source-column-heading">
                <span />
                <span>테이블</span>
                <span>소스컬럼</span>
                <span />
              </div>
              {orderedSourceMappings.length > 0 ? orderedSourceMappings.map((sourceColumn, index) => {
                const sourceKey = sourceColumnKey(sourceColumn.table, sourceColumn.name);
                const sourceTableLabel = sourceColumn.table === LOAD_SQL_SOURCE_TABLE
                  ? loadSqlTableByColumn.get(normalizeName(sourceColumn.name)) ?? "SQL"
                  : sourceColumn.table || "-";
                return (
                <div
                  key={sourceKey}
                  className={`etl-source-column-row${draggingSourceKey === sourceKey ? " dragging" : ""}`}
                  draggable
                  onDragStart={(event) => startSourceDrag(event, sourceKey)}
                  onDragEnd={() => {
                    setDraggingSourceKey(null);
                    setDragOverTargetMappingId(null);
                  }}
                  onDragOver={(event) => {
                    if (draggingSourceKey && draggingSourceKey !== sourceKey) {
                      event.preventDefault();
                      event.dataTransfer.dropEffect = "move";
                    }
                  }}
                  onDrop={(event) => {
                    event.preventDefault();
                    if (draggingSourceKey) {
                      reorderSourceMapping(draggingSourceKey, sourceKey);
                    }
                    setDraggingSourceKey(null);
                  }}
                >
                  <div className="etl-mapping-order-buttons">
                    <Button
                      size="small"
                      title="위로 이동"
                      icon={<UpOutlined />}
                      disabled={index === 0}
                      onClick={() => moveSourceMapping(sourceKey, -1)}
                    />
                    <Button
                      size="small"
                      title="아래로 이동"
                      icon={<DownOutlined />}
                      disabled={index === orderedSourceMappings.length - 1}
                      onClick={() => moveSourceMapping(sourceKey, 1)}
                    />
                  </div>
                  <span title={sourceTableLabel}>{sourceTableLabel}</span>
                  <strong>{sourceColumn.name}</strong>
                  <Button
                    size="small"
                    title="소스 컬럼 삭제"
                    icon={<CloseOutlined />}
                    onClick={() => deleteSourceColumn(sourceColumn)}
                  />
                </div>
                );
              }) : (
                <div className="etl-column-empty">소스 컬럼 조회 결과가 없습니다.</div>
              )}
            </div>
            <div className="etl-column-flow-list">
              <div className="etl-column-list-heading">연결</div>
              {columnMappings.length > 0 ? (
                <svg
                  className="etl-column-flow-canvas"
                  viewBox={`0 0 100 ${flowHeight}`}
                  preserveAspectRatio="none"
                  style={{ height: flowHeight }}
                >
                  <defs>
                    <marker
                      id="etl-column-flow-arrow"
                      viewBox="0 0 10 10"
                      refX="9"
                      refY="5"
                      markerWidth="6"
                      markerHeight="6"
                      orient="auto-start-reverse"
                    >
                      <path d="M 0 0 L 10 5 L 0 10 z" />
                    </marker>
                  </defs>
                  {flowConnections.map((connection, index) => {
                    const sourceIndex = sourceIndexByKey.get(connection.sourceKey);
                    const targetIndex = targetIndexByMappingId.get(connection.targetMappingId);
                    if (sourceIndex === undefined || targetIndex === undefined) {
                      return null;
                    }
                    const sourceY = sourceIndex * COLUMN_MAPPING_ROW_HEIGHT + COLUMN_MAPPING_ROW_HEIGHT / 2;
                    const targetY = targetIndex * COLUMN_MAPPING_ROW_HEIGHT + COLUMN_MAPPING_ROW_HEIGHT / 2;
                    const pathKey = `${connection.sourceKey}-${connection.targetMappingId}-${index}`;
                    const pathData = `M 1 ${sourceY} C 34 ${sourceY}, 66 ${targetY}, 99 ${targetY}`;
                    return (
                      <g
                        key={pathKey}
                        onDoubleClick={() => removeFlowConnection(connection.sourceKey, connection.targetMappingId)}
                      >
                        <title>더블클릭하면 연결과 처리 로직 참조를 삭제합니다.</title>
                        <path className="etl-column-flow-hit" d={pathData} />
                        <path d={pathData} />
                      </g>
                    );
                  })}
                </svg>
              ) : (
                <div className="etl-column-empty">-</div>
              )}
            </div>
            <div className="etl-target-column-list">
              <div className="etl-column-list-heading">
                타깃컬럼{connectionInfo.targetTable ? `(${connectionInfo.targetTable})` : ""}
              </div>
              {columnMappings.length > 0 ? columnMappings.map((mapping, index) => (
                <div
                  key={mapping.id}
                  className={`etl-target-column-row${draggingMappingId === mapping.id ? " dragging" : ""}${dragOverTargetMappingId === mapping.id ? " drop-target" : ""}`}
                  draggable
                  onDragStart={(event) => startMappingDrag(event, mapping.id)}
                  onDragEnd={() => {
                    setDraggingMappingId(null);
                    setDragOverTargetMappingId(null);
                  }}
                  onDragOver={(event) => {
                    const sourceKey = draggingSourceKey || event.dataTransfer.types.includes(SOURCE_COLUMN_DRAG_TYPE);
                    if (sourceKey || (draggingMappingId && draggingMappingId !== mapping.id)) {
                      event.preventDefault();
                      event.dataTransfer.dropEffect = sourceKey ? "copy" : "move";
                    }
                    if (sourceKey) {
                      setDragOverTargetMappingId(mapping.id);
                    }
                  }}
                  onDragLeave={(event) => {
                    const nextTarget = event.relatedTarget;
                    if (!(nextTarget instanceof Node) || !event.currentTarget.contains(nextTarget)) {
                      setDragOverTargetMappingId((current) => current === mapping.id ? null : current);
                    }
                  }}
                  onDrop={(event) => {
                    event.preventDefault();
                    const droppedSourceKey = draggingSourceKey || event.dataTransfer.getData(SOURCE_COLUMN_DRAG_TYPE);
                    if (droppedSourceKey) {
                      addSourceColumnToTarget(droppedSourceKey, mapping.id);
                    } else if (draggingMappingId) {
                      reorderMapping(draggingMappingId, mapping.id);
                    }
                    setDraggingSourceKey(null);
                    setDraggingMappingId(null);
                    setDragOverTargetMappingId(null);
                  }}
                >
                  <Button
                    size="small"
                    title="처리 로직 편집"
                    icon={<EditOutlined />}
                    onClick={() => openLogicEditor(mapping)}
                  />
                  <div className="etl-mapping-order-buttons">
                    <Button
                      size="small"
                      title="위로 이동"
                      icon={<UpOutlined />}
                      disabled={index === 0}
                      onClick={() => moveMapping(mapping.id, -1)}
                    />
                    <Button
                      size="small"
                      title="아래로 이동"
                      icon={<DownOutlined />}
                      disabled={index === columnMappings.length - 1}
                      onClick={() => moveMapping(mapping.id, 1)}
                    />
                  </div>
                  <strong>{mapping.targetColumn}</strong>
                </div>
              )) : (
                <div className="etl-column-empty">타깃 컬럼 조회 결과가 없습니다.</div>
              )}
            </div>
          </div>
      </div>

      <Modal
        title="변환 - 처리 로직"
        open={!!editingMapping}
        width="min(960px, 92vw)"
        onOk={saveLogicEditor}
        onCancel={() => {
          setEditingMappingId(null);
          setEditingLogic("");
        }}
        okText="확인"
        cancelText="취소"
      >
        <div className="etl-logic-modal-body">
          <label>대상 컬럼</label>
          <Input value={editingMapping?.targetColumn ?? ""} disabled />
          <label>처리 로직</label>
          <Input.TextArea
            value={editingLogic}
            rows={12}
            onChange={(event) => setEditingLogic(event.target.value)}
            placeholder={editingMapping?.sourceTable && editingMapping.sourceColumn
              ? editingMapping.sourceColumn
              : "예: NVL(column_name, 'N')"}
          />
          <small>* Apache Calcite SQL 문법에 따라 작성해야합니다.</small>
          <div className="etl-logic-modal-actions">
            <Button size="small" onClick={validateCurrentLogic} loading={validatingCalcite}>
              유효성 체크
            </Button>
            <button type="button" className="etl-formula-help-link bounce-animation" onClick={openFormulaHelp}>
              <QuestionCircleOutlined /> 도움말
            </button>
          </div>
        </div>
      </Modal>
      <Modal
        title="매핑 오류 내용"
        open={!!mappingValidationDialog}
        width={940}
        footer={null}
        className="mapping-error-modal"
        onCancel={() => setMappingValidationDialog(null)}
      >
        {mappingValidationDialog ? (
          <div className="mapping-error-content">
            <section className="mapping-error-section">
              <div className="mapping-error-label">GPT 해석</div>
              <div className="mapping-error-gpt">
                <div className="mapping-error-gpt-head">
                  <strong>GPT 해석</strong>
                  <span>매핑 검사 결과 기반 자동 해석</span>
                </div>
                <div className="mapping-error-gpt-body">
                  {buildMappingValidationInterpretation(mappingValidationDialog).map((line, index) => (
                    <p key={index}>{index + 1}. {line}</p>
                  ))}
                </div>
              </div>
            </section>
            <section className="mapping-error-section">
              <div className="mapping-error-label">에러 내용</div>
              <pre className="mapping-error-log">{[
                mappingValidationDialog.calciteMessage,
                `오류 ${mappingValidationDialog.errorCount}건 / 경고 ${mappingValidationDialog.warningCount}건`,
                "",
                ...mappingValidationDialog.issues.map((issue, index) =>
                  `${index + 1}. [${issue.severity === "error" ? "오류" : "경고"}] ${issue.message}`,
                ),
              ].join("\n")}</pre>
            </section>
          </div>
        ) : null}
      </Modal>
    </div>
  );
}

function FileTargetStep({ columns }: { columns: string[] }) {
  return (
    <div className="etl-target-preview">
      <table className="etl-spec-table">
        <tbody>
          <tr>
            <th>적재 방식</th>
            <td>
              <div className="spec-radio-group">
                <label className="spec-radio-label">
                  <input type="radio" checked readOnly />
                  <span>INSERT</span>
                </label>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
      {columns.length > 0 ? (
        <div className="etl-file-columns">
          {columns.map((column) => (
            <Tag key={column}>{column.toUpperCase()}</Tag>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function flowLabel(loadMode: LoadMode) {
  if (loadMode === "TRUNCATE") {
    return "extract > truncate > load";
  }
  if (loadMode === "UPSERT") {
    return "extract > upsert > load";
  }
  return "extract > insert > load";
}

function summaryFlowLabel(summary: CompletionSummary) {
  if (summary.etlType === "FILE_LOAD") {
    return "file > query record > insert";
  }
  if (summary.etlType === "UNSTRUCTURED") {
    return "unstructured file > put sql > record load";
  }
  return flowLabel(summary.loadMode);
}

function CompletionView({
  summary,
  onOpenCanvas,
  onOpenDag,
}: {
  summary: CompletionSummary;
  onOpenCanvas: () => void;
  onOpenDag: () => void;
}) {
  const canvasLabel = summary.canvasReady ? "캔버스에서 보기" : "캔버스에서 보기 (ETL 관리 준비 중)";
  const dagLabel = summary.dagReady ? "DAG 보기" : "DAG 보기 (Airflow 준비 중)";
  return (
    <div className="std-card std-card-body etl-completion-card">
      <Result
        status="success"
        title="ETL 템플릿 그룹 생성 완료"
        subTitle="아래 리소스가 생성되었습니다."
        extra={[
          <Button type="primary" key="canvas" disabled={!summary.canvasReady} onClick={onOpenCanvas}>
            {canvasLabel}
          </Button>,
          <Button key="dag" disabled={!summary.dagReady} onClick={onOpenDag}>
            {dagLabel}
          </Button>,
        ]}
      />
      <Descriptions bordered column={1} size="middle" className="etl-completion-summary">
        <Descriptions.Item label="프로세스 그룹">
          1개 ({summary.processGroupPath})
        </Descriptions.Item>
        <Descriptions.Item label="프로세서">
          {summary.processorCount}개
        </Descriptions.Item>
        <Descriptions.Item label="흐름">
          {summaryFlowLabel(summary)}
        </Descriptions.Item>
        <Descriptions.Item label="Airflow 작업">
          {summary.dagId ? `1개 (${summary.dagId})` : "1개 (준비 중)"}
        </Descriptions.Item>
      </Descriptions>
    </div>
  );
}

export function EtlCreatePage() {
  const navigate = useNavigate();
  const restoredDraft = useMemo(() => readEtlCreateDraft(), []);
  const preserveRestoredSqlRef = useRef(Boolean(restoredDraft?.loadSql));
  const [activeStep, setActiveStep] = useState<WizardStepId>(restoredDraft?.activeStep ?? "type");
  const [etlType, setEtlType] = useState<EtlType>(restoredEtlType(restoredDraft));
  const [completed, setCompleted] = useState(false);
  const [isCreating, setIsCreating] = useState(false);
  const [completedSteps, setCompletedSteps] = useState<WizardStepId[]>(restoredDraft?.completedSteps ?? []);
  const [completionSummary, setCompletionSummary] = useState<CompletionSummary | null>(null);
  const [basicInfo, setBasicInfo] = useState<BasicInfo>({
    ...DEFAULT_BASIC_INFO,
    ...restoredDraft?.basicInfo,
    files: [],
  });
  const [connectionInfo, setConnectionInfo] = useState<ConnectionInfo>({
    ...DEFAULT_CONNECTION_INFO,
    ...restoredDraft?.connectionInfo,
  });
  const [loadMode, setLoadMode] = useState<LoadMode>(restoredDraft?.loadMode ?? "INSERT");
  const [showTruncateSql, setShowTruncateSql] = useState(restoredDraft?.showTruncateSql ?? false);
  const [truncateSql, setTruncateSql] = useState(restoredDraft?.truncateSql ?? "");
  const [loadSql, setLoadSql] = useState(restoredDraft?.loadSql ?? defaultLoadSql("INSERT", "", "", "PostgreSQL"));
  const [queryRecordSql, setQueryRecordSql] = useState(restoredDraft?.queryRecordSql ?? "");
  const [sourceColumns, setSourceColumns] = useState<SourceColumnMetadata[]>(restoredDraft?.sourceColumns ?? []);
  const [loadSqlColumns, setLoadSqlColumns] = useState<SourceColumnMetadata[]>(restoredDraft?.loadSqlColumns ?? []);
  const [httpSourceJson, setHttpSourceJson] = useState(restoredDraft?.httpSourceJson ?? "");
  const [httpTestPassed, setHttpTestPassed] = useState(restoredDraft?.httpTestPassed ?? false);
  const [httpTestType, setHttpTestType] = useState<"JSON_API" | "FILE_DOWNLOAD" | undefined>(restoredDraft?.httpTestType);
  const [targetColumns, setTargetColumns] = useState<ColumnMetadataResponse[]>(restoredDraft?.targetColumns ?? []);
  const [columnMappings, setColumnMappings] = useState<ColumnMapping[]>(restoredDraft?.columnMappings ?? []);
  const mappingSourceColumns = useMemo(
    () => etlType === "HTTP" ? HTTP_SOURCE_COLUMNS : loadSqlColumns.length > 0 ? loadSqlColumns : sourceColumns,
    [etlType, loadSqlColumns, sourceColumns],
  );
  const targetPrimaryKeys = useMemo(
    () => targetColumns
      .filter((column) => column.primaryKey)
      .map((column) => column.name)
      .join(","),
    [targetColumns],
  );

  const moveStep = (stepId: WizardStepId) => {
    if (completed) {
      return;
    }
    setCompleted(false);
    setActiveStep(stepId);
  };

  const completeAndOpen = (currentStep: WizardStepId, nextStep: WizardStepId) => {
    setCompletedSteps((previous) => previous.includes(currentStep) ? previous : [...previous, currentStep]);
    moveStep(nextStep);
  };

  const invalidateFrom = (stepId: WizardStepId) => {
    const index = WIZARD_STEPS.findIndex((step) => step.id === stepId);
    setCompletedSteps((previous) => previous.filter((step) => WIZARD_STEPS.findIndex((item) => item.id === step) < index));
  };

  const isFileLoad = etlType === "FILE_LOAD";
  const isHttpLoad = etlType === "HTTP";
  const isUnstructuredLoad = etlType === "UNSTRUCTURED";
  const visibleWizardSteps = isUnstructuredLoad ? WIZARD_STEPS.slice(0, 3) : WIZARD_STEPS;

  useEffect(() => {
    if (isUnstructuredLoad && (activeStep === "query" || activeStep === "target")) {
      setActiveStep("connection");
    }
  }, [activeStep, isUnstructuredLoad]);

  const selectedSourceTablesForQueryRecord = useMemo(
    () => isHttpLoad ? HTTP_SELECTED_TABLES : commaSeparatedValues(connectionInfo.sourceTable),
    [connectionInfo.sourceTable, isHttpLoad],
  );
  const canCompleteConnection = (isFileLoad || isHttpLoad || isUnstructuredLoad || !!connectionInfo.sourceServiceId) &&
    (!isHttpLoad || (!!connectionInfo.apiUrl.trim() && httpTestPassed)) &&
    !!connectionInfo.targetServiceId &&
    !!connectionInfo.targetSchema.trim() &&
    (isFileLoad || isHttpLoad || isUnstructuredLoad || !!connectionInfo.sourceSchema.trim()) &&
    (isFileLoad || isHttpLoad || isUnstructuredLoad || !!connectionInfo.sourceTable.trim()) &&
    !!connectionInfo.targetTable.trim();

  useEffect(() => {
    if (completed) {
      return;
    }
    writeEtlCreateDraft({
      activeStep,
      etlType,
      completedSteps,
      basicInfo: {
        jobName: basicInfo.jobName,
        parentGroupId: basicInfo.parentGroupId,
        parentGroupPath: basicInfo.parentGroupPath,
        comments: basicInfo.comments,
        fileExtension: basicInfo.fileExtension,
      },
      connectionInfo,
      loadMode,
      showTruncateSql,
      truncateSql,
      loadSql,
      queryRecordSql,
      sourceColumns,
      loadSqlColumns,
      httpSourceJson,
      httpTestPassed,
      httpTestType,
      targetColumns,
      columnMappings,
    });
  }, [
    activeStep,
    basicInfo,
    columnMappings,
    completed,
    completedSteps,
    connectionInfo,
    etlType,
    loadMode,
    loadSql,
    loadSqlColumns,
    httpSourceJson,
    httpTestPassed,
    httpTestType,
    queryRecordSql,
    showTruncateSql,
    sourceColumns,
    targetColumns,
    truncateSql,
  ]);

  useEffect(() => {
    if (!completed || !completionSummary || (completionSummary.canvasReady && completionSummary.dagReady)) {
      return;
    }
    let cancelled = false;
    const processGroupId = completionSummary.processGroupId;

    const updateReadiness = (next: Pick<CompletionSummary, "canvasReady" | "dagId" | "dagReady">) => {
      if (cancelled) {
        return;
      }
      setCompletionSummary((previous) => {
        if (!previous || previous.processGroupId !== processGroupId) {
          return previous;
        }
        return { ...previous, ...next };
      });
    };

    const checkReadiness = async () => {
      let canvasReady = completionSummary.canvasReady;
      let dagId = completionSummary.dagId ?? null;
      let dagReady = completionSummary.dagReady;

      for (let attempt = 0; attempt < 30 && !cancelled && (!canvasReady || !dagReady); attempt += 1) {
        const treeResult = await refreshNifiProcessGroupTree()
          .then((tree) => processGroupTreeIncludes(tree, processGroupId))
          .catch(() => false);
        canvasReady = canvasReady || treeResult;

        await syncEtlJobs().catch(() => undefined);
        const mirroredJob = await listEtlJobs()
          .then((jobs) => jobs.find((job) => job.nifiPgId === processGroupId) ?? null)
          .catch(() => null);
        dagId = dagId ?? mirroredJob?.airflowDagId ?? null;

        if (dagId) {
          await syncAirflowDagCatalog().catch(() => undefined);
          const catalogHasDag = await listAirflowDagCatalog()
            .then((catalog) => catalog.some((entry) => entry.dagId === dagId))
            .catch(() => false);
          dagReady = dagReady || catalogHasDag;
        }

        updateReadiness({ canvasReady, dagId, dagReady });
        if (!canvasReady || !dagReady) {
          await sleep(2000);
        }
      }
    };

    void checkReadiness();
    return () => {
      cancelled = true;
    };
  }, [completed, completionSummary?.processGroupId]);

  useEffect(() => {
    if (isHttpLoad) {
      setSourceColumns(HTTP_SOURCE_COLUMNS);
      setLoadSqlColumns([]);
      if (loadMode === "TRUNCATE") {
        setLoadMode("INSERT");
      }
      return;
    }
    if (isFileLoad) {
      return;
    }
    if (isUnstructuredLoad) {
      setSourceColumns([]);
      setLoadSqlColumns([]);
      setColumnMappings([]);
      setQueryRecordSql("");
      return;
    }
    if (targetColumns.length > 0) {
      setColumnMappings((previousMappings) => {
        if (preserveRestoredSqlRef.current && previousMappings.length > 0) {
          setQueryRecordSql(buildQueryRecordSql(previousMappings, selectedSourceTablesForQueryRecord));
          return previousMappings;
        }
        const nextMappings = buildDefaultColumnMappings(targetColumns, sourceColumns, previousMappings);
        if (!preserveRestoredSqlRef.current) {
          setLoadSql(formatLoadSqlIndent(buildMappedLoadSql(connectionInfo, sourceColumns, targetColumns, loadMode)));
        }
        setQueryRecordSql(buildQueryRecordSql(nextMappings, selectedSourceTablesForQueryRecord));
        return nextMappings;
      });
      return;
    }
    if (preserveRestoredSqlRef.current) {
      return;
    }
    setColumnMappings([]);
    setQueryRecordSql("");
    if (!preserveRestoredSqlRef.current) {
      setLoadSql(formatLoadSqlIndent(defaultLoadSql(
        loadMode,
        connectionInfo.sourceSchema,
        connectionInfo.sourceTable,
        connectionInfo.sourceDatabaseType,
      )));
    }
  }, [
    connectionInfo.sourceDatabaseType,
    connectionInfo.sourceSchema,
    connectionInfo.sourceTable,
    connectionInfo.targetTable,
    isHttpLoad,
    isFileLoad,
    isUnstructuredLoad,
    loadMode,
    selectedSourceTablesForQueryRecord,
    sourceColumns,
    targetColumns,
  ]);

  useEffect(() => {
    if (!isHttpLoad) {
      return;
    }
    setColumnMappings((previousMappings) => {
      const nextMappings = buildDefaultColumnMappings(targetColumns, HTTP_SOURCE_COLUMNS, previousMappings);
      setQueryRecordSql(buildQueryRecordSql(nextMappings, HTTP_SELECTED_TABLES));
      return nextMappings;
    });
  }, [isHttpLoad, targetColumns]);

  const completeQueryStep = () => {
    if (isHttpLoad) {
      setLoadSqlColumns([]);
      setColumnMappings((previousMappings) => {
        const nextMappings = buildDefaultColumnMappings(targetColumns, HTTP_SOURCE_COLUMNS, previousMappings);
        setQueryRecordSql(buildQueryRecordSql(nextMappings, HTTP_SELECTED_TABLES));
        return nextMappings;
      });
      completeAndOpen("query", "target");
      return;
    }
    const extractedColumns = extractLoadSqlAliasColumns(loadSql);
    const nextSourceColumns = extractedColumns.length > 0 ? extractedColumns : sourceColumns;
    setLoadSqlColumns(extractedColumns);
    setColumnMappings((previousMappings) => {
      const nextMappings = buildDefaultColumnMappings(targetColumns, nextSourceColumns, previousMappings);
      setQueryRecordSql(buildQueryRecordSql(nextMappings, commaSeparatedValues(connectionInfo.sourceTable)));
      return nextMappings;
    });
    completeAndOpen("query", "target");
  };

  const completeWizard = async () => {
    if (!basicInfo.jobName.trim() || !basicInfo.parentGroupId) {
      message.warning("기본 정보의 작업명과 상위 그룹을 입력하세요.");
      moveStep("basic");
      return;
    }
    if (isFileLoad && basicInfo.files.length === 0) {
      message.warning("파일을 선택하세요.");
      moveStep("basic");
      return;
    }
    if (isHttpLoad && (!connectionInfo.apiUrl.trim() || !httpTestPassed)) {
      message.warning("API URL 입력 후 JSON API 테스트 또는 파일 다운로드 테스트를 완료하세요.");
      moveStep("connection");
      return;
    }
    if (
      (!isFileLoad && !isHttpLoad && !isUnstructuredLoad && (!connectionInfo.sourceServiceId ||
        !connectionInfo.sourceSchema.trim() ||
        !connectionInfo.sourceTable.trim())) ||
      !connectionInfo.targetServiceId ||
      !connectionInfo.targetSchema.trim() ||
      !connectionInfo.targetTable.trim()
    ) {
      message.warning("연결 정보를 모두 입력하세요.");
      moveStep("connection");
      return;
    }
    if (!isFileLoad && !isHttpLoad && !isUnstructuredLoad && (loadMode === "INSERT" || loadMode === "UPSERT") && !loadSql.trim()) {
      message.warning("적재로직 SQL문을 입력해야 합니다.");
      return;
    }
    if (!isFileLoad && !isUnstructuredLoad && loadMode === "UPSERT" && !targetPrimaryKeys.trim()) {
      message.warning("타깃 테이블의 Primary Key를 찾지 못했습니다.");
      return;
    }

    setIsCreating(true);
    try {
      let createdGroup: NifiProcessGroupEntity;
      if (isFileLoad) {
        const uploadResult = await uploadFileLoadFiles(
          basicInfo.jobName.trim(),
          basicInfo.fileExtension as "csv" | "excel",
          basicInfo.files,
        );
        if (uploadResult.columns.length === 0) {
          throw new Error("파일 헤더에서 컬럼을 찾지 못했습니다.");
        }
        createdGroup = await createFileLoadFlow({
          jobName: basicInfo.jobName.trim(),
          parentGroupId: basicInfo.parentGroupId,
          comments: basicInfo.comments.trim(),
          fileExtension: basicInfo.fileExtension as "csv" | "excel",
          inputDirectory: uploadResult.nifiInputDirectory,
          columns: uploadResult.columns,
          targetServiceId: connectionInfo.targetServiceId,
          targetDatabaseType: connectionInfo.targetDatabaseType,
          targetSchema: connectionInfo.targetSchema.trim(),
          targetTable: connectionInfo.targetTable.trim(),
        });
      } else if (isHttpLoad) {
        createdGroup = await createHttpLoadFlow({
          jobName: basicInfo.jobName.trim(),
          parentGroupId: basicInfo.parentGroupId,
          comments: basicInfo.comments.trim(),
          httpMethod: connectionInfo.httpMethod,
          apiUrl: connectionInfo.apiUrl.trim(),
          apiKey: connectionInfo.apiKey.trim() || undefined,
          targetServiceId: connectionInfo.targetServiceId,
          targetDatabaseType: connectionInfo.targetDatabaseType,
          targetSchema: connectionInfo.targetSchema.trim(),
          targetTable: connectionInfo.targetTable.trim(),
          loadMode: loadMode === "UPSERT" ? "UPSERT" : "INSERT",
          queryRecordSql: queryRecordSql.trim() || undefined,
          primaryKeys: loadMode === "UPSERT" ? targetPrimaryKeys : undefined,
        });
      } else if (isUnstructuredLoad) {
        createdGroup = await createUnstructuredLoadFlow({
          jobName: basicInfo.jobName.trim(),
          parentGroupId: basicInfo.parentGroupId,
          comments: basicInfo.comments.trim(),
          targetServiceId: connectionInfo.targetServiceId,
          targetDatabaseType: connectionInfo.targetDatabaseType,
          targetSchema: connectionInfo.targetSchema.trim(),
          targetTable: connectionInfo.targetTable.trim(),
        });
      } else {
        createdGroup = await createInitialDbToDbFlow({
          jobName: basicInfo.jobName.trim(),
          parentGroupId: basicInfo.parentGroupId,
          comments: basicInfo.comments.trim(),
          sourceServiceId: connectionInfo.sourceServiceId,
          sourceDatabaseType: connectionInfo.sourceDatabaseType,
          sourceSchema: connectionInfo.sourceSchema.trim(),
          sourceTable: connectionInfo.sourceTable.trim(),
          targetServiceId: connectionInfo.targetServiceId,
          targetDatabaseType: connectionInfo.targetDatabaseType,
          targetSchema: connectionInfo.targetSchema.trim(),
          targetTable: connectionInfo.targetTable.trim(),
          loadMode,
          truncateSql: loadMode === "TRUNCATE"
            ? truncateSql.trim() || defaultTruncateSql(connectionInfo.targetSchema, connectionInfo.targetTable)
            : undefined,
          loadSql: loadSql.trim() || undefined,
          queryRecordSql: queryRecordSql.trim() || undefined,
          primaryKeys: loadMode === "UPSERT" ? targetPrimaryKeys : undefined,
        });
      }
      const processGroupId = createdGroup.id;
      if (!processGroupId) {
        throw new Error("missing process group id");
      }
      void syncEtlJobs().catch(() => {
        message.warning("생성은 완료됐지만 ETL Job 상세 동기화가 지연되고 있습니다.");
      });
      setCompletionSummary({
        processGroupId,
        processGroupPath: `${basicInfo.parentGroupPath || basicInfo.parentGroupId} / ${basicInfo.jobName.trim()}`,
        processorCount: createdGroup.processorCount ?? 0,
        loadMode: isFileLoad || isUnstructuredLoad ? "INSERT" : isHttpLoad ? (loadMode === "UPSERT" ? "UPSERT" : "INSERT") : loadMode,
        etlType,
        canvasReady: false,
        dagId: null,
        dagReady: false,
      });
      message.success("ETL 템플릿 그룹을 복제하고 설정을 반영했습니다.");
      clearEtlCreateDraft();
      const finalStep = isUnstructuredLoad ? "connection" : "target";
      setCompletedSteps((previous) => previous.includes(finalStep) ? previous : [...previous, finalStep]);
      setCompleted(true);
      setActiveStep(finalStep);
    } catch (ex) {
      message.error(ex instanceof Error ? ex.message : "오류가 발생하였습니다. 관리자에게 문의 하십시오.");
    } finally {
      setIsCreating(false);
    }
  };

  const content = {
    type: (
      <TypeStep
        selectedType={etlType}
        onTypeChange={(nextType) => {
          preserveRestoredSqlRef.current = false;
          setEtlType(nextType);
          invalidateFrom("type");
        }}
        onComplete={() => completeAndOpen("type", "basic")}
      />
    ),
    basic: (
      <BasicStep
        value={basicInfo}
        etlType={etlType}
        onChange={(nextValue) => {
          setBasicInfo((current) => ({ ...current, ...nextValue }));
          invalidateFrom("basic");
        }}
        onComplete={() => completeAndOpen("basic", "connection")}
      />
    ),
    connection: (
      <ConnectionStep
        value={connectionInfo}
        etlType={etlType}
        httpSourceJson={httpSourceJson}
        httpTestPassed={httpTestPassed}
        httpTestType={httpTestType}
        onChange={(nextValue) => {
          preserveRestoredSqlRef.current = false;
          setConnectionInfo((current) => {
            return { ...current, ...nextValue };
          });
          if (
            nextValue.httpMethod !== undefined ||
            nextValue.apiUrl !== undefined ||
            nextValue.apiKey !== undefined
          ) {
            setHttpSourceJson("");
            setHttpTestPassed(false);
            setHttpTestType(undefined);
          }
          if (
            nextValue.targetServiceId !== undefined ||
            nextValue.targetSchema !== undefined ||
            nextValue.targetTable !== undefined
          ) {
            setColumnMappings([]);
          }
          setLoadSqlColumns([]);
          invalidateFrom("connection");
        }}
        onHttpTestSuccess={(nextTestType, responseBody) => {
          setHttpTestPassed(true);
          setHttpTestType(nextTestType);
          setHttpSourceJson(responseBody);
        }}
        onSourceColumnsChange={setSourceColumns}
        onTargetColumnsChange={setTargetColumns}
      />
    ),
    query: isFileLoad ? (
      <FileTargetStep columns={[]} />
    ) : (
      <QueryStep
        etlType={etlType}
        httpSourceJson={httpSourceJson}
        connectionInfo={connectionInfo}
        loadMode={loadMode}
        truncateSql={truncateSql}
        loadSql={loadSql}
        showTruncateSql={showTruncateSql}
        onLoadModeChange={(nextLoadMode) => {
          preserveRestoredSqlRef.current = false;
          const effectiveLoadMode = isHttpLoad && nextLoadMode === "TRUNCATE" ? "INSERT" : nextLoadMode;
          setLoadMode(effectiveLoadMode);
          if (!isHttpLoad && targetColumns.length > 0) {
            setLoadSql(formatLoadSqlIndent(buildMappedLoadSql(connectionInfo, sourceColumns, targetColumns, effectiveLoadMode)));
          }
          if (effectiveLoadMode !== "TRUNCATE") {
            setShowTruncateSql(false);
          }
        }}
        onTruncateSqlChange={setTruncateSql}
        onLoadSqlChange={setLoadSql}
        onShowTruncateSqlChange={setShowTruncateSql}
      />
    ),
    target: isFileLoad ? (
      <FileTargetStep columns={[]} />
    ) : (
      <TargetStep
        etlType={etlType}
        connectionInfo={connectionInfo}
        sourceColumns={mappingSourceColumns}
        targetColumns={targetColumns}
        columnMappings={columnMappings}
        loadSql={loadSql}
        onQueryRecordSqlChange={setQueryRecordSql}
        onColumnMappingsChange={setColumnMappings}
        onSourceColumnsChange={setSourceColumns}
        onLoadSqlColumnsChange={setLoadSqlColumns}
        onLoadSqlChange={setLoadSql}
      />
    ),
  } satisfies Record<WizardStepId, ReactNode>;

  const summaryOptions = { etlType, basicInfo, connectionInfo, loadMode };

  return (
    <div className="etl-create-page etl-create-wrap">
      {completed && completionSummary ? (
        <CompletionView
          summary={completionSummary}
          onOpenCanvas={() => navigate(`/etl/manage?processGroupId=${encodeURIComponent(completionSummary.processGroupId)}`)}
          onOpenDag={() => navigate(`/airflow/manage?dagId=${encodeURIComponent(completionSummary.dagId ?? "")}`)}
        />
      ) : (
        <>
          <CreateStepper
            activeStep={activeStep}
            completedSteps={completedSteps}
            steps={visibleWizardSteps}
            onStepClick={setActiveStep}
          />

          <div className="etl-wizard-wrap">
            <WizardStepCard
              step="type"
              title="유형 선택"
              activeStep={activeStep}
              completedSteps={completedSteps}
              summary={stepSummary("type", summaryOptions)}
              onOpen={setActiveStep}
            >
              {content.type}
            </WizardStepCard>

            <WizardStepCard
              step="basic"
              title="기본 정보"
              activeStep={activeStep}
              completedSteps={completedSteps}
              summary={stepSummary("basic", summaryOptions)}
              onOpen={setActiveStep}
            >
              {content.basic}
            </WizardStepCard>

            <WizardStepCard
              step="connection"
              title="연결"
              activeStep={activeStep}
              completedSteps={completedSteps}
              summary={stepSummary("connection", summaryOptions)}
              onOpen={setActiveStep}
            >
              {content.connection}
              <div className="step-actions-bar">
                <div className="actions-left">
                  <button type="button" className="btn-step-prev" onClick={() => setActiveStep("basic")}>이전</button>
                </div>
                <div className="actions-right">
                  <Button
                    className="btn-step-next"
                    loading={isUnstructuredLoad && isCreating}
                    disabled={!canCompleteConnection}
                    onClick={() => {
                      if (isUnstructuredLoad) {
                        void completeWizard();
                        return;
                      }
                      completeAndOpen("connection", "query");
                    }}
                  >
                    {isUnstructuredLoad ? "완료" : "다음"}
                  </Button>
                </div>
              </div>
            </WizardStepCard>

            {!isUnstructuredLoad ? (
              <>
                <WizardStepCard
                  step="query"
                  title="적재쿼리"
                  activeStep={activeStep}
                  completedSteps={completedSteps}
                  summary={stepSummary("query", summaryOptions)}
                  onOpen={setActiveStep}
                >
                  {content.query}
                  <div className="step-actions-bar">
                    <div className="actions-left">
                      <button type="button" className="btn-step-prev" onClick={() => setActiveStep("connection")}>이전</button>
                    </div>
                    <div className="actions-right">
                      <button
                        type="button"
                        className="btn-step-next"
                        disabled={!isFileLoad && !isHttpLoad && (loadMode === "INSERT" || loadMode === "UPSERT") && !loadSql.trim()}
                        onClick={completeQueryStep}
                      >
                        다음
                      </button>
                    </div>
                  </div>
                </WizardStepCard>

                <WizardStepCard
                  step="target"
                  title="컬럼매핑"
                  activeStep={activeStep}
                  completedSteps={completedSteps}
                  summary={stepSummary("target", summaryOptions)}
                  onOpen={setActiveStep}
                >
                  {content.target}
                  <div className="step-actions-bar">
                    <div className="actions-left">
                      <button type="button" className="btn-step-prev" onClick={() => setActiveStep("query")}>이전</button>
                    </div>
                    <div className="actions-right">
                      <Button
                        className="btn-step-next"
                        loading={isCreating}
                        disabled={!isFileLoad && !isHttpLoad && (
                          (loadMode === "INSERT" || loadMode === "UPSERT") && !loadSql.trim()
                        )}
                        onClick={completeWizard}
                      >
                        완료
                      </Button>
                    </div>
                  </div>
                </WizardStepCard>
              </>
            ) : null}
          </div>
        </>
      )}
    </div>
  );
}
