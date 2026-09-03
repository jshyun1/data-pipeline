import {
  CheckCircleOutlined,
  CheckOutlined,
  CloseOutlined,
  DownOutlined,
  EditOutlined,
  LockOutlined,
  PlusOutlined,
  UpOutlined,
  UploadOutlined,
} from "@ant-design/icons";
import { Alert, Button, Card, Collapse, Input, message, Modal, Radio, Select, Space, Tag } from "antd";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  listConnections,
  listConnectionColumns,
  listConnectionSchemas,
  listConnectionTables,
  type ColumnMetadataResponse,
} from "../api/connections";
import {
  createInitialDbToDbFlow,
  createFileLoadFlow,
  getNifiProcessGroupTree,
  listRootNifiControllerServices,
  uploadFileLoadFiles,
  type NifiProcessGroupEntity,
  type NifiControllerServiceEntity,
  type NifiProcessGroupTreeNode,
} from "../api/platform";
import { syncEtlJobs } from "../api/etlJobs";
import type { ConnectionResponse } from "../types/connection";
import { useNavigate } from "react-router-dom";

const WIZARD_STEPS = [
  { id: "type", label: "유형 선택" },
  { id: "basic", label: "기본 정보" },
  { id: "connection", label: "연결" },
  { id: "query", label: "적재쿼리" },
  { id: "target", label: "대상" },
] as const;

type WizardStepId = (typeof WIZARD_STEPS)[number]["id"];
type EtlType = "DB_TO_DB" | "FILE_LOAD" | "HTTP" | "UNSTRUCTURED" | "MANUAL";
type LoadMode = "INSERT" | "TRUNCATE" | "UPSERT";
type FileExtension = "csv" | "excel" | "log" | "json";
type NifiDatabaseType = "Generic" | "Oracle 12+" | "PostgreSQL" | "MySQL" | "MS SQL 2012+";
type ConnectionDbType = ConnectionResponse["dbType"];
const COLUMN_MAPPING_ROW_HEIGHT = 40;
const LOAD_SQL_SOURCE_TABLE = "__LOAD_SQL__";

const STEP_NUMBER: Record<WizardStepId, number> = {
  type: 1,
  basic: 2,
  connection: 3,
  query: 4,
  target: 5,
};

const TYPE_OPTIONS = [
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
    disabled: true,
  },
  {
    value: "UNSTRUCTURED",
    title: "비정형 전송",
    description: "이미지, 문서를 받아 저장",
    disabled: true,
  },
  {
    value: "MANUAL",
    title: "직접 구성",
    description: "빈 그룹만 생성",
    disabled: true,
  },
] satisfies Array<{ value: EtlType; title: string; description: string; disabled?: boolean }>;

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
  sourceServiceId: string;
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
  return normalizeName(value).replace(/[^a-z0-9가-힣]/g, "");
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

function appendLoadSqlAliasColumn(loadSql: string, columnName: string, expression: string) {
  const trimmedColumnName = columnName.trim();
  const trimmedExpression = expression.trim();
  if (!trimmedColumnName) {
    return loadSql;
  }
  if (!loadSql.trim()) {
    return `select \n${trimmedExpression} as ${trimmedColumnName}\nfrom 소스테이블`;
  }
  const fromMatch = loadSql.match(/\bfrom\b/i);
  if (!fromMatch || fromMatch.index === undefined) {
    return `${loadSql.trimEnd()}\n,${trimmedExpression} as ${trimmedColumnName}`;
  }
  const beforeFrom = loadSql.slice(0, fromMatch.index).trimEnd();
  const afterFrom = loadSql.slice(fromMatch.index);
  return `${beforeFrom}\n,${trimmedExpression} as ${trimmedColumnName}\n${afterFrom}`;
}

function loadSqlProjectionAlias(item: string) {
  const aliasMatch = item.match(/\bas\s+("[^"]+"|`[^`]+`|\[[^\]]+\]|[a-zA-Z_][\w$]*)\s*$/i);
  return aliasMatch ? unquoteSqlIdentifier(aliasMatch[1]) : "";
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
    const matchedSource = sourceColumns.find((sourceColumn) =>
      normalizeComparableName(sourceColumn.name) === targetName ||
      normalizeComparableName(sourceColumn.name).includes(targetName) ||
      targetName.includes(normalizeComparableName(sourceColumn.name)),
    );
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
    const targetColumn = targetColumnByName.get(normalizeComparableName(sourceColumn.name));
    const prefix = index === 0 ? "" : ",";
    return targetColumn ? `${prefix}${expression} as ${targetColumn}` : `${prefix}${expression}`;
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

function displayMappingExpression(mapping: ColumnMapping, selectedTables: string[]) {
  return stripSourceAliases(mapping.logic.trim() || mapping.sourceColumn || "", selectedTables);
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
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
    const unqualifiedPattern = new RegExp(`\\b${escapeRegExp(sourceColumn.name)}\\b`, "i");
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

function StepLabel({
  step,
  title,
  completed,
  unlocked,
}: {
  step: WizardStepId;
  title: string;
  completed: boolean;
  unlocked: boolean;
}) {
  return (
    <Space>
      <Tag color={completed ? "success" : unlocked ? "blue" : "default"}>{STEP_NUMBER[step]}</Tag>
      <span>{title}</span>
      {completed && <CheckCircleOutlined style={{ color: "#52c41a" }} />}
      {!unlocked && <LockOutlined style={{ color: "#999" }} />}
    </Space>
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
      <div className="etl-wizard-type-grid">
        {TYPE_OPTIONS.map((option) => (
          <button
            key={option.value}
            type="button"
            disabled={option.disabled}
            className={selectedType === option.value ? "etl-type-card selected" : "etl-type-card"}
            onClick={() => onTypeChange(option.value)}
          >
            <strong>{option.title}</strong>
            <span>{option.description}</span>
          </button>
        ))}
      </div>
      <div className="etl-step-complete-actions">
        <Button type="primary" onClick={onComplete}>
          다음: 기본 정보
        </Button>
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
      <div className="etl-wizard-form-table">
        <div className="form-row">
          <label>작업명</label>
          <input
            value={value.jobName}
            onChange={(event) => onChange({ jobName: event.target.value })}
            placeholder="작업명을 입력하세요"
          />
        </div>
        <div className="form-row">
          <label>상위 그룹</label>
          <select
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
        </div>
        <div className="form-row">
          <label>설명</label>
          <textarea
            value={value.comments}
            onChange={(event) => onChange({ comments: event.target.value })}
            placeholder="설명을 입력하세요"
          />
        </div>
        {isFileLoad ? (
          <>
            <div className="form-row">
              <label>파일확장자</label>
              <div className="etl-radio-cell">
                <Radio.Group
                  value={value.fileExtension}
                  options={FILE_EXTENSION_OPTIONS}
                  onChange={(event) => onChange({ fileExtension: event.target.value, files: [] })}
                />
              </div>
            </div>
            <div className="form-row">
              <label>파일 추가</label>
              <div className="etl-file-cell">
                <Button icon={<UploadOutlined />} onClick={() => document.getElementById("etl-file-load-input")?.click()}>
                  파일선택
                </Button>
                <input
                  id="etl-file-load-input"
                  type="file"
                  multiple
                  accept={value.fileExtension === "csv" ? ".csv" : value.fileExtension === "excel" ? ".xlsx,.xls" : undefined}
                  onChange={(event) => onChange({ files: Array.from(event.target.files ?? []) })}
                />
                <span>{value.files.length > 0 ? `${value.files.length}개 파일 선택됨` : "선택된 파일 없음"}</span>
              </div>
            </div>
          </>
        ) : null}
      </div>

      <div className="etl-step-complete-actions">
        <Button type="primary" disabled={!canComplete} onClick={onComplete}>
          다음: 연결
        </Button>
      </div>
    </>
  );
}

function ConnectionStep({
  value,
  etlType,
  onChange,
  onSourceColumnsChange,
  onTargetColumnsChange,
}: {
  value: ConnectionInfo;
  etlType: EtlType;
  onChange: (nextValue: Partial<ConnectionInfo>) => void;
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
  const sourceEnabled = etlType === "DB_TO_DB";
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
    if (sourceDatabaseType && value.sourceDatabaseType !== sourceDatabaseType) {
      onChange({ sourceDatabaseType });
    }
  }, [sourceDatabaseType, value.sourceDatabaseType, onChange]);

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

  const handleSourceServiceChange = (serviceId: string) => {
    const nextDatabaseType = nifiDatabaseTypeFromConnection(connectionByServiceId(serviceId));
    updateValue({
      sourceServiceId: serviceId,
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
    <div className="etl-wizard-form-table compact">
      <div className={sourceEnabled ? "form-row two-column" : "form-row"}>
        {sourceEnabled ? (
          <>
            <label>소스 연결</label>
            <select
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
          </>
        ) : null}
        <label>타깃 연결</label>
        <select
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
      </div>
      <div className={sourceEnabled ? "form-row two-column" : "form-row"}>
        {sourceEnabled ? (
          <>
            <label>소스 데이터베이스</label>
            <select
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
          </>
        ) : null}
        <label>타깃 데이터베이스</label>
        <select
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
      </div>
      <div className={sourceEnabled ? "form-row two-column" : "form-row"}>
        {sourceEnabled ? (
          <>
            <label>소스 스키마</label>
            <Select
              showSearch
              value={value.sourceSchema || undefined}
              placeholder={sourceSchemaPlaceholder}
              disabled={!sourceConnectionReady || sourceSchemaLoading || !!sourceSchemaError || sourceSchemas.length === 0}
              loading={sourceSchemaLoading}
              options={sourceSchemaOptions}
              filterOption={matchesSelectOption}
              onChange={(nextSchema) => updateValue({ sourceSchema: nextSchema, sourceTable: "" })}
            />
          </>
        ) : null}
        <label>타깃 스키마</label>
        <Select
          showSearch
          value={value.targetSchema || undefined}
          placeholder={targetSchemaPlaceholder}
          disabled={!targetConnectionReady || targetSchemaLoading || !!targetSchemaError || targetSchemas.length === 0}
          loading={targetSchemaLoading}
          options={targetSchemaOptions}
          filterOption={matchesSelectOption}
          onChange={(nextSchema) => updateValue({ targetSchema: nextSchema, targetTable: "" })}
        />
      </div>
      <div className={sourceEnabled ? "form-row two-column" : "form-row"}>
        {sourceEnabled ? (
          <>
            <label>소스 테이블</label>
            <Select
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
          </>
        ) : null}
        <label>타깃 테이블</label>
        <Select
          showSearch
          value={value.targetTable || undefined}
          placeholder={targetTablePlaceholder}
          disabled={!value.targetSchema.trim() || targetTableLoading || !!targetTableError || targetTables.length === 0}
          loading={targetTableLoading}
          options={targetTableOptions}
          filterOption={matchesSelectOption}
          onChange={(nextTable) => updateValue({ targetTable: nextTable })}
        />
      </div>
    </div>
  );
}

function QueryStep({
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
  return (
    <div className="etl-target-preview">
      <div className="etl-load-mode">
        <span>적재 방식</span>
        <label>
          <input
            type="radio"
            name="etl-load-mode"
            checked={loadMode === "INSERT"}
            onChange={() => onLoadModeChange("INSERT")}
          />{" "}
          INSERT
        </label>
        <label>
          <input
            type="radio"
            name="etl-load-mode"
            checked={loadMode === "TRUNCATE"}
            onChange={() => onLoadModeChange("TRUNCATE")}
          />{" "}
          TRUNCATE 후 적재
        </label>
        <label>
          <input
            type="radio"
            name="etl-load-mode"
            checked={loadMode === "UPSERT"}
            onChange={() => onLoadModeChange("UPSERT")}
          />{" "}
          UPSERT
        </label>
      </div>
      {loadMode === "TRUNCATE" ? (
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
      <div className="etl-change-key-column">
        <label>적재로직 SQL문</label>
        <textarea
          value={loadSql}
          onChange={(event) => onLoadSqlChange(event.target.value)}
          placeholder={`select\n     as 타겟컬럼1\n    , as 타겟컬럼2\nfrom ${connectionInfo.sourceSchema || "소스스키마"}.소스테이블`}
        />
        <small className="etl-sql-hint">테이블 간 조인 조건을 추가 바랍니다.</small>
      </div>
    </div>
  );
}

function TargetStep({
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
  const [addingSourceColumn, setAddingSourceColumn] = useState(false);
  const [newSourceColumnValue, setNewSourceColumnValue] = useState("");
  const [newSourceColumnName, setNewSourceColumnName] = useState("");
  const [sourceOrderIds, setSourceOrderIds] = useState<string[]>([]);
  const selectedSourceTables = commaSeparatedValues(connectionInfo.sourceTable);
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
    applyColumnMappings(columnMappings.map((mapping) =>
      sourceColumnKey(mapping.sourceTable, mapping.sourceColumn) === removedKey
        ? { ...mapping, sourceTable: "", sourceColumn: "", logic: "" }
        : mapping,
    ));
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

  const saveSourceColumn = () => {
    const columnValue = newSourceColumnValue.trim();
    const columnName = newSourceColumnName.trim();
    if (!columnValue) {
      message.warning("추가할 값을 입력하세요.");
      return;
    }
    if (!columnName) {
      message.warning("추가할 소스 컬럼명을 입력하세요.");
      return;
    }
    const exists = sourceColumns.some((sourceColumn) => normalizeName(sourceColumn.name) === normalizeName(columnName));
    if (exists) {
      message.warning("이미 존재하는 소스 컬럼입니다.");
      return;
    }
    const nextSourceColumns = [...sourceColumns, sourceColumnFromLoadSqlAlias(columnName)];
    onLoadSqlColumnsChange(nextSourceColumns);
    const nextMappings = buildDefaultColumnMappings(targetColumns, nextSourceColumns, columnMappings);
    onColumnMappingsChange(nextMappings);
    onQueryRecordSqlChange(buildQueryRecordSql(nextMappings, selectedSourceTables));
    onLoadSqlChange(appendLoadSqlAliasColumn(loadSql, columnName, columnValue));
    setAddingSourceColumn(false);
    setNewSourceColumnValue("");
    setNewSourceColumnName("");
  };

  return (
    <div className="etl-target-preview">
      <div className="etl-column-mapping-panel">
        <div className="etl-column-mapping-toolbar">
          <strong>컬럼 매핑</strong>
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
              <div className="etl-column-list-heading">소스컬럼</div>
              {orderedSourceMappings.length > 0 ? orderedSourceMappings.map((sourceColumn, index) => {
                const sourceKey = sourceColumnKey(sourceColumn.table, sourceColumn.name);
                return (
                <div key={sourceKey} className="etl-source-column-row">
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
              <div className="etl-source-column-add-row">
                <Button
                  size="small"
                  type="dashed"
                  icon={<PlusOutlined />}
                  onClick={() => setAddingSourceColumn(true)}
                >
                  소스컬럼 추가
                </Button>
              </div>
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
                    return (
                      <path
                        key={`${connection.sourceKey}-${connection.targetMappingId}-${index}`}
                        d={`M 1 ${sourceY} C 34 ${sourceY}, 66 ${targetY}, 99 ${targetY}`}
                      />
                    );
                  })}
                </svg>
              ) : (
                <div className="etl-column-empty">-</div>
              )}
            </div>
            <div className="etl-target-column-list">
              <div className="etl-column-list-heading">타깃 컬럼</div>
              {columnMappings.length > 0 ? columnMappings.map((mapping, index) => (
                <div key={mapping.id} className="etl-target-column-row">
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
                  <Button
                    size="small"
                    title="처리 로직 편집"
                    icon={<EditOutlined />}
                    onClick={() => openLogicEditor(mapping)}
                  />
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
        </div>
      </Modal>
      <Modal
        title="소스컬럼 추가"
        open={addingSourceColumn}
        onOk={saveSourceColumn}
        onCancel={() => {
          setAddingSourceColumn(false);
          setNewSourceColumnValue("");
          setNewSourceColumnName("");
        }}
        okText="확인"
        cancelText="취소"
      >
        <div className="etl-source-column-modal-body">
          <label>값</label>
          <Input
            value={newSourceColumnValue}
            autoFocus
            placeholder="예: 'N'"
            onChange={(event) => setNewSourceColumnValue(event.target.value)}
          />
          <label>소스컬럼</label>
          <Input
            value={newSourceColumnName}
            placeholder="예: del_yn"
            onChange={(event) => setNewSourceColumnName(event.target.value)}
            onPressEnter={saveSourceColumn}
          />
        </div>
      </Modal>
    </div>
  );
}

function FileTargetStep({ columns }: { columns: string[] }) {
  return (
    <div className="etl-target-preview">
      <div className="etl-load-mode">
        <span>적재 방식</span>
        <label>
          <input type="radio" checked readOnly /> INSERT
        </label>
      </div>
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
  return summary.etlType === "FILE_LOAD" ? "file > query record > insert" : flowLabel(summary.loadMode);
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
  return (
    <div className="etl-completion-stack">
      <section className="etl-review-box">
        <h3>검토</h3>
        <div className="etl-terminal-panel">
          <p>아래 리소스가 생성됩니다.</p>
          <dl>
            <div>
              <dt>프로세스 그룹</dt>
              <dd>1개 ({summary.processGroupPath})</dd>
            </div>
            <div>
              <dt>프로세서</dt>
              <dd>{summary.processorCount}개</dd>
            </div>
            <div>
              <dt>흐름</dt>
              <dd>{summaryFlowLabel(summary)}</dd>
            </div>
            <div>
              <dt>Airflow 작업</dt>
              <dd>1개</dd>
            </div>
          </dl>
        </div>
      </section>

      <section className="etl-review-box">
        <h3>완료</h3>
        <div className="etl-done-panel">
          <CheckOutlined />
          <span>생성되었습니다.</span>
          <button type="button" onClick={onOpenCanvas}>캔버스에서 보기</button>
          <button type="button" onClick={onOpenDag}>DAG 보기</button>
        </div>
      </section>
    </div>
  );
}

export function EtlCreatePage() {
  const navigate = useNavigate();
  const [activeStep, setActiveStep] = useState<WizardStepId>("type");
  const [etlType, setEtlType] = useState<EtlType>("DB_TO_DB");
  const [completed, setCompleted] = useState(false);
  const [isCreating, setIsCreating] = useState(false);
  const [completedSteps, setCompletedSteps] = useState<WizardStepId[]>([]);
  const [completionSummary, setCompletionSummary] = useState<CompletionSummary | null>(null);
  const [basicInfo, setBasicInfo] = useState<BasicInfo>({
    jobName: "",
    parentGroupId: "",
    parentGroupPath: "",
    comments: "",
    fileExtension: "csv",
    files: [],
  });
  const [connectionInfo, setConnectionInfo] = useState<ConnectionInfo>({
    sourceServiceId: "",
    sourceDatabaseType: "PostgreSQL",
    sourceSchema: "",
    sourceTable: "",
    targetServiceId: "",
    targetDatabaseType: "PostgreSQL",
    targetSchema: "",
    targetTable: "",
  });
  const [loadMode, setLoadMode] = useState<LoadMode>("INSERT");
  const [showTruncateSql, setShowTruncateSql] = useState(false);
  const [truncateSql, setTruncateSql] = useState("");
  const [loadSql, setLoadSql] = useState(defaultLoadSql("INSERT", "", "", "PostgreSQL"));
  const [queryRecordSql, setQueryRecordSql] = useState("");
  const [sourceColumns, setSourceColumns] = useState<SourceColumnMetadata[]>([]);
  const [loadSqlColumns, setLoadSqlColumns] = useState<SourceColumnMetadata[]>([]);
  const [targetColumns, setTargetColumns] = useState<ColumnMetadataResponse[]>([]);
  const [columnMappings, setColumnMappings] = useState<ColumnMapping[]>([]);
  const mappingSourceColumns = useMemo(
    () => loadSqlColumns.length > 0 ? loadSqlColumns : sourceColumns,
    [loadSqlColumns, sourceColumns],
  );
  const targetPrimaryKeys = useMemo(
    () => targetColumns
      .filter((column) => column.primaryKey)
      .map((column) => column.name)
      .join(","),
    [targetColumns],
  );

  const isUnlocked = (stepId: WizardStepId) => {
    const index = WIZARD_STEPS.findIndex((step) => step.id === stepId);
    return index === 0 || completedSteps.includes(WIZARD_STEPS[index - 1].id);
  };

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
  const canCompleteConnection = (isFileLoad || !!connectionInfo.sourceServiceId) &&
    !!connectionInfo.targetServiceId &&
    !!connectionInfo.targetSchema.trim() &&
    (isFileLoad || !!connectionInfo.sourceSchema.trim()) &&
    (isFileLoad || !!connectionInfo.sourceTable.trim()) &&
    !!connectionInfo.targetTable.trim();

  useEffect(() => {
    if (isFileLoad) {
      return;
    }
    if (targetColumns.length > 0) {
      setColumnMappings((previousMappings) => {
        const nextMappings = buildDefaultColumnMappings(targetColumns, sourceColumns, previousMappings);
        setLoadSql(buildMappedLoadSql(connectionInfo, sourceColumns, targetColumns, loadMode));
        setQueryRecordSql(buildQueryRecordSql(nextMappings, commaSeparatedValues(connectionInfo.sourceTable)));
        return nextMappings;
      });
      return;
    }
    setColumnMappings([]);
    setQueryRecordSql("");
    setLoadSql(defaultLoadSql(
      loadMode,
      connectionInfo.sourceSchema,
      connectionInfo.sourceTable,
      connectionInfo.sourceDatabaseType,
    ));
  }, [
    connectionInfo.sourceDatabaseType,
    connectionInfo.sourceSchema,
    connectionInfo.sourceTable,
    connectionInfo.targetTable,
    isFileLoad,
    loadMode,
    sourceColumns,
    targetColumns,
  ]);

  const completeQueryStep = () => {
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
    if (
      (!isFileLoad && (!connectionInfo.sourceServiceId ||
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
    if (!isFileLoad && (loadMode === "INSERT" || loadMode === "UPSERT") && !loadSql.trim()) {
      message.warning("적재로직 SQL문을 입력해야 합니다.");
      return;
    }
    if (!isFileLoad && loadMode === "UPSERT" && !targetPrimaryKeys.trim()) {
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
      try {
        await syncEtlJobs();
      } catch {
        message.warning("생성은 완료됐지만 ETL Job 상세 동기화가 지연되고 있습니다.");
      }
      setCompletionSummary({
        processGroupId,
        processGroupPath: `${basicInfo.parentGroupPath || basicInfo.parentGroupId} / ${basicInfo.jobName.trim()}`,
        processorCount: createdGroup.processorCount ?? 0,
        loadMode: isFileLoad ? "INSERT" : loadMode,
        etlType,
      });
      message.success("NiFi 템플릿 그룹을 복제하고 설정을 반영했습니다.");
      setCompletedSteps((previous) => previous.includes("target") ? previous : [...previous, "target"]);
      setCompleted(true);
      setActiveStep("target");
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
        onChange={(nextValue) => {
          setConnectionInfo((current) => {
            return { ...current, ...nextValue };
          });
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
        onSourceColumnsChange={setSourceColumns}
        onTargetColumnsChange={setTargetColumns}
      />
    ),
    query: isFileLoad ? (
      <FileTargetStep columns={[]} />
    ) : (
      <QueryStep
        connectionInfo={connectionInfo}
        loadMode={loadMode}
        truncateSql={truncateSql}
        loadSql={loadSql}
        showTruncateSql={showTruncateSql}
        onLoadModeChange={(nextLoadMode) => {
          setLoadMode(nextLoadMode);
          if (targetColumns.length > 0) {
            setLoadSql(buildMappedLoadSql(connectionInfo, sourceColumns, targetColumns, nextLoadMode));
          }
          if (nextLoadMode !== "TRUNCATE") {
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

  return (
    <div className="etl-create-page">
      <Card title="ETL 생성" style={{ marginBottom: 16 }}>
        <Alert
          type="info"
          showIcon
          message="각 단계를 완료해야 다음 단계가 열립니다."
        />
      </Card>

      {completed && completionSummary ? (
        <CompletionView
          summary={completionSummary}
          onOpenCanvas={() => navigate(`/etl/manage?processGroupId=${encodeURIComponent(completionSummary.processGroupId)}`)}
          onOpenDag={() => navigate("/airflow/manage")}
        />
      ) : (
        <Collapse
          accordion
          activeKey={activeStep}
          onChange={(key) => {
            const requested = Array.isArray(key) ? key[0] : key;
            if (requested && isUnlocked(requested as WizardStepId)) {
              setActiveStep(requested as WizardStepId);
            }
          }}
          items={[
            {
              key: "type",
              label: <StepLabel step="type" title="유형선택" completed={completedSteps.includes("type")} unlocked />,
              children: content.type,
            },
            {
              key: "basic",
              collapsible: isUnlocked("basic") ? undefined : "disabled",
              label: <StepLabel step="basic" title="기본 정보" completed={completedSteps.includes("basic")} unlocked={isUnlocked("basic")} />,
              children: content.basic,
            },
            {
              key: "connection",
              collapsible: isUnlocked("connection") ? undefined : "disabled",
              label: <StepLabel step="connection" title="연결" completed={completedSteps.includes("connection")} unlocked={isUnlocked("connection")} />,
              children: (
                <>
                  {content.connection}
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}>
                    <Button onClick={() => setActiveStep("basic")}>이전</Button>
                    <Button
                      type="primary"
                      disabled={!canCompleteConnection}
                      onClick={() => completeAndOpen("connection", "query")}
                    >
                      다음: 적재쿼리
                    </Button>
                  </div>
                </>
              ),
            },
            {
              key: "query",
              collapsible: isUnlocked("query") ? undefined : "disabled",
              label: <StepLabel step="query" title="적재쿼리" completed={completedSteps.includes("query")} unlocked={isUnlocked("query")} />,
              children: (
                <>
                  {content.query}
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}>
                    <Button onClick={() => setActiveStep("connection")}>이전</Button>
                    <Button
                      type="primary"
                      disabled={!isFileLoad && (loadMode === "INSERT" || loadMode === "UPSERT") && !loadSql.trim()}
                      onClick={completeQueryStep}
                    >
                      다음: 대상
                    </Button>
                  </div>
                </>
              ),
            },
            {
              key: "target",
              collapsible: isUnlocked("target") ? undefined : "disabled",
              label: <StepLabel step="target" title="대상" completed={completedSteps.includes("target")} unlocked={isUnlocked("target")} />,
              children: (
                <>
                  {content.target}
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}>
                    <Button onClick={() => setActiveStep("query")}>이전</Button>
                    <Button
                      type="primary"
                      loading={isCreating}
                      disabled={!isFileLoad && (
                        (loadMode === "INSERT" || loadMode === "UPSERT") && !loadSql.trim()
                      )}
                      onClick={completeWizard}
                    >
                      완료
                    </Button>
                  </div>
                </>
              ),
            },
          ]}
        />
      )}
    </div>
  );
}
