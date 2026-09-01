import { CheckCircleOutlined, CheckOutlined, LockOutlined, UploadOutlined } from "@ant-design/icons";
import { Alert, Button, Card, Collapse, message, Radio, Select, Space, Tag } from "antd";
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
import type { ConnectionResponse } from "../types/connection";
import { useNavigate } from "react-router-dom";

const WIZARD_STEPS = [
  { id: "type", label: "유형 선택" },
  { id: "basic", label: "기본 정보" },
  { id: "connection", label: "연결" },
  { id: "target", label: "대상" },
] as const;

type WizardStepId = (typeof WIZARD_STEPS)[number]["id"];
type EtlType = "DB_TO_DB" | "FILE_LOAD" | "HTTP" | "UNSTRUCTURED" | "MANUAL";
type LoadMode = "INSERT" | "TRUNCATE" | "UPSERT";
type FileExtension = "csv" | "excel" | "log" | "json";
type NifiDatabaseType = "Generic" | "Oracle 12+" | "PostgreSQL" | "MySQL" | "MS SQL 2012+";
type ConnectionDbType = ConnectionResponse["dbType"];

const STEP_NUMBER: Record<WizardStepId, number> = {
  type: 1,
  basic: 2,
  connection: 3,
  target: 4,
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
  if (!id || !name) {
    return null;
  }
  return {
    id,
    name,
    displayName: stripCdcControllerServicePrefix(name),
  };
}

function normalizeName(value: string) {
  return value.trim().toLowerCase();
}

function stripCdcControllerServicePrefix(value: string) {
  return value.trim().replace(/^cdc-\d+-/i, "");
}

function normalizeControllerServiceName(value: string) {
  return normalizeName(stripCdcControllerServicePrefix(value));
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

function defaultLoadSql(
  loadMode: LoadMode,
  sourceSchema: string,
  sourceTable: string,
  databaseType: NifiDatabaseType,
) {
  const schema = sourceSchema.trim() || "소스스키마";
  const table = sourceTable.trim() || "소스테이블";
  const baseSql = `SELECT *\nFROM ${schema}.${table}`;
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

function columnOptions(columns: ColumnMetadataResponse[]) {
  return columns.map((column) => ({ label: column.name, value: column.name }));
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
  onSourceColumnsChange: (columns: ColumnMetadataResponse[]) => void;
  onTargetColumnsChange: (columns: ColumnMetadataResponse[]) => void;
}) {
  const [services, setServices] = useState<ControllerServiceOption[]>([]);
  const [connections, setConnections] = useState<ConnectionResponse[]>([]);
  const [connectionsLoaded, setConnectionsLoaded] = useState(false);
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
            .filter((service): service is ControllerServiceOption => service !== null),
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
          setConnectionsLoaded(true);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setConnections([]);
          setConnectionsLoaded(true);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const connectionMatchesControllerService = (connection: ConnectionResponse, service: ControllerServiceOption) =>
    normalizeControllerServiceName(connection.name) === normalizeControllerServiceName(service.name);

  const matchedServices = services.filter((service) =>
    connections.some((connection) => connectionMatchesControllerService(connection, service)),
  );

  const servicePlaceholder = serviceError
    ? "Controller Service 조회 실패"
    : !connectionsLoaded || services.length === 0
      ? "Controller Service 조회 중"
      : matchedServices.length > 0
        ? "Controller Service를 선택하세요"
        : "연결정보와 일치하는 Controller Service가 없습니다";

  const connectionByServiceId = (serviceId: string) => {
    const service = matchedServices.find((entry) => entry.id === serviceId);
    if (!service) {
      return null;
    }
    return connections.find((connection) => connectionMatchesControllerService(connection, service)) ?? null;
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

    if (!sourceEnabled || !sourceConnection || !value.sourceSchema.trim() || !value.sourceTable.trim()) {
      return () => {
        cancelled = true;
      };
    }

    listConnectionColumns(sourceConnection.id, value.sourceSchema.trim(), value.sourceTable.trim())
      .then((columns) => {
        if (!cancelled) {
          onSourceColumnsChange(columns);
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
              disabled={!connectionsLoaded || matchedServices.length === 0 || !!serviceError}
              onChange={(event) => handleSourceServiceChange(event.target.value)}
            >
              <option value="" disabled>
                {servicePlaceholder}
              </option>
              {matchedServices.map((service) => (
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
          disabled={!connectionsLoaded || matchedServices.length === 0 || !!serviceError}
          onChange={(event) => handleTargetServiceChange(event.target.value)}
        >
          <option value="" disabled>
            {servicePlaceholder}
          </option>
          {matchedServices.map((service) => (
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
              showSearch
              value={value.sourceTable || undefined}
              placeholder={sourceTablePlaceholder}
              disabled={!value.sourceSchema.trim() || sourceTableLoading || !!sourceTableError || sourceTables.length === 0}
              loading={sourceTableLoading}
              options={sourceTableOptions}
              filterOption={matchesSelectOption}
              onChange={(nextTable) => updateValue({ sourceTable: nextTable })}
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

function TargetStep({
  loadMode,
  truncateSql,
  loadSql,
  primaryKeys,
  targetColumns,
  showTruncateSql,
  onLoadModeChange,
  onTruncateSqlChange,
  onLoadSqlChange,
  onPrimaryKeysChange,
  onShowTruncateSqlChange,
}: {
  loadMode: LoadMode;
  onLoadModeChange: (loadMode: LoadMode) => void;
  truncateSql: string;
  loadSql: string;
  primaryKeys: string;
  targetColumns: ColumnMetadataResponse[];
  showTruncateSql: boolean;
  onTruncateSqlChange: (truncateSql: string) => void;
  onLoadSqlChange: (loadSql: string) => void;
  onPrimaryKeysChange: (primaryKeys: string) => void;
  onShowTruncateSqlChange: (show: boolean) => void;
}) {
  const targetColumnOptions = columnOptions(targetColumns);

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

      {(loadMode === "INSERT" || loadMode === "UPSERT") ? (
        <div className="etl-change-key-column">
          <label>적재로직 SQL문</label>
          <textarea
            value={loadSql}
            onChange={(event) => onLoadSqlChange(event.target.value)}
            placeholder={"예: SELECT *\nFROM source_schema.source_table"}
          />
        </div>
      ) : null}

      {loadMode === "UPSERT" ? (
        <div className="etl-change-key-column">
          <label>Target Primary Keys</label>
          <Select
            mode="multiple"
            showSearch
            allowClear
            value={commaSeparatedValues(primaryKeys)}
            disabled={targetColumns.length === 0}
            options={targetColumnOptions}
            filterOption={matchesSelectOption}
            placeholder="예: ID 또는 ID,SEQ"
            onChange={(nextColumns) => onPrimaryKeysChange(nextColumns.join(","))}
          />
        </div>
      ) : null}
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
  const [primaryKeys, setPrimaryKeys] = useState("");
  const [, setSourceColumns] = useState<ColumnMetadataResponse[]>([]);
  const [targetColumns, setTargetColumns] = useState<ColumnMetadataResponse[]>([]);

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
    if (isFileLoad || loadMode === "TRUNCATE") {
      return;
    }
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
    isFileLoad,
    loadMode,
  ]);

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
    if (!isFileLoad && loadMode === "UPSERT" && !primaryKeys.trim()) {
      message.warning("UPSERT 적재 방식은 Target Primary Keys를 입력해야 합니다.");
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
          loadSql: (loadMode === "INSERT" || loadMode === "UPSERT") ? loadSql.trim() : undefined,
          primaryKeys: loadMode === "UPSERT" ? primaryKeys.trim() : undefined,
        });
      }
      const processGroupId = createdGroup.id;
      if (!processGroupId) {
        throw new Error("missing process group id");
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
            setPrimaryKeys("");
          }
          invalidateFrom("connection");
        }}
        onSourceColumnsChange={setSourceColumns}
        onTargetColumnsChange={setTargetColumns}
      />
    ),
    target: isFileLoad ? (
      <FileTargetStep columns={[]} />
    ) : (
      <TargetStep
        loadMode={loadMode}
        truncateSql={truncateSql}
        loadSql={loadSql}
        primaryKeys={primaryKeys}
        targetColumns={targetColumns}
        showTruncateSql={showTruncateSql}
        onLoadModeChange={(nextLoadMode) => {
          setLoadMode(nextLoadMode);
          if (nextLoadMode !== "TRUNCATE") {
            setShowTruncateSql(false);
          }
        }}
        onTruncateSqlChange={setTruncateSql}
        onLoadSqlChange={setLoadSql}
        onPrimaryKeysChange={setPrimaryKeys}
        onShowTruncateSqlChange={setShowTruncateSql}
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
                      onClick={() => completeAndOpen("connection", "target")}
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
                    <Button onClick={() => setActiveStep("connection")}>이전</Button>
                    <Button
                      type="primary"
                      loading={isCreating}
                      disabled={!isFileLoad && (
                        ((loadMode === "INSERT" || loadMode === "UPSERT") && !loadSql.trim()) ||
                        (loadMode === "UPSERT" && !primaryKeys.trim())
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
