import { CheckOutlined } from "@ant-design/icons";
import { message } from "antd";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import { listConnections, listConnectionTables } from "../api/connections";
import {
  createInitialDbToDbFlow,
  getNifiProcessGroupTree,
  listRootNifiControllerServices,
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
type LoadMode = "INSERT" | "TRUNCATE" | "UPSERT";
type NifiDatabaseType = "Generic" | "Oracle 12+" | "PostgreSQL" | "MySQL" | "MS SQL 2012+";

const ACTION_STEPS: Array<{ id: WizardStepId; label: string }> = [
  { id: "type", label: "유형 선택" },
  { id: "basic", label: "기본 정보" },
  { id: "connection", label: "연결" },
  { id: "target", label: "대상" },
];

const TYPE_OPTIONS = [
  {
    title: "DB -> DB 적재",
    description: "원천 DB에서 읽어 대상에 적재",
  },
  {
    title: "파일 적재",
    description: "서버 폴더의 파일을 읽어 적재",
  },
  {
    title: "HTTP 수집",
    description: "HTTP로 받은 데이터를 적재",
  },
  {
    title: "비정형 전송",
    description: "이미지, 문서를 받아 저장",
  },
  {
    title: "직접 구성",
    description: "빈 그룹만 생성",
  },
];

const DATABASE_TYPE_OPTIONS: NifiDatabaseType[] = [
  "PostgreSQL",
  "Oracle 12+",
  "MySQL",
  "MS SQL 2012+",
  "Generic",
];

interface ProcessGroupOption {
  id: string;
  label: string;
}

interface ControllerServiceOption {
  id: string;
  name: string;
  properties: Record<string, string | null | undefined>;
}

interface BasicInfo {
  jobName: string;
  parentGroupId: string;
  parentGroupPath: string;
  comments: string;
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
    properties: service.component?.properties ?? {},
  };
}

function normalizeName(value: string) {
  return value.trim().toLowerCase();
}

function normalizeJdbcUrl(value: string) {
  return value.trim().replace(/\s+/g, "").toLowerCase();
}

function propertyValue(properties: Record<string, string | null | undefined>, keys: string[]) {
  const entries = Object.entries(properties);
  for (const key of keys) {
    const matched = entries.find(([entryKey]) => normalizeName(entryKey) === normalizeName(key));
    if (matched?.[1]?.trim()) {
      return matched[1].trim();
    }
  }
  return null;
}

function connectionJdbcUrl(connection: ConnectionResponse) {
  if (connection.dbType === "POSTGRESQL" && connection.databaseName) {
    return `jdbc:postgresql://${connection.host}:${connection.port}/${connection.databaseName}`;
  }
  if (connection.dbType === "ORACLE" && connection.serviceName) {
    return `jdbc:oracle:thin:@${connection.host}:${connection.port}/${connection.serviceName}`;
  }
  return null;
}

async function tableExists(connection: ConnectionResponse, schemaName: string, tableName: string) {
  const schema = schemaName.trim();
  const table = tableName.trim();
  if (!schema || !table) {
    throw new Error("스키마와 테이블을 모두 입력해야 합니다.");
  }

  const tables = await listConnectionTables(connection.id, schema);
  return tables.some((candidate) => normalizeName(candidate) === normalizeName(table));
}

function TypeStep() {
  return (
    <div className="etl-wizard-type-grid">
      {TYPE_OPTIONS.map((option, index) => (
        <button key={option.title} type="button" className={index === 0 ? "etl-type-card selected" : "etl-type-card"}>
          <strong>{option.title}</strong>
          <span>{option.description}</span>
        </button>
      ))}
    </div>
  );
}

function BasicStep({
  value,
  onChange,
  onComplete,
}: {
  value: BasicInfo;
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
  const canComplete = value.jobName.trim().length > 0 && value.parentGroupId.length > 0;

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
      </div>

      <div className="etl-step-complete-actions">
        <button type="button" disabled={!canComplete} onClick={onComplete}>
          완료
        </button>
      </div>
    </>
  );
}

function ConnectionStep({
  value,
  onChange,
}: {
  value: ConnectionInfo;
  onChange: (nextValue: Partial<ConnectionInfo>) => void;
}) {
  const [services, setServices] = useState<ControllerServiceOption[]>([]);
  const [connections, setConnections] = useState<ConnectionResponse[]>([]);
  const [serviceError, setServiceError] = useState<string | null>(null);
  const [isTesting, setIsTesting] = useState(false);
  const [testResult, setTestResult] = useState<string | null>(null);

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
      : "Controller Service 조회 중";

  const connectionByServiceId = (serviceId: string) => {
    const service = services.find((entry) => entry.id === serviceId);
    if (!service) {
      return null;
    }
    const serviceUrl = propertyValue(service.properties, [
      "Database Connection URL",
      "Connection URL",
      "JDBC URL",
      "database.connection.url",
    ]);
    const serviceUser = propertyValue(service.properties, [
      "Database User",
      "Database User Name",
      "User",
      "Username",
      "database.user",
    ]);

    return connections.find((connection) => {
      if (connection.nifiControllerServiceId === service.id) {
        return true;
      }
      if (connection.nifiControllerServiceName && normalizeName(connection.nifiControllerServiceName) === normalizeName(service.name)) {
        return true;
      }
      if (normalizeName(connection.name) === normalizeName(service.name)) {
        return true;
      }

      const appUrl = connectionJdbcUrl(connection);
      const urlMatches = !!serviceUrl && !!appUrl && normalizeJdbcUrl(serviceUrl) === normalizeJdbcUrl(appUrl);
      const userMatches = !serviceUser || normalizeName(connection.username) === normalizeName(serviceUser);
      return urlMatches && userMatches;
    }) ?? null;
  };

  const testTables = async () => {
    setIsTesting(true);
    setTestResult(null);
    try {
      const sourceConnection = connectionByServiceId(value.sourceServiceId);
      const targetConnection = connectionByServiceId(value.targetServiceId);
      if (!sourceConnection || !targetConnection) {
        throw new Error("선택한 Controller Service와 매칭되는 연결 정보를 찾을 수 없습니다. 연결 관리에 같은 DB 접속 정보를 등록해 주세요.");
      }

      const [sourceExists, targetExists] = await Promise.all([
        tableExists(sourceConnection, value.sourceSchema, value.sourceTable),
        tableExists(targetConnection, value.targetSchema, value.targetTable),
      ]);

      if (sourceExists && targetExists) {
        setTestResult("소스와 타깃 테이블을 모두 확인했습니다.");
      } else {
        setTestResult(
          [
            sourceExists ? null : "소스 테이블 없음",
            targetExists ? null : "타깃 테이블 없음",
          ].filter(Boolean).join(" / "),
        );
      }
    } catch (ex) {
      setTestResult(ex instanceof Error ? ex.message : "연결 테스트에 실패했습니다.");
    } finally {
      setIsTesting(false);
    }
  };

  return (
    <div className="etl-wizard-form-table compact">
      <div className="form-row two-column">
        <label>소스 연결</label>
        <select
          value={value.sourceServiceId}
          disabled={services.length === 0 || !!serviceError}
          onChange={(event) => onChange({ sourceServiceId: event.target.value })}
        >
          <option value="" disabled>
            {servicePlaceholder}
          </option>
          {services.map((service) => (
            <option key={service.id} value={service.id}>
              {service.name}
            </option>
          ))}
        </select>
        <label>타깃 연결</label>
        <select
          value={value.targetServiceId}
          disabled={services.length === 0 || !!serviceError}
          onChange={(event) => onChange({ targetServiceId: event.target.value })}
        >
          <option value="" disabled>
            {servicePlaceholder}
          </option>
          {services.map((service) => (
            <option key={service.id} value={service.id}>
              {service.name}
            </option>
          ))}
        </select>
      </div>
      <div className="form-row two-column">
        <label>소스 데이터베이스</label>
        <select
          value={value.sourceDatabaseType}
          onChange={(event) => onChange({ sourceDatabaseType: event.target.value as NifiDatabaseType })}
        >
          {DATABASE_TYPE_OPTIONS.map((databaseType) => (
            <option key={databaseType} value={databaseType}>
              {databaseType}
            </option>
          ))}
        </select>
        <label>타깃 데이터베이스</label>
        <select
          value={value.targetDatabaseType}
          onChange={(event) => onChange({ targetDatabaseType: event.target.value as NifiDatabaseType })}
        >
          {DATABASE_TYPE_OPTIONS.map((databaseType) => (
            <option key={databaseType} value={databaseType}>
              {databaseType}
            </option>
          ))}
        </select>
      </div>
      <div className="form-row two-column">
        <label>소스 스키마</label>
        <input
          value={value.sourceSchema}
          onChange={(event) => onChange({ sourceSchema: event.target.value })}
          placeholder="예: CSB"
        />
        <label>타깃 스키마</label>
        <input
          value={value.targetSchema}
          onChange={(event) => onChange({ targetSchema: event.target.value })}
          placeholder="예: public"
        />
      </div>
      <div className="form-row two-column">
        <label>소스 테이블</label>
        <input
          value={value.sourceTable}
          onChange={(event) => onChange({ sourceTable: event.target.value })}
          placeholder="예: TB_COM001M"
        />
        <label>타깃 테이블</label>
        <input
          value={value.targetTable}
          onChange={(event) => onChange({ targetTable: event.target.value })}
          placeholder="예: dz_com001m"
        />
      </div>
      <div className="form-row action-row">
        <div className="etl-connection-test">
          <button
            type="button"
            onClick={testTables}
            disabled={
              isTesting ||
              !value.sourceServiceId ||
              !value.targetServiceId ||
              !value.sourceSchema.trim() ||
              !value.targetSchema.trim() ||
              !value.sourceTable.trim() ||
              !value.targetTable.trim()
            }
          >
            {isTesting ? "확인 중" : "연결 테스트"}
          </button>
          {testResult ? <span>{testResult}</span> : null}
        </div>
      </div>
    </div>
  );
}

function TargetStep({
  loadMode,
  onLoadModeChange,
}: {
  loadMode: LoadMode;
  onLoadModeChange: (loadMode: LoadMode) => void;
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

      <div className="etl-preview-title">컬럼 매핑 미리보기</div>
      <table className="etl-preview-table">
        <thead>
          <tr>
            <th>소스 컬럼</th>
            <th>타깃 컬럼</th>
            <th>상태</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>CRTR_YY</td>
            <td>crtr_yy</td>
            <td>✓</td>
          </tr>
          <tr>
            <td>DCLR_YY</td>
            <td>dclr_yy</td>
            <td>✓</td>
          </tr>
        </tbody>
      </table>
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
              <dd>{flowLabel(summary.loadMode)}</dd>
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
  const [completed, setCompleted] = useState(false);
  const [isCreating, setIsCreating] = useState(false);
  const [completionSummary, setCompletionSummary] = useState<CompletionSummary | null>(null);
  const [basicInfo, setBasicInfo] = useState<BasicInfo>({
    jobName: "",
    parentGroupId: "",
    parentGroupPath: "",
    comments: "",
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

  const moveStep = (stepId: WizardStepId) => {
    if (completed) {
      return;
    }
    setCompleted(false);
    setActiveStep(stepId);
  };

  const selectActionStep = (stepId: WizardStepId) => {
    moveStep(stepId);
  };

  const completeWizard = async () => {
    if (loadMode !== "INSERT") {
      message.warning("현재 Initial 템플릿 복제는 INSERT 적재 방식만 지원합니다.");
      return;
    }
    if (!basicInfo.jobName.trim() || !basicInfo.parentGroupId) {
      message.warning("기본 정보의 작업명과 상위 그룹을 입력하세요.");
      moveStep("basic");
      return;
    }
    if (
      !connectionInfo.sourceServiceId ||
      !connectionInfo.sourceSchema.trim() ||
      !connectionInfo.sourceTable.trim() ||
      !connectionInfo.targetServiceId ||
      !connectionInfo.targetSchema.trim() ||
      !connectionInfo.targetTable.trim()
    ) {
      message.warning("연결 정보를 모두 입력하세요.");
      moveStep("connection");
      return;
    }

    setIsCreating(true);
    try {
      const createdGroup: NifiProcessGroupEntity = await createInitialDbToDbFlow({
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
        loadMode: "INSERT",
      });
      const processGroupId = createdGroup.id;
      if (!processGroupId) {
        throw new Error("missing process group id");
      }
      setCompletionSummary({
        processGroupId,
        processGroupPath: `${basicInfo.parentGroupPath || basicInfo.parentGroupId} / ${basicInfo.jobName.trim()}`,
        processorCount: createdGroup.processorCount ?? 0,
        loadMode,
      });
      message.success("NiFi Initial 그룹을 복제하고 설정을 반영했습니다.");
      setCompleted(true);
      setActiveStep("target");
    } catch {
      message.error("오류가 발생하였습니다. 관리자에게 문의 하십시오.");
    } finally {
      setIsCreating(false);
    }
  };

  const content = {
    type: <TypeStep />,
    basic: (
      <BasicStep
        value={basicInfo}
        onChange={(nextValue) => setBasicInfo((current) => ({ ...current, ...nextValue }))}
        onComplete={() => moveStep("connection")}
      />
    ),
    connection: (
      <ConnectionStep
        value={connectionInfo}
        onChange={(nextValue) => setConnectionInfo((current) => ({ ...current, ...nextValue }))}
      />
    ),
    target: <TargetStep loadMode={loadMode} onLoadModeChange={setLoadMode} />,
  } satisfies Record<WizardStepId, ReactNode>;

  return (
    <div className="etl-create-page">
      <header className="etl-wizard-titlebar">
        <h1>ETL 생성 마법사</h1>
      </header>

      <section className="etl-wizard-layout">
        <div className="etl-wizard-main">
          <nav className="etl-wizard-actions" aria-label="ETL 생성 단계">
            {ACTION_STEPS.map((step) => {
              const active = !completed && activeStep === step.id;
              return (
                <button
                  key={step.id}
                  type="button"
                  className={active ? "etl-wizard-action active" : "etl-wizard-action"}
                  disabled={completed}
                  onClick={() => selectActionStep(step.id)}
                >
                  {step.label}
                </button>
              );
            })}
          </nav>

          <div className="etl-wizard-content">
            {completed && completionSummary ? (
              <CompletionView
                summary={completionSummary}
                onOpenCanvas={() => navigate(`/etl/manage?processGroupId=${encodeURIComponent(completionSummary.processGroupId)}`)}
                onOpenDag={() => navigate("/airflow/manage")}
              />
            ) : (
              content[activeStep]
            )}
          </div>
        </div>

        {!completed && activeStep === "target" ? (
          <div className="etl-target-actions">
            <button type="button" className="etl-target-complete" disabled={isCreating} onClick={completeWizard}>
              {isCreating ? "생성 중" : "완료"}
            </button>
          </div>
        ) : null}
      </section>
    </div>
  );
}
