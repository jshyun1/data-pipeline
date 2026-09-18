import type { ReactNode } from "react";
import { CheckOutlined, LockOutlined } from "@ant-design/icons";
import { WfIcon } from "./WorkflowPageHeader";

/** 단계 하나의 상태. 지금 여는 단계 · 끝낸 단계 · 열 수 있는 단계 · 앞 단계를 끝내야 열리는 단계. */
export type WizardStepState = "active" | "done" | "open" | "locked";

/**
 * 생성 위저드 윗부분 카드 — 디자인 작업물 cdc-create.html: 왼쪽 모드 전환(CDC 생성 · 로그파일 생성) ··· 오른쪽 안내,
 * 아래 가로 단계 표시(번호 원 + 잇는 선). 단계를 누르면 그 단계로 간다 - 아래 접이식 단계 머리를 누를 때와 같은 규칙
 * (열린 단계만)을 부르는 쪽이 onStep 에서 지킨다.
 */
export function WizardTop({
  modeSwitch,
  noticeTitle,
  noticeDesc,
  steps,
  stateOf,
  onStep,
}: {
  modeSwitch?: ReactNode;
  noticeTitle: string;
  noticeDesc?: string;
  steps: Array<{ key: string; title: string }>;
  stateOf: (key: string) => WizardStepState;
  onStep: (key: string) => void;
}) {
  return (
    <section className="std-card std-wizard-top">
      <div className="std-wizard-top-bar">
        {modeSwitch}
        <span className="std-wizard-notice">
          <WfIcon name="info" size={14} />
          <b>{noticeTitle}</b>
          {noticeDesc ? <small>· {noticeDesc}</small> : null}
        </span>
      </div>
      <ol className="std-stepper">
        {steps.map((step, index) => {
          const state = stateOf(step.key);
          return (
            <li key={step.key} className={`std-stepper-item is-${state}`}>
              <button type="button" disabled={state === "locked"} onClick={() => onStep(step.key)}
                      aria-current={state === "active" ? "step" : undefined}>
                <span className="std-stepper-dot">{state === "done" ? <CheckOutlined /> : index + 1}</span>
                <span className="std-stepper-label">{step.title}</span>
              </button>
            </li>
          );
        })}
      </ol>
    </section>
  );
}

/** 접이식 단계 머리: 번호 원 + 제목. */
export function WizardStepTitle({ number, title, state }: { number: number; title: string; state: WizardStepState }) {
  return (
    <span className={`std-step-title is-${state}`}>
      <span className="std-step-no">{state === "done" ? <CheckOutlined /> : number}</span>
      {title}
    </span>
  );
}

/** 접이식 단계 오른쪽 상태 알약(작성 중 · 완료 · 잠김). */
export function WizardStepStatus({ state }: { state: WizardStepState }) {
  if (state === "active") return <span className="std-step-status is-active"><i />작성 중</span>;
  if (state === "done") return <span className="std-step-status is-done"><CheckOutlined />완료</span>;
  if (state === "locked") return <span className="std-step-status is-locked"><LockOutlined />잠김</span>;
  return <span className="std-step-status">대기</span>;
}
