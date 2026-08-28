package com.company.pipeline.jobcatalog.dto;

import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobLink;
import com.company.pipeline.jobcatalog.EtlJobParam;
import com.company.pipeline.jobcatalog.EtlJobStep;
import java.util.List;

/**
 * 잡 상세 - 스텝/연결선/파라미터까지. 화면에서 흐름을 다시 그릴 수 있도록 좌표를 함께 준다.
 */
public record EtlJobDetailResponse(
        EtlJobResponse job,
        List<StepView> steps,
        List<LinkView> links,
        List<ParamView> params) {

    public record StepView(
            Long id,
            String nifiProcessorId,
            String stepName,
            String stepType,
            String schedulingStrategy,
            String schedulingPeriod,
            String sqlText,
            String targetTable,
            String statementType,
            String updateKeys,
            String dbcpServiceId,
            String propsJson,
            String validationStatus,
            String runStatus,
            Double xPos,
            Double yPos) {

        public static StepView from(EtlJobStep step) {
            return new StepView(step.getId(), step.getNifiProcessorId(), step.getStepName(),
                    step.getStepType(), step.getSchedulingStrategy(), step.getSchedulingPeriod(),
                    step.getSqlText(), step.getTargetTable(), step.getStatementType(),
                    step.getUpdateKeys(), step.getDbcpServiceId(), step.getPropsJson(),
                    step.getValidationStatus(), step.getRunStatus(),
                    step.getXPos(), step.getYPos());
        }
    }

    public record LinkView(
            String fromComponentId,
            String fromName,
            String toComponentId,
            String toName,
            String relationships) {

        public static LinkView from(EtlJobLink link) {
            return new LinkView(link.getFromComponentId(), link.getFromName(),
                    link.getToComponentId(), link.getToName(), link.getRelationships());
        }
    }

    /** sensitive 파라미터는 NiFi가 값을 주지 않으므로 항상 값이 비어 있다. */
    public record ParamView(String paramName, String paramValue, boolean sensitive,
                            String description, String syncDirection) {

        public static ParamView from(EtlJobParam param) {
            return new ParamView(param.getParamName(), param.getParamValue(), param.isSensitive(),
                    param.getDescription(), param.getSyncDirection());
        }
    }

    public static EtlJobDetailResponse of(EtlJob job, List<EtlJobStep> steps,
                                          List<EtlJobLink> links, List<EtlJobParam> params) {
        return new EtlJobDetailResponse(
                EtlJobResponse.from(job),
                steps.stream().map(StepView::from).toList(),
                links.stream().map(LinkView::from).toList(),
                params.stream().map(ParamView::from).toList());
    }
}
