package com.company.pipeline.workflow.dto;

import java.util.List;

/**
 * 캔버스 검증 결과.
 *
 * <p>오류(errors)가 하나라도 있으면 게시할 수 없다. 경고(warnings)는 게시를 막지 않지만
 * 화면에 띄워 운영자가 알고 넘어가게 한다 - 예를 들어 같은 job을 두 워크플로우가 참조하는 건
 * 의도일 수도 있어서 차단하지 않는다.
 */
public record WorkflowValidationResult(
        boolean valid,
        List<Issue> errors,
        List<Issue> warnings) {

    /** {@code nodeKey}가 있으면 화면이 그 노드를 빨갛게 표시한다. */
    public record Issue(String code, String message, String nodeKey) {

        public static Issue of(String code, String message) {
            return new Issue(code, message, null);
        }

        public static Issue at(String code, String message, String nodeKey) {
            return new Issue(code, message, nodeKey);
        }
    }

    public static WorkflowValidationResult of(List<Issue> errors, List<Issue> warnings) {
        return new WorkflowValidationResult(errors.isEmpty(), errors, warnings);
    }
}
