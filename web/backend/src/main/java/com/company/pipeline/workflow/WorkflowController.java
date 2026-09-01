package com.company.pipeline.workflow;

import com.company.pipeline.authz.AccessBits;
import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.user.AppUser;
import com.company.pipeline.workflow.dto.WorkflowRequests.CreateWorkflow;
import com.company.pipeline.workflow.dto.WorkflowRequests.SaveGraph;
import com.company.pipeline.workflow.dto.WorkflowRequests.UpdateWorkflow;
import com.company.pipeline.workflow.dto.WorkflowResponses.WorkflowDetail;
import com.company.pipeline.workflow.dto.WorkflowResponses.WorkflowSummary;
import com.company.pipeline.workflow.dto.WorkflowValidationResult;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 워크플로우 캔버스 API (설계서 2026-08-26 §6 Part C).
 *
 * <p>ETL 자산이므로 NIFI 권한으로 게이트한다. 인가 스위치를 켜도 비어 있는 컨트롤러가
 * 남지 않도록 <b>클래스 레벨에서 처음부터</b> 애노테이션을 단다(검토 리포트 S-1~S-3의 전철).
 * 조회는 READ, 변경은 각 메서드에서 WRITE로 좁힌다.
 */
@RestController
@RequestMapping("/api/etl/workflows")
@RequirePermission(system = SystemCode.NIFI)
public class WorkflowController {

    private final WorkflowService workflowService;
    private final WorkflowPublishService publishService;

    public WorkflowController(WorkflowService workflowService,
                              WorkflowPublishService publishService) {
        this.workflowService = workflowService;
        this.publishService = publishService;
    }

    @GetMapping
    public ApiResponse<List<WorkflowSummary>> list() {
        return ApiResponse.success(workflowService.list());
    }

    @GetMapping("/{id}")
    public ApiResponse<WorkflowDetail> get(@PathVariable Long id) {
        return ApiResponse.success(workflowService.get(id));
    }

    @PostMapping
    @RequirePermission(system = SystemCode.NIFI, bits = AccessBits.WRITE)
    public ApiResponse<WorkflowSummary> create(@Valid @RequestBody CreateWorkflow request) {
        return ApiResponse.success(workflowService.create(request));
    }

    @PutMapping("/{id}")
    @RequirePermission(system = SystemCode.NIFI, bits = AccessBits.WRITE)
    public ApiResponse<WorkflowSummary> update(@PathVariable Long id,
                                               @Valid @RequestBody UpdateWorkflow request) {
        return ApiResponse.success(workflowService.update(id, request));
    }

    /** 캔버스 저장(draft). 게시 전까지 DAG는 바뀌지 않는다. */
    @PutMapping("/{id}/graph")
    @RequirePermission(system = SystemCode.NIFI, bits = AccessBits.WRITE)
    public ApiResponse<WorkflowDetail> saveGraph(@PathVariable Long id,
                                                 @Valid @RequestBody SaveGraph request) {
        return ApiResponse.success(workflowService.saveGraph(id, request));
    }

    /** 게시하지 않고 검증만 한다. 화면의 [검증] 버튼. */
    @PostMapping("/{id}/validate")
    public ApiResponse<WorkflowValidationResult> validate(@PathVariable Long id) {
        return ApiResponse.success(publishService.validate(id));
    }

    /**
     * 검증 → 컴파일 → Airflow에 게시. 여기까지 와야 DAG가 생긴다.
     *
     * <p>검증 오류가 있으면 게시하지 않고 결과를 그대로 돌려준다(화면이 문제 노드를 표시).
     */
    // 게시는 «설계»가 아니라 «운영 반영»이다 - Airflow Variable 을 갱신해 DAG 를 만들고
    // 스케줄을 켠다. 그래서 그래프 편집(NIFI)과 달리 AIRFLOW 쓰기를 요구한다.
    @PostMapping("/{id}/publish")
    @RequirePermission(system = SystemCode.AIRFLOW, bits = AccessBits.WRITE)
    public ApiResponse<WorkflowValidationResult> publish(@PathVariable Long id,
                                                        @AuthenticationPrincipal AppUser actor) {
        return ApiResponse.success(
                publishService.publish(id, actor == null ? "system" : actor.getUserId()));
    }

    /** 게시를 내린다. 다음 파싱 주기에 DAG가 사라진다. */
    // 게시 취소도 DAG 를 없애는 운영 동작이라 게시와 같은 권한을 요구한다.
    @PostMapping("/{id}/unpublish")
    @RequirePermission(system = SystemCode.AIRFLOW, bits = AccessBits.WRITE)
    public ApiResponse<Void> unpublish(@PathVariable Long id) {
        publishService.unpublish(id);
        return ApiResponse.success(null);
    }

    @DeleteMapping("/{id}")
    @RequirePermission(system = SystemCode.NIFI, bits = AccessBits.WRITE)
    public ApiResponse<Void> delete(@PathVariable Long id) {
        workflowService.delete(id);
        return ApiResponse.success(null);
    }
}
