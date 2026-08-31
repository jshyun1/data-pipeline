package com.company.pipeline.jobcatalog;

import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.dto.EtlJobDetailResponse;
import com.company.pipeline.jobcatalog.dto.EtlJobResponse;
import com.company.pipeline.jobcatalog.dto.EtlJobRunResponse;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 잡 카탈로그 조회 API.
 *
 * <p>읽기 전용이다 - 원본은 NiFi 캔버스이고 이 테이블은 사본이라, 여기서 수정을 받으면
 * 다음 동기화에 덮어써져 사용자를 속이게 된다. 값 수정은 파라미터 푸시를 붙이는 단계에서
 * 별도 엔드포인트로 연다.
 */
@RestController
@RequestMapping("/api/etl/jobs")
@RequirePermission(system = SystemCode.NIFI)
public class EtlJobController {

    private final EtlJobRepository jobRepository;
    private final EtlJobStepRepository stepRepository;
    private final EtlJobLinkRepository linkRepository;
    private final EtlJobParamRepository paramRepository;
    private final EtlJobRunRepository runRepository;
    private final NifiJobMirrorService mirrorService;
    private final com.company.pipeline.nifi.NifiProcessGroupMetadataRepository pgRepository;

    public EtlJobController(EtlJobRepository jobRepository,
                            EtlJobStepRepository stepRepository,
                            EtlJobLinkRepository linkRepository,
                            EtlJobParamRepository paramRepository,
                            EtlJobRunRepository runRepository,
                            NifiJobMirrorService mirrorService,
                            com.company.pipeline.nifi.NifiProcessGroupMetadataRepository pgRepository) {
        this.jobRepository = jobRepository;
        this.stepRepository = stepRepository;
        this.linkRepository = linkRepository;
        this.paramRepository = paramRepository;
        this.runRepository = runRepository;
        this.mirrorService = mirrorService;
        this.pgRepository = pgRepository;
    }

    @GetMapping
    public ApiResponse<List<EtlJobResponse>> list() {
        // 체인을 자식 PG로 나누면 잡 이름이 겹친다(DW/DZ 아래 COM001M 등). 부모 그룹은
        // 프로세서가 없어 잡 목록에서 빠지므로, 이름을 미러 테이블에서 따로 채워준다.
        java.util.Map<String, String> groupNames = pgRepository.findAll().stream()
                .collect(java.util.stream.Collectors.toMap(
                        com.company.pipeline.nifi.NifiProcessGroupMetadata::getProcessGroupId,
                        com.company.pipeline.nifi.NifiProcessGroupMetadata::getProcessGroupName,
                        (a, b) -> a));
        return ApiResponse.success(jobRepository.findByDeletedAtIsNullOrderByJobNameAsc().stream()
                .map(job -> EtlJobResponse.from(job, groupNames.get(job.getParentPgId())))
                .toList());
    }

    @GetMapping("/{id}")
    public ApiResponse<EtlJobDetailResponse> get(@PathVariable Long id) {
        EtlJob job = jobRepository.findById(id)
                .orElseThrow(() -> new BusinessException(ErrorCode.PIPELINE_NOT_FOUND,
                        "잡을 찾을 수 없습니다. id=" + id));
        return ApiResponse.success(EtlJobDetailResponse.of(
                job,
                stepRepository.findByJobIdAndDeletedAtIsNullOrderByStepNameAsc(job.getId()),
                linkRepository.findByJobIdAndDeletedAtIsNull(job.getId()),
                paramRepository.findByJobIdAndDeletedAtIsNull(job.getId())));
    }

    /** 이 잡의 최근 실행 이력(최대 20건). 진행 중인 실행은 endedAt이 비어 있다. */
    @GetMapping("/{id}/runs")
    public ApiResponse<List<EtlJobRunResponse>> runs(@PathVariable Long id) {
        return ApiResponse.success(runRepository.findTop20ByJobIdOrderByStartedAtDesc(id).stream()
                .map(EtlJobRunResponse::from)
                .toList());
    }

    /** 5분 주기를 기다리지 않고 즉시 NiFi에서 다시 읽어온다. */
    @PostMapping("/sync")
    public ApiResponse<NifiJobMirrorService.SyncResult> sync() {
        return ApiResponse.success(mirrorService.sync());
    }
}
