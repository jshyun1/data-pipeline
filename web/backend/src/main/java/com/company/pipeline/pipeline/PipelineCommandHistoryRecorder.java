package com.company.pipeline.pipeline;

import com.company.pipeline.user.security.RequestActorResolver;
import java.time.LocalDateTime;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * 별도 빈으로 분리해둔 이유: PipelineDeployService.deploy() 안에서 실패 시 이걸
 * 같은 클래스의 메서드로 self-invocation하면 REQUIRES_NEW가 Spring 프록시를
 * 안 거쳐서 무시되고 바깥 트랜잭션 롤백에 같이 휩쓸려버린다. 별도 빈을 통해
 * 호출해야 진짜 독립 트랜잭션으로 커밋된다.
 */
@Component
public class PipelineCommandHistoryRecorder {

    /** 스케줄러가 스스로 남기는 기록 - 요청자를 채우지 않는다. */
    static final String RUNTIME_MONITOR = "RUNTIME_MONITOR";

    private final PipelineCommandHistoryRepository pipelineCommandHistoryRepository;
    private final RequestActorResolver actorResolver;

    public PipelineCommandHistoryRecorder(PipelineCommandHistoryRepository pipelineCommandHistoryRepository,
                                          RequestActorResolver actorResolver) {
        this.pipelineCommandHistoryRepository = pipelineCommandHistoryRepository;
        this.actorResolver = actorResolver;
    }

    public void record(Long pipelineId, String command, String result, String message) {
        record(pipelineId, command, result, message, null);
    }

    /**
     * @param requestedBy 사람이 누른 동작이면 그 사용자. 되돌릴 수 없는 동작(멈춘 건 건너뛰기 등)은 «누가» 가 남아야 한다.
     *                    null 이면 지금 변경 요청의 주체(로그인 사용자 · «airflow:DAG»)로 채운다(2026-10-13) - 예전에는 준비 · 시작 ·
     *                    정지 · 재시작 이력이 전부 요청자 없이 남았다. 스케줄러 · 감시가 스스로 남기는 기록(요청 밖, 자동 감시)은 null 로 둔다
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(Long pipelineId, String command, String result, String message, String requestedBy) {
        PipelineCommandHistory history = new PipelineCommandHistory();
        if (requestedBy == null && !RUNTIME_MONITOR.equals(command)) {
            requestedBy = actorResolver.currentForChange().orElse(null);
        }
        history.setRequestedBy(requestedBy);
        history.setPipelineId(pipelineId);
        history.setCommand(command);
        history.setResult(result);
        history.setMessage(message);
        history.setRequestedAt(LocalDateTime.now());
        history.setCompletedAt(LocalDateTime.now());
        pipelineCommandHistoryRepository.save(history);
    }
}
