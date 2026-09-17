package com.company.pipeline.pipeline;

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

    private final PipelineCommandHistoryRepository pipelineCommandHistoryRepository;

    public PipelineCommandHistoryRecorder(PipelineCommandHistoryRepository pipelineCommandHistoryRepository) {
        this.pipelineCommandHistoryRepository = pipelineCommandHistoryRepository;
    }

    public void record(Long pipelineId, String command, String result, String message) {
        record(pipelineId, command, result, message, null);
    }

    /**
     * @param requestedBy 사람이 누른 동작이면 그 사용자. 되돌릴 수 없는 동작(멈춘 건 건너뛰기 등)은 «누가» 가 남아야 한다.
     *                    스케줄러 · 감시가 스스로 남기는 기록은 null 이다
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(Long pipelineId, String command, String result, String message, String requestedBy) {
        PipelineCommandHistory history = new PipelineCommandHistory();
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
