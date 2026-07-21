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

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(Long pipelineId, String command, String result, String message) {
        PipelineCommandHistory history = new PipelineCommandHistory();
        history.setPipelineId(pipelineId);
        history.setCommand(command);
        history.setResult(result);
        history.setMessage(message);
        history.setRequestedAt(LocalDateTime.now());
        history.setCompletedAt(LocalDateTime.now());
        pipelineCommandHistoryRepository.save(history);
    }
}
