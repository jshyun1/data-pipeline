package com.company.pipeline.jobcatalog;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlJobRunRepository extends JpaRepository<EtlJobRun, Long> {

    Optional<EtlJobRun> findByJobIdAndEndedAtIsNull(Long jobId);

    List<EtlJobRun> findByEndedAtIsNull();

    List<EtlJobRun> findTop20ByJobIdOrderByStartedAtDesc(Long jobId);

    /** 가장 최근에 닫힌 실행(관측기가 «방금 콜백으로 닫힌 실행»에 꼬리 활동을 붙일 때 쓴다). */
    Optional<EtlJobRun> findFirstByJobIdAndEndedAtIsNotNullOrderByEndedAtDesc(Long jobId);
}
