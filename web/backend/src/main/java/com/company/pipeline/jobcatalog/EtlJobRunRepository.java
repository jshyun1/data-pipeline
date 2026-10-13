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

    /** 워크플로우 실행 한 번(Airflow dag run)이 연 job 실행들 - 로그 화면이 진행 건수를 읽는다(idx_etl_job_run_dag_run). */
    List<EtlJobRun> findByAirflowDagRunIdOrderByStartedAtAsc(String airflowDagRunId);

    /** 실행 토큰으로 한 행(uq_etl_job_run_token). Airflow 완료 대기가 1초마다 부른다. */
    Optional<EtlJobRun> findByRunToken(String runToken);
}
