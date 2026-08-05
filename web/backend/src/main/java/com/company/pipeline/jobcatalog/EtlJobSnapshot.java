package com.company.pipeline.jobcatalog;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * 잡 구조가 실제로 바뀐 시점의 전체 스냅샷. V17 마이그레이션.
 *
 * <p>동기화는 5분마다 돌지만 이 행은 직전 해시와 달라졌을 때만 쌓인다. 용도는 두 가지다 -
 * "언제 무엇이 바뀌었나" 추적, 그리고 캔버스 유실 시 복구 근거(flow.json.gz 백업이
 * 최신이 아닐 때 마지막으로 관측된 구조가 여기 남는다).
 */
@Entity
@Table(name = "etl_job_snapshot")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EtlJobSnapshot {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false)
    private Long jobId;

    @Column(name = "captured_at", nullable = false)
    private LocalDateTime capturedAt;

    @Column(name = "content_hash", length = 64, nullable = false)
    private String contentHash;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "snapshot", columnDefinition = "jsonb", nullable = false)
    private String snapshot;

    public EtlJobSnapshot(Long jobId, String contentHash, String snapshot) {
        this.jobId = jobId;
        this.contentHash = contentHash;
        this.snapshot = snapshot;
        this.capturedAt = LocalDateTime.now();
    }
}
