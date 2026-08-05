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

/**
 * 스텝 사이 연결선. V17 마이그레이션.
 *
 * <p>{@code relationships}가 핵심이다 - 같은 두 프로세서를 이어도 success로 이었는지
 * failure로 이었는지에 따라 잡의 의미가 완전히 달라진다(예: DZ_UPSERT의 오류 경로).
 */
@Entity
@Table(name = "etl_job_link")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EtlJobLink {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false)
    private Long jobId;

    @Column(name = "nifi_connection_id", length = 100, nullable = false)
    private String nifiConnectionId;

    @Column(name = "from_component_id", length = 100, nullable = false)
    private String fromComponentId;

    @Column(name = "from_name", length = 200)
    private String fromName;

    @Column(name = "to_component_id", length = 100, nullable = false)
    private String toComponentId;

    @Column(name = "to_name", length = 200)
    private String toName;

    @Column(name = "relationships", length = 200)
    private String relationships;

    @Column(name = "deleted_at")
    private LocalDateTime deletedAt;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public EtlJobLink(Long jobId, String nifiConnectionId, String fromComponentId, String toComponentId) {
        this.jobId = jobId;
        this.nifiConnectionId = nifiConnectionId;
        this.fromComponentId = fromComponentId;
        this.toComponentId = toComponentId;
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void applySnapshot(Long jobId, String fromComponentId, String fromName,
                              String toComponentId, String toName, String relationships) {
        this.jobId = jobId;
        this.fromComponentId = fromComponentId;
        this.fromName = fromName;
        this.toComponentId = toComponentId;
        this.toName = toName;
        this.relationships = relationships;
        this.deletedAt = null;
        this.updatedAt = LocalDateTime.now();
    }

    public void markDeleted() {
        if (this.deletedAt == null) {
            this.deletedAt = LocalDateTime.now();
            this.updatedAt = this.deletedAt;
        }
    }
}
