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
 * 잡에 바인딩된 파라미터 컨텍스트의 파라미터 하나. V17 마이그레이션.
 *
 * <p>지금은 NiFi에서 읽어오기만 한다({@code syncDirection=FROM_NIFI}). 화면에서 값을
 * 바꾸는 단계로 갈 때 TO_NIFI로 두고 푸시 로직만 붙이면 되도록 방향 컬럼을 미리 뒀다.
 *
 * <p>sensitive 파라미터는 NiFi가 값을 내려주지 않으므로 이름만 남는다.
 */
@Entity
@Table(name = "etl_job_param")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EtlJobParam {

    public static final String DIRECTION_FROM_NIFI = "FROM_NIFI";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false)
    private Long jobId;

    @Column(name = "param_name", length = 200, nullable = false)
    private String paramName;

    @Column(name = "param_value")
    private String paramValue;

    @Column(name = "sensitive", nullable = false)
    private boolean sensitive;

    @Column(name = "description")
    private String description;

    @Column(name = "sync_direction", length = 20, nullable = false)
    private String syncDirection = DIRECTION_FROM_NIFI;

    @Column(name = "deleted_at")
    private LocalDateTime deletedAt;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public EtlJobParam(Long jobId, String paramName) {
        this.jobId = jobId;
        this.paramName = paramName;
        this.syncDirection = DIRECTION_FROM_NIFI;
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void applySnapshot(String paramValue, boolean sensitive, String description) {
        this.paramValue = paramValue;
        this.sensitive = sensitive;
        this.description = description;
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
