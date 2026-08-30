package com.company.pipeline.nifi;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "nifi_process_group_metadata")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class NifiProcessGroupMetadata {

    @Id
    @Column(name = "process_group_id", length = 100)
    private String processGroupId;

    @Column(name = "process_group_name", length = 200, nullable = false)
    private String processGroupName;

    @Column(name = "parent_group_id", length = 100)
    private String parentGroupId;

    @Column(name = "comments")
    private String comments;

    @Column(name = "created_by", length = 255, nullable = false)
    private String createdBy;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_by", length = 255, nullable = false)
    private String updatedBy;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public NifiProcessGroupMetadata(String processGroupId, String processGroupName,
                                    String parentGroupId, String comments, String actor) {
        LocalDateTime now = LocalDateTime.now();
        this.processGroupId = processGroupId;
        this.processGroupName = processGroupName;
        this.parentGroupId = parentGroupId;
        this.comments = comments;
        this.createdBy = actor;
        this.createdAt = now;
        this.updatedBy = actor;
        this.updatedAt = now;
    }

    public void applySnapshot(String processGroupName, String parentGroupId, String comments, String actor) {
        this.processGroupName = processGroupName;
        this.parentGroupId = parentGroupId;
        this.comments = comments;
        this.updatedBy = actor;
        this.updatedAt = LocalDateTime.now();
    }
}
