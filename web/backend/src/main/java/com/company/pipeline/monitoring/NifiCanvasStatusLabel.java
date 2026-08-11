package com.company.pipeline.monitoring;

import com.company.pipeline.common.entity.BaseAuditEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "nifi_canvas_status_label")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class NifiCanvasStatusLabel extends BaseAuditEntity {

    @Id
    @Column(name = "group_id", length = 100)
    private String groupId;

    @Column(name = "parent_group_id", length = 100, nullable = false)
    private String parentGroupId;

    @Column(name = "label_id", length = 100, nullable = false)
    private String labelId;

    public NifiCanvasStatusLabel(String groupId, String parentGroupId, String labelId) {
        this.groupId = groupId;
        this.parentGroupId = parentGroupId;
        this.labelId = labelId;
    }

    public void update(String parentGroupId, String labelId) {
        this.parentGroupId = parentGroupId;
        this.labelId = labelId;
    }
}
