package com.company.pipeline.pipeline;

import com.company.pipeline.common.entity.BaseAuditEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * CDC 파이프라인을 담는 그룹(사용자가 만드는 폴더).
 *
 * <p>관리 화면 트리의 최상단 «전체 파이프라인»은 행이 아니라 화면이 그리는 가상 뿌리다.
 * 그래서 parent 가 NULL 인 그룹이 그 바로 아래 칸이 된다.
 */
@Entity
@Table(name = "pipeline_group")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PipelineGroup extends BaseAuditEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 상위 그룹. NULL 이면 «전체 파이프라인» 바로 아래. */
    @Column(name = "parent_id")
    private Long parentId;

    @Column(nullable = false, length = 100)
    private String name;

    public PipelineGroup(Long parentId, String name) {
        this.parentId = parentId;
        this.name = name;
    }
}
