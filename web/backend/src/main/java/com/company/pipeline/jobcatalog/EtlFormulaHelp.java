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

@Entity
@Table(name = "etl_formula_help")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EtlFormulaHelp {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "function_name", length = 80, nullable = false)
    private String functionName;

    @Column(name = "category", length = 40, nullable = false)
    private String category;

    @Column(name = "syntax", length = 300, nullable = false)
    private String syntax;

    @Column(name = "summary", nullable = false)
    private String summary;

    @Column(name = "usage_text", nullable = false)
    private String usageText;

    @Column(name = "examples_json", nullable = false)
    private String examplesJson;

    @Column(name = "notice")
    private String notice;

    @Column(name = "sort_order", nullable = false)
    private int sortOrder;

    @Column(name = "enabled", nullable = false)
    private boolean enabled;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
