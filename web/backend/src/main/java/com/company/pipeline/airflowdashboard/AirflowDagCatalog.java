package com.company.pipeline.airflowdashboard;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "airflow_dag_catalog")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AirflowDagCatalog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dag_id", nullable = false, unique = true, length = 250)
    private String dagId;

    @Column(name = "business_group", nullable = false, length = 20)
    private String businessGroup;

    @Column(name = "business_folder", nullable = false, length = 200)
    private String businessFolder;

    @Column(name = "display_name", nullable = false, length = 200)
    private String displayName;

    @Column(name = "description")
    private String description;

    @Column(name = "sort_order", nullable = false)
    private int sortOrder;

    @Column(name = "enabled", nullable = false)
    private boolean enabled;
}
