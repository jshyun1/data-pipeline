package com.company.pipeline.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.connection.DbType;
import org.junit.jupiter.api.Test;

class DeltaTargetTableServiceTest {

    // 세 DB 모두 순번(자동 증가 PK) + 구분컬럼 두 열짜리 뼈대만 만든다. 소스 컬럼은 싱크가 붙인다.
    @Test
    void createTableDdl_postgres() {
        assertThat(DeltaTargetTableService.createTableDdl(DbType.POSTGRESQL, "public", "aa_table_delta", "cdc_op"))
                .isEqualTo("CREATE TABLE public.aa_table_delta (cdc_seq BIGSERIAL PRIMARY KEY, cdc_op VARCHAR(10) NOT NULL)");
    }

    @Test
    void createTableDdl_oracle() {
        assertThat(DeltaTargetTableService.createTableDdl(DbType.ORACLE, "APPUSER", "AA_TABLE_DELTA", "CDC_OP"))
                .isEqualTo("CREATE TABLE APPUSER.AA_TABLE_DELTA (cdc_seq NUMBER(19) GENERATED ALWAYS AS IDENTITY PRIMARY KEY, CDC_OP VARCHAR2(10) NOT NULL)");
    }

    @Test
    void createTableDdl_mysql() {
        assertThat(DeltaTargetTableService.createTableDdl(DbType.MYSQL, "warehouse", "aa_table_delta", "cdc_op"))
                .isEqualTo("CREATE TABLE warehouse.aa_table_delta (cdc_seq BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, cdc_op VARCHAR(10) NOT NULL)");
    }

    // upsert 키 검증: 소스 PK 컬럼 집합이 타깃의 PK/UNIQUE 중 하나와 대소문자 무시하고 정확히 같아야 한다.
    @Test
    void keyMatches_ignoresCaseAndRequiresExactColumnSet() {
        java.util.Set<String> sourceKey = java.util.Set.of("ID", "TENANT_ID");
        assertThat(DeltaTargetTableService.keyMatches(sourceKey,
                java.util.List.of(java.util.Set.of("tenant_id", "id")))).isTrue();
        assertThat(DeltaTargetTableService.keyMatches(sourceKey,
                java.util.List.of(java.util.Set.of("id")))).isFalse();
        assertThat(DeltaTargetTableService.keyMatches(sourceKey,
                java.util.List.of(java.util.Set.of("id", "tenant_id", "cdc_seq"), java.util.Set.of("id", "tenant_id")))).isTrue();
        assertThat(DeltaTargetTableService.keyMatches(sourceKey, java.util.List.of())).isFalse();
    }

    @Test
    void normalizeDeltaOpColumn_rejectsReservedTimestampName() {
        assertThat(org.junit.jupiter.api.Assertions.assertThrows(
                com.company.pipeline.common.BusinessException.class,
                () -> PipelineLoadMode.normalizeDeltaOpColumn("cdc_ts")).getMessage()).contains("예약");
    }

    @Test
    void normalizeDeltaOpColumn_rejectsReservedSequenceName() {
        assertThat(org.junit.jupiter.api.Assertions.assertThrows(
                com.company.pipeline.common.BusinessException.class,
                () -> PipelineLoadMode.normalizeDeltaOpColumn("CDC_SEQ")).getMessage()).contains("예약");
    }
}
