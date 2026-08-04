package com.company.pipeline.connector;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * Oracle CDC(Debezium LogMiner) 소스 커넥터의 환경별 설정.
 *
 * <p>archiveLogOnly는 "온라인 redo를 읽지 않고 아카이브 로그만 읽는다"는 뜻이다. 기본값은
 * false이고, 이때 LogMiner 세션에는 온라인 redo와 아카이브가 모두 등록되어 평소에는
 * 온라인 redo에서 바로 읽는다(지연이 가장 짧다).
 *
 * <p>true로 켜면 변경이 아카이브로 넘어간 뒤에야 읽으므로 <b>지연이 redo 스위치 주기만큼
 * 늘어난다</b>. 운영 DB에서 온라인 redo 접근을 막는 경우에만 켤 것. 켤 때는 아카이브 보존
 * 기간이 그 지연보다 반드시 길어야 한다 - 아직 안 읽은 아카이브가 RMAN 등으로 먼저 지워지면
 * 그 구간은 복구할 수 없고 전체 재적재로만 되돌릴 수 있다.
 *
 * <p>cdbName은 LogMiner가 접속할 CDB 이름이다(PDB 이름은 커넥션별 serviceName을 쓴다).
 * XE는 CDB가 항상 XE지만, 다른 에디션은 설치할 때 정한 이름이라 환경변수로 뺀다.
 */
@ConfigurationProperties(prefix = "oracle-cdc")
public record OracleCdcProperties(Boolean archiveLogOnly, String cdbName) {

    public OracleCdcProperties {
        archiveLogOnly = archiveLogOnly != null && archiveLogOnly;
        cdbName = StringUtils.hasText(cdbName) ? cdbName : "XE";
    }
}
