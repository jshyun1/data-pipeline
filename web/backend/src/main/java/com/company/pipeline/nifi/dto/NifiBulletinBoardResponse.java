package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * GET /nifi-api/flow/bulletin-board 응답.
 *
 * <p>NiFi가 프로세서에서 난 경고/에러를 담아두는 곳으로, 이 환경에서 실제 실패
 * 원인을 얻을 수 있는 사실상 유일한 경로다(Provenance는 이벤트를 0건 반환하고,
 * 프로세서 단위 Status History는 항상 비어 있는 것으로 확인됨).
 *
 * <p>주의: bulletin은 NiFi 메모리에 5분 남짓만 남는 링버퍼다. 주기적으로 가져와
 * DB에 옮겨두지 않으면 그대로 사라진다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NifiBulletinBoardResponse(BulletinBoard bulletinBoard) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BulletinBoard(List<BulletinEntity> bulletins) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BulletinEntity(
            Long id,
            String groupId,
            String sourceId,
            Bulletin bulletin) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Bulletin(
            Long id,
            String category,
            String groupId,
            String sourceId,
            String sourceName,
            String level,
            String message,
            /** "HH:mm:ss z" 형태의 표시용 문자열이라 시각 계산엔 쓰지 않는다. */
            String timestamp) {
    }
}
