package com.company.pipeline.user.dto;

import com.company.pipeline.user.AppUser;

/** 사용자 정보 응답. 비밀번호(해시 포함)는 절대 노출하지 않는다. */
public record UserResponse(
        String userId,
        String userNm,
        String email,
        String telNo,
        String hqCd,
        String positionCd,
        boolean admin) {

    public static UserResponse from(AppUser user) {
        return new UserResponse(
                user.getUserId(),
                user.getUserNm(),
                user.getEmail(),
                user.getTelNo(),
                user.getHqCd(),
                user.getPositionCd(),
                user.isAdmin());
    }
}
