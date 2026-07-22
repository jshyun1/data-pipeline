package com.company.pipeline.user;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 통합 웹 로그인 계정. V7 마이그레이션(app_user)과 매핑. 감사 컬럼명이 기존
 * BaseAuditEntity(created_at/updated_at)와 달라(reg_dt/upd_dt) 상속하지 않고
 * 직접 관리한다. user_pw에는 bcrypt 해시만 저장한다(평문 저장 금지).
 */
@Entity
@Table(name = "app_user")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AppUser {

    @Id
    @Column(name = "user_id", length = 255)
    private String userId;

    @Column(name = "user_nm", nullable = false, length = 255)
    private String userNm;

    @Column(name = "user_pw", nullable = false, length = 255)
    private String userPw;

    @Column(name = "email", length = 255)
    private String email;

    @Column(name = "tel_no", length = 255)
    private String telNo;

    @Column(name = "hq_cd", nullable = false, length = 50)
    private String hqCd;

    @Column(name = "position_cd", nullable = false, length = 50)
    private String positionCd;

    @Column(name = "admin_yn", nullable = false, length = 255)
    private String adminYn = "N";

    @Column(name = "use_yn", nullable = false, length = 255)
    private String useYn = "Y";

    @Column(name = "last_login_dt")
    private LocalDateTime lastLoginDt;

    @Column(name = "reg_dt", nullable = false, updatable = false)
    private LocalDateTime regDt;

    @Column(name = "upd_dt")
    private LocalDateTime updDt;

    @Column(name = "use_strt_dttm", nullable = false)
    private LocalDateTime useStrtDttm;

    @Column(name = "use_end_dttm", nullable = false)
    private LocalDateTime useEndDttm;

    @PrePersist
    void applyDefaults() {
        LocalDateTime now = LocalDateTime.now();
        if (regDt == null) {
            regDt = now;
        }
        if (useStrtDttm == null) {
            useStrtDttm = now;
        }
        if (useEndDttm == null) {
            useEndDttm = now.plusMonths(3);
        }
    }

    public boolean isAdmin() {
        return "Y".equalsIgnoreCase(adminYn);
    }

    /** 로그인 가능 여부: 승인(use_yn='Y')됐고 사용 기간(use_strt~use_end) 안에 있어야 한다. */
    public boolean isLoginAllowed(LocalDateTime at) {
        return "Y".equalsIgnoreCase(useYn)
                && !at.isBefore(useStrtDttm)
                && !at.isAfter(useEndDttm);
    }
}
