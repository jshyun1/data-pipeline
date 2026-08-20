package com.company.pipeline.user;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
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

    // 인증은 Keycloak이 담당하므로 로컬 비밀번호는 저장하지 않는다(nullable, V8 참고).
    // Keycloak 제거 후 로컬 인증(U3)이 복원되어 다시 bcrypt 해시 검증에 쓰인다.
    @Column(name = "user_pw", length = 255)
    private String userPw;

    // V24(로컬 인증 U3): bcrypt 갱신 시각·강제변경·실패잠금. TIMESTAMPTZ 컬럼이라 OffsetDateTime
    // 으로 매핑한다(LocalDateTime 으로 매핑하면 ddl-auto=validate 가 타입 불일치로 부팅을 막는다).
    @Column(name = "pw_updated_at")
    private OffsetDateTime pwUpdatedAt;

    @Column(name = "pw_must_change", nullable = false)
    private boolean pwMustChange = false;

    @Column(name = "login_fail_count", nullable = false)
    private int loginFailCount = 0;

    @Column(name = "locked_until")
    private OffsetDateTime lockedUntil;

    @Column(name = "email", length = 255)
    private String email;

    @Column(name = "tel_no", length = 255)
    private String telNo;

    // 첫 로그인 자동 프로비저닝 시 비어있을 수 있고 관리자가 나중에 채운다(nullable, V8 참고).
    @Column(name = "hq_cd", length = 50)
    private String hqCd;

    @Column(name = "position_cd", length = 50)
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

    /** 신규 계정 생성용 공개 생성자(관리 화면에서 userId 지정 후 나머지는 setter 로 채운다). */
    public AppUser(String userId) {
        this.userId = userId;
    }

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
}
