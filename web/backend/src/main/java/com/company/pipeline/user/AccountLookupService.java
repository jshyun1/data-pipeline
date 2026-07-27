package com.company.pipeline.user;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

/**
 * 통합 계정 테이블(ST_USER, MySQL) 조회 전용. 이 테이블은 여러 시스템이 공유하는 실
 * 사용자 디렉터리라 여기서는 SELECT만 한다(계정 생성/수정/삭제는 이 서비스 책임이 아님).
 */
@Service
public class AccountLookupService {

    public record Account(
            String userId,
            String userNm,
            String userPw,
            String email,
            String useYn,
            LocalDateTime useStrtDttm,
            LocalDateTime useEndDttm) {

        public boolean isUsable() {
            if (!"Y".equalsIgnoreCase(useYn)) {
                return false;
            }
            LocalDateTime now = LocalDateTime.now();
            return !(useStrtDttm != null && now.isBefore(useStrtDttm))
                    && !(useEndDttm != null && now.isAfter(useEndDttm));
        }
    }

    private static final RowMapper<Account> ROW_MAPPER = (rs, rowNum) -> new Account(
            rs.getString("USER_ID"),
            rs.getString("USER_NM"),
            rs.getString("USER_PW"),
            rs.getString("EMAIL"),
            rs.getString("USE_YN"),
            toLocalDateTime(rs.getTimestamp("USE_STRT_DTTM")),
            toLocalDateTime(rs.getTimestamp("USE_END_DTTM")));

    private final JdbcTemplate accountJdbcTemplate;

    public AccountLookupService(@Qualifier("accountJdbcTemplate") JdbcTemplate accountJdbcTemplate) {
        this.accountJdbcTemplate = accountJdbcTemplate;
    }

    public Account findById(String userId) {
        return accountJdbcTemplate.query(
                "SELECT USER_ID, USER_NM, USER_PW, EMAIL, USE_YN, USE_STRT_DTTM, USE_END_DTTM "
                        + "FROM ST_USER WHERE USER_ID = ?",
                ROW_MAPPER,
                userId
        ).stream().findFirst().orElse(null);
    }

    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp != null ? timestamp.toLocalDateTime() : null;
    }
}
