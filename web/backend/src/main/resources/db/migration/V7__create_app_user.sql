-- 통합 웹(Cerebro ETL) 로그인 계정 테이블. 첨부된 사용자 테이블 컬럼 정의 그대로 옮김.
-- 테이블명은 "user"가 PostgreSQL 예약어라 app_user로 둔다(컬럼명은 원안 유지).
-- user_pw에는 평문이 아니라 bcrypt 해시를 저장한다.
CREATE TABLE app_user (
    user_id        VARCHAR(255) PRIMARY KEY,                 -- 사용자ID (로그인 아이디)
    user_nm        VARCHAR(255) NOT NULL,                    -- 사용자명
    user_pw        VARCHAR(255) NOT NULL,                    -- 사용자비밀번호 (bcrypt 해시)
    email          VARCHAR(255),                             -- 이메일
    tel_no         VARCHAR(255),                             -- 전화번호
    hq_cd          VARCHAR(50)  NOT NULL,                    -- 본부ID
    position_cd    VARCHAR(50)  NOT NULL,                    -- 직함ID
    admin_yn       VARCHAR(255) NOT NULL DEFAULT 'N',        -- 관리자여부
    use_yn         VARCHAR(255) NOT NULL DEFAULT 'Y',        -- 사용여부(승인 여부)
    last_login_dt  TIMESTAMP,                                -- 최종로그인일시
    reg_dt         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- 등록일시
    upd_dt         TIMESTAMP,                                -- 수정일시
    use_strt_dttm  TIMESTAMP    NOT NULL DEFAULT now(),                 -- 사용시작일자
    use_end_dttm   TIMESTAMP    NOT NULL DEFAULT (now() + INTERVAL '3 months')  -- 사용종료일자
);
