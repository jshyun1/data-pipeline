package com.company.pipeline.authz;

/**
 * 인가 대상 시스템(리소스) 코드. 역할이 이 시스템별로 접근 비트를 갖는다(설계서 §4.2).
 *
 * <ul>
 *   <li>{@link #COMMON} - 대시보드·인프라 상태 (항상 READ만)</li>
 *   <li>{@link #NIFI} - ETL 생성/관리/로그</li>
 *   <li>{@link #AIRFLOW} - 스케줄 관리</li>
 *   <li>{@link #KAFKA} - CDC 파이프라인/연결정보/로그</li>
 *   <li>{@link #ADMIN} - 계정·역할·권한 관리</li>
 * </ul>
 *
 * <p>DB(app_role_system_permission.system_code, app_menu.system_code)에는 이 enum의 name()이
 * 그대로 저장된다.
 */
public enum SystemCode {
    COMMON,
    NIFI,
    AIRFLOW,
    KAFKA,
    ADMIN
}
