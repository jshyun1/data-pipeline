package com.company.pipeline.user.security;

import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 통합 계정 테이블(ST_USER, MySQL) 전용 커넥션. 앱의 기본 DataSource(JPA/Flyway 대상)는
 * 여전히 metadata-db(Postgres)이어야 하는데, DataSource 빈이 2개가 되면 Flyway/JPA
 * 자동설정이 어느 쪽을 써야 할지 모호해져서(실제로 이 MySQL 빈을 집어가 Flyway가 깨짐)
 * Postgres 쪽을 명시적으로 @Primary로 다시 선언해 모호성을 없앤다.
 * 이 계정 DB는 여러 시스템이 공유하는 실 사용자 디렉터리라 - 여기서는 절대 스키마를
 * 변경하거나 데이터를 쓰지 않는다(조회 전용).
 */
@Configuration
@EnableConfigurationProperties
public class AccountDataSourceConfig {

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties primaryDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @Primary
    public DataSource dataSource(DataSourceProperties primaryDataSourceProperties) {
        return primaryDataSourceProperties.initializeDataSourceBuilder().build();
    }

    @Value("${account-db.host}")
    private String host;

    @Value("${account-db.port}")
    private String port;

    @Value("${account-db.name}")
    private String name;

    @Value("${account-db.username}")
    private String username;

    @Value("${account-db.password}")
    private String password;

    @Bean
    @Qualifier("accountDataSource")
    public DataSource accountDataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + name
                + "?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8");
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        // 로그인 검증 한 번 하고 마는 저빈도 조회라 풀을 작게 유지한다.
        dataSource.setMaximumPoolSize(3);
        dataSource.setPoolName("account-db-pool");
        return dataSource;
    }

    @Bean
    @Qualifier("accountJdbcTemplate")
    public JdbcTemplate accountJdbcTemplate(@Qualifier("accountDataSource") DataSource accountDataSource) {
        return new JdbcTemplate(accountDataSource);
    }
}
