package com.company.pipeline.user.security;

import static org.assertj.core.api.Assertions.assertThat;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;

class AccountDataSourceConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(AccountDataSourceConfig.class)
            .withPropertyValues(
                    "authz.provider=EXTERNAL",
                    "spring.datasource.url=jdbc:postgresql://localhost/pipeline_meta",
                    "spring.datasource.username=pipeline_app",
                    "spring.datasource.password=test",
                    "account-db.host=localhost",
                    "account-db.port=3306",
                    "account-db.name=mydb",
                    "account-db.username=account_user",
                    "account-db.password=test");

    @Test
    void keepsPostgresJdbcTemplatePrimaryWhenExternalAccountDbIsEnabled() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            JdbcTemplate primary = context.getBean(JdbcTemplate.class);
            DataSource primaryDataSource = context.getBean("dataSource", DataSource.class);
            JdbcTemplate account = context.getBean("accountJdbcTemplate", JdbcTemplate.class);
            DataSource accountDataSource = context.getBean("accountDataSource", DataSource.class);

            assertThat(primary.getDataSource()).isSameAs(primaryDataSource);
            assertThat(account.getDataSource()).isSameAs(accountDataSource);
            assertThat(primary.getDataSource()).isNotSameAs(account.getDataSource());
        });
    }
}
