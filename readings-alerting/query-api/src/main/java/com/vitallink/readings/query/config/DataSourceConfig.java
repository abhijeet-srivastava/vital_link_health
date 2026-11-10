package com.vitallink.readings.query.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.zaxxer.hikari.HikariDataSource;

/**
 * Three pools, one per trust level and load:
 * <ul>
 *   <li>{@code primary}: coach login (vitallink_query, row-level security applies). Acknowledge/resolve, threshold
 *       changes, panel checks and audit rows. {@code @Primary}, so anything unqualified lands here.</li>
 *   <li>{@code replica}: same login, read-only. Every PHI read. The primary locally; a streaming replica in
 *       production (architecture.md: query-api reads replicas).</li>
 *   <li>{@code admin}: vitallink_service login from {@code spring.datasource.*}, used only by the internal
 *       care-assignment endpoint, which moves alerts between coaches and so cannot run in one coach's context.</li>
 * </ul>
 */
@Configuration
class DataSourceConfig {

    @Bean
    @Primary
    @ConfigurationProperties("vitallink.datasource.primary")
    HikariDataSource dataSource() {
        return new HikariDataSource();
    }

    @Bean
    @ConfigurationProperties("vitallink.datasource.replica")
    HikariDataSource replicaDataSource() {
        return new HikariDataSource();
    }

    @Bean
    HikariDataSource adminDataSource(DataSourceProperties props) {
        return props.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean
    @Primary
    PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    @Primary
    JdbcClient jdbcClient(DataSource dataSource) {
        return JdbcClient.create(dataSource);
    }

    @Bean
    @Primary
    TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }

    @Bean
    JdbcClient replicaJdbcClient(@Qualifier("replicaDataSource") DataSource replica) {
        return JdbcClient.create(replica);
    }

    @Bean
    TransactionTemplate replicaTransactionTemplate(@Qualifier("replicaDataSource") DataSource replica) {
        TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(replica));
        tx.setReadOnly(true);
        return tx;
    }

    @Bean
    JdbcClient adminJdbcClient(@Qualifier("adminDataSource") DataSource admin) {
        return JdbcClient.create(admin);
    }

    @Bean
    TransactionTemplate adminTransactionTemplate(@Qualifier("adminDataSource") DataSource admin) {
        return new TransactionTemplate(new DataSourceTransactionManager(admin));
    }
}
