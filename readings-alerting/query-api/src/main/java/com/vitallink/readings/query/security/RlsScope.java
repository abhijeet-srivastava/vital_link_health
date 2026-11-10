package com.vitallink.readings.query.security;

import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Runs work in a transaction whose first statement sets {@code app.coach_id} (transaction-local, so a pooled
 * connection never carries it into the next request). The row-level security policies on reading and alert then
 * show only the coach's current panel: the second check behind {@link PanelAccessGuard}. Work that opens its own
 * TransactionTemplate on the same pool (DefaultAlertService) joins this transaction.
 */
@Component
public class RlsScope {

    private final JdbcClient primary;
    private final TransactionTemplate primaryTx;
    private final JdbcClient replica;
    private final TransactionTemplate replicaTx;

    public RlsScope(JdbcClient primary, TransactionTemplate primaryTx,
                    @Qualifier("replicaJdbcClient") JdbcClient replica,
                    @Qualifier("replicaTransactionTemplate") TransactionTemplate replicaTx) {
        this.primary = primary;
        this.primaryTx = primaryTx;
        this.replica = replica;
        this.replicaTx = replicaTx;
    }

    /** Read-only transaction on the replica pool. */
    public <T> T read(Caller caller, Supplier<T> work) {
        return replicaTx.execute(status -> {
            bind(replica, caller);
            return work.get();
        });
    }

    /** Read-write transaction on the primary pool. */
    public <T> T write(Caller caller, Supplier<T> work) {
        return primaryTx.execute(status -> {
            bind(primary, caller);
            return work.get();
        });
    }

    private static void bind(JdbcClient jdbc, Caller caller) {
        // Non-coaches get no coach id, which the policies treat as "no rows".
        String coachId = caller.isCoach() ? String.valueOf(caller.coachId()) : "";
        jdbc.sql("SELECT set_config('app.coach_id', ?, true)").param(coachId).query(String.class).single();
    }
}
