package com.vitallink.readings.alerting.state;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.alerts.DefaultAlertService;
import com.vitallink.readings.alerts.DefaultEscalationScheduler;
import com.vitallink.readings.alerts.ReassignmentService;
import com.vitallink.readings.alerts.StateChangeRecords;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.persistence.JdbcAlertStateRepository;
import com.vitallink.readings.persistence.JdbcAuditWriter;
import com.vitallink.readings.persistence.JdbcCareAssignmentRepository;
import com.vitallink.readings.persistence.JdbcOutboxWriter;
import com.vitallink.readings.testsupport.IntegrationTestBase;

/**
 * Alert state changes against real PostgreSQL. The services run as {@code vitallink_service} (so the grants are
 * exercised); assertions read as the owner, because the service role cannot read audit_log. Time is a
 * {@link MutableClock}: timers are crossed by moving the clock and calling the scheduler step directly.
 */
public abstract class AlertStateTestSupport extends IntegrationTestBase {

    static final Instant T0 = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    static final Duration WINDOW = Duration.ofMinutes(15);
    static final long PATIENT = 8812;
    static final long COACH = 41;
    static final long OTHER_COACH = 42;

    final MutableClock clock = new MutableClock(T0);
    final JdbcClient owner = JdbcClient.create(dataSource());
    final DataSource service = asServiceRole(dataSource());
    final ObjectMapper json = new ObjectMapper();

    DefaultAlertService alertService() {
        return new DefaultAlertService(new JdbcAlertStateRepository(JdbcClient.create(service)), outbox(), audit(),
                tx(), records(), clock);
    }

    DefaultEscalationScheduler scheduler() {
        return new DefaultEscalationScheduler(new JdbcAlertStateRepository(JdbcClient.create(service)), outbox(),
                audit(), tx(), records());
    }

    ReassignmentService reassignment() {
        JdbcClient jdbc = JdbcClient.create(service);
        return new ReassignmentService(new JdbcCareAssignmentRepository(jdbc), new JdbcAlertStateRepository(jdbc),
                outbox(), audit(), tx(), records(), clock);
    }

    private JdbcOutboxWriter outbox() {
        return new JdbcOutboxWriter(JdbcClient.create(service));
    }

    private JdbcAuditWriter audit() {
        return new JdbcAuditWriter(JdbcClient.create(service));
    }

    private TransactionTemplate tx() {
        return new TransactionTemplate(new DataSourceTransactionManager(service));
    }

    private StateChangeRecords records() {
        return new StateChangeRecords(clock, new UuidV7(clock), json);
    }

    void seedPrimary(long patientId, long coachId) {
        owner.sql("INSERT INTO care_assignment (patient_id, coach_id, role, valid_from) VALUES (?, ?, 'PRIMARY', ?)")
                .params(patientId, coachId, utc(T0.minus(Duration.ofDays(30))))
                .update();
    }

    /** An OPEN CRITICAL alert whose escalation timer started at T0. */
    long openCritical(long patientId) {
        return insertAlert(patientId, Measure.SYSTOLIC, Severity.CRITICAL, AlertStatus.OPEN, T0.plus(WINDOW));
    }

    long insertAlert(long patientId, Measure measure, Severity severity, AlertStatus status, Instant escalateAt) {
        Instant ackAt = status == AlertStatus.ACKNOWLEDGED ? T0 : null;
        Instant resolvedAt = status == AlertStatus.RESOLVED ? T0 : null;
        return owner.sql("""
                        INSERT INTO alert (patient_id, measure_code, coach_id, severity, status, escalate_at,
                                           first_seen_at, last_seen_at, acknowledged_by, acknowledged_at,
                                           resolved_by, resolved_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        RETURNING alert_id
                        """)
                .params(patientId, measure.code(), COACH, severity.name(), status.name(), utc(escalateAt), utc(T0),
                        utc(T0), ackAt == null ? null : COACH, utc(ackAt), resolvedAt == null ? null : COACH,
                        utc(resolvedAt))
                .query(Long.class).single();
    }

    Map<String, Object> alertRow(long alertId) {
        return owner.sql("SELECT status, version, coach_id, escalate_at, resolution_note FROM alert WHERE alert_id = ?")
                .param(alertId).query().singleRow();
    }

    List<JsonNode> stateChangedEvents() {
        return owner.sql("""
                        SELECT payload::text FROM outbox_event WHERE event_type = 'alert.state_changed'
                        ORDER BY created_at, event_id
                        """)
                .query(String.class).list().stream().map(this::parse).toList();
    }

    List<JsonNode> stateChangedEvents(long alertId) {
        return stateChangedEvents().stream().filter(e -> e.get("alert_id").asLong() == alertId).toList();
    }

    List<String> outboxTopics() {
        return owner.sql("SELECT topic FROM outbox_event WHERE event_type = 'alert.state_changed'")
                .query(String.class).list();
    }

    List<Map<String, Object>> audits() {
        return owner.sql("SELECT actor_type, actor_id, action, outcome, patient_id, resource, request_id FROM audit_log")
                .query().listOfRows();
    }

    List<Map<String, Object>> audits(String action) {
        return audits().stream().filter(a -> action.equals(a.get("action"))).toList();
    }

    private JsonNode parse(String payload) {
        try {
            return json.readTree(payload);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static OffsetDateTime utc(Instant t) {
        return t == null ? null : t.atOffset(ZoneOffset.UTC);
    }

    static DataSource asServiceRole(DataSource ownerDs) {
        return new DelegatingDataSource(ownerDs) {
            @Override
            public Connection getConnection() throws SQLException {
                Connection c = super.getConnection();
                try (Statement st = c.createStatement()) {
                    st.execute("SET ROLE vitallink_service");
                }
                return c;
            }
        };
    }
}
