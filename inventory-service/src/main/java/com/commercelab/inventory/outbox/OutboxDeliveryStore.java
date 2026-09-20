package com.commercelab.inventory.outbox;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Each call commits its own short transaction before returning to the relay. */
@Repository
public class OutboxDeliveryStore {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;

    public OutboxDeliveryStore(JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
        this.jdbc = jdbc;
        this.transaction = new TransactionTemplate(transactionManager);
        this.transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public Optional<OutboxClaim> claimNext(Duration lease) {
        UUID token = UUID.randomUUID();
        return transaction.execute(tx -> jdbc.query("""
                WITH candidate AS (
                    SELECT event_id FROM inventory_result_outbox
                    WHERE delivered_at IS NULL AND next_attempt_at <= clock_timestamp()
                      AND (lease_until IS NULL OR lease_until <= clock_timestamp())
                    ORDER BY next_attempt_at, created_at, event_id
                    FOR UPDATE SKIP LOCKED LIMIT 1
                )
                UPDATE inventory_result_outbox o SET lease_token=?,
                    lease_until=clock_timestamp()+(? * interval '1 millisecond'),
                    attempt_count=CASE WHEN attempt_count < 9223372036854775807
                                       THEN attempt_count+1 ELSE attempt_count END
                FROM candidate c WHERE o.event_id=c.event_id RETURNING o.*
                """, (rs, row) -> new OutboxClaim(new OutboxMessage(
                        rs.getObject("event_id", UUID.class), rs.getObject("order_id", UUID.class),
                        rs.getString("topic"), rs.getString("message_key"), rs.getString("payload")),
                        rs.getObject("lease_token", UUID.class), rs.getLong("attempt_count")),
                token, lease.toMillis()).stream().findFirst());
    }

    public boolean markDelivered(UUID eventId, UUID token) {
        return transaction.execute(tx -> jdbc.update("""
                UPDATE inventory_result_outbox SET delivered_at=clock_timestamp(),
                    lease_token=NULL, lease_until=NULL, last_error_code=NULL
                WHERE event_id=? AND lease_token=? AND delivered_at IS NULL
                """, eventId, token) == 1);
    }

    public boolean reschedule(UUID eventId, UUID token, Duration delay, String errorCode) {
        return transaction.execute(tx -> jdbc.update("""
                UPDATE inventory_result_outbox SET next_attempt_at=clock_timestamp()+(? * interval '1 millisecond'),
                    last_error_code=?, lease_token=NULL, lease_until=NULL
                WHERE event_id=? AND lease_token=? AND delivered_at IS NULL
                """, delay.toMillis(), errorCode, eventId, token) == 1);
    }

    public OutboxStats stats() {
        return transaction.execute(tx -> jdbc.queryForObject("""
                SELECT count(*) AS pending_count,
                    COALESCE(GREATEST(0, EXTRACT(EPOCH FROM (clock_timestamp()-min(created_at)))), 0) AS oldest_age,
                    count(*) FILTER (WHERE last_error_code IS NOT NULL) AS failed_count
                FROM inventory_result_outbox WHERE delivered_at IS NULL
                """, (rs, row) -> new OutboxStats(rs.getLong("pending_count"),
                        rs.getDouble("oldest_age"), rs.getLong("failed_count"))));
    }
}
