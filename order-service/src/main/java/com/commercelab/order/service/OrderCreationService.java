package com.commercelab.order.service;

import com.commercelab.order.domain.IdempotencyConflictException;
import com.commercelab.order.domain.Order;
import com.commercelab.order.persistence.OrderRequestStore;
import com.commercelab.order.repository.OrderRepository;
import org.postgresql.util.PSQLException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class OrderCreationService {
    private final OrderRepository orders;
    private final OrderRequestStore requests;
    private final TransactionTemplate transaction;

    public OrderCreationService(OrderRepository orders, OrderRequestStore requests,
            PlatformTransactionManager transactionManager) {
        this.orders = orders;
        this.requests = requests;
        transaction = new TransactionTemplate(transactionManager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public OrderCreation createOrReplay(String key, PlaceOrderCommand command, String correlationId) {
        if (key == null || !key.matches("[A-Za-z0-9._:-]{1,128}")) {
            throw new IllegalArgumentException("Idempotency-Key must match [A-Za-z0-9._:-]{1,128}");
        }
        OrderPayload payload = OrderPayload.from(command);
        try {
            return transaction.execute(status -> {
                var existing = requests.find(key);
                if (existing.isPresent()) return replay(existing.get(), payload);
                Order order = orders.add(Order.place(payload.customerId(), payload.lines()));
                requests.insert(key, order.id(), payload, correlationId);
                return new OrderCreation(order, true);
            });
        } catch (RuntimeException ex) {
            if (!isRequestKeyRace(ex)) throw ex;
            // execute has completed rollback before a fresh transaction reads the winner.
            return transaction.execute(status -> replay(requests.find(key).orElseThrow(() -> ex), payload));
        }
    }

    private OrderCreation replay(OrderRequestStore.StoredRequest stored, OrderPayload payload) {
        if (stored.fingerprintVersion() != 1) {
            throw new IllegalStateException("Unsupported order fingerprint version");
        }
        if (!stored.canonicalPayload().equals(payload.canonicalJson())) {
            throw new IdempotencyConflictException();
        }
        return new OrderCreation(orders.findById(stored.orderId()).orElseThrow(
                () -> new IllegalStateException("Order request references missing order")), false);
    }

    private static boolean isRequestKeyRace(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof PSQLException constraint
                    && "23505".equals(constraint.getSQLState())
                    && constraint.getServerErrorMessage() != null
                    && OrderRequestStore.KEY_CONSTRAINT.equals(constraint.getServerErrorMessage().getConstraint())) {
                return true;
            }
        }
        return false;
    }
}
