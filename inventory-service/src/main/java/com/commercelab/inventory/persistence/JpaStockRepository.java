package com.commercelab.inventory.persistence;

import com.commercelab.inventory.domain.StockItem;
import com.commercelab.inventory.repository.StockRepository;
import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class JpaStockRepository implements StockRepository {

    private final StockJpaRepository jpa;
    private final EntityManager entityManager;

    public JpaStockRepository(StockJpaRepository jpa, EntityManager entityManager) {
        this.jpa = jpa;
        this.entityManager = entityManager;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<StockItem> findBySku(String sku) {
        return jpa.findById(sku).map(StockEntity::toDomain);
    }

    @Override
    @Transactional
    public List<StockItem> lockBySkus(List<String> sortedSkus) {
        return jpa.lockBySkus(sortedSkus).stream()
                .map(StockEntity::toDomain)
                .toList();
    }

    @Override
    @Transactional
    public void updateAll(List<StockItem> stock) {
        for (StockItem item : stock) {
            StockEntity entity = entityManager.find(StockEntity.class, item.sku());
            if (entity == null) {
                throw new IllegalStateException("locked stock row is missing: " + item.sku());
            }
            entity.updateFrom(item);
        }
    }
}
