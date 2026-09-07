package com.commercelab.inventory.persistence;

import com.commercelab.inventory.domain.StockItem;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "stock")
public class StockEntity {

    @Id
    @Column(nullable = false, length = 64)
    private String sku;

    @Column(name = "available_quantity", nullable = false)
    private int availableQuantity;

    protected StockEntity() {
        // for JPA
    }

    StockItem toDomain() {
        return new StockItem(sku, availableQuantity);
    }

    void updateFrom(StockItem stock) {
        availableQuantity = stock.availableQuantity();
    }
}
