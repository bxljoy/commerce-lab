package com.commercelab.inventory.repository;

import com.commercelab.inventory.domain.StockItem;
import java.util.List;
import java.util.Optional;

public interface StockRepository {

    Optional<StockItem> findBySku(String sku);

    List<StockItem> lockBySkus(List<String> sortedSkus);

    void updateAll(List<StockItem> stock);
}
