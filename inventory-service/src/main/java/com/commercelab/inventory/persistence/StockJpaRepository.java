package com.commercelab.inventory.persistence;

import jakarta.persistence.LockModeType;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface StockJpaRepository extends JpaRepository<StockEntity, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from StockEntity s where s.sku in :skus order by s.sku")
    List<StockEntity> lockBySkus(@Param("skus") Collection<String> skus);
}
