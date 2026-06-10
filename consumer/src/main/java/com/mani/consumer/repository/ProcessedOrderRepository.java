package com.mani.consumer.repository;

import com.mani.consumer.entity.ProcessedOrder;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

public interface ProcessedOrderRepository extends JpaRepository<ProcessedOrder, String> {
    @Modifying
    @Transactional
    @Query(
            value = """
                    INSERT INTO processed_orders(order_id, processed_at)
                    VALUES (:orderId, CURRENT_TIMESTAMP)
                    """,
            nativeQuery = true
    )
    void insertOrder(String orderId);
}
