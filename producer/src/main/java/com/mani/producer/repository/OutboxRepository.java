package com.mani.producer.repository;

import com.mani.producer.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, Integer> {
    List<OutboxEvent> findByPublishedFalse();
}
