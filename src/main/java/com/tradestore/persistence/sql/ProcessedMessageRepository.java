package com.tradestore.persistence.sql;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessageEntity, String> {
}
