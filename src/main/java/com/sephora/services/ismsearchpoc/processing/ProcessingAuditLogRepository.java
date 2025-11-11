package com.sephora.services.ismsearchpoc.processing;

import java.time.LocalDateTime;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessingAuditLogRepository extends JpaRepository<ProcessingAuditLogEntry, Long> {

    @Query("SELECT COUNT(a) FROM ProcessingAuditLogEntry a WHERE a.processedBy = :serviceName " +
            "AND a.processingOutcome = :outcome AND a.timestamp >= :since")
    long countByServiceAndOutcomeSince(
            @Param("serviceName") String serviceName,
            @Param("outcome") ProcessingOutcome outcome,
            @Param("since") LocalDateTime since);
}