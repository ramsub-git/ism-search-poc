package com.sephora.services.ismsearchpoc.processing;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;
import java.time.LocalDateTime;

@Repository
public interface ProcessingErrorLogRepository extends JpaRepository<ProcessingErrorLogEntry, Long> {

    @Query("SELECT e FROM ProcessingErrorLogEntry e WHERE e.serviceName = :serviceName " +
            "AND e.timestamp >= :since ORDER BY e.timestamp DESC")
    List<ProcessingErrorLogEntry> findRecentErrorsByService(
            @Param("serviceName") String serviceName,
            @Param("since") LocalDateTime since);

    @Query("SELECT e FROM ProcessingErrorLogEntry e WHERE e.reprocessStatus = :status " +
            "ORDER BY e.timestamp ASC")
    List<ProcessingErrorLogEntry> findByReprocessStatus(
            @Param("status") ProcessingErrorLogEntry.ReprocessStatus status);

    long countByServiceNameAndSeverityAndTimestampGreaterThan(
            String serviceName, SeverityLevel severity, LocalDateTime since);
}