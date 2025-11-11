package com.sephora.services.ismsearchpoc.processing;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "processing_error_log", indexes = {
        @Index(name = "idx_processing_error_source_timestamp", columnList = "source, timestamp"),
        @Index(name = "idx_processing_error_service_severity", columnList = "service_name, severity"),
        @Index(name = "idx_processing_error_reprocess_status", columnList = "reprocess_status")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessingErrorLogEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(name = "source", nullable = false, length = 20)
    private ProcessingSource source;

    @Column(name = "identifier", nullable = false, length = 500)
    private String identifier;

    @Column(name = "sub_identifier", length = 200)
    private String subIdentifier;

    @Lob
    @Column(name = "content", columnDefinition = "TEXT")
    private String content;

    @Column(name = "exception_class", length = 500)
    private String exceptionClass;

    @Lob
    @Column(name = "exception_message", columnDefinition = "TEXT")
    private String exceptionMessage;

    @Lob
    @Column(name = "stack_trace", columnDefinition = "TEXT")
    private String stackTrace;

    @Lob
    @Column(name = "metadata", columnDefinition = "TEXT")
    private String metadata;

    @Column(name = "service_name", nullable = false, length = 100)
    private String serviceName;

    @Column(name = "processing_method", length = 100)
    private String processingMethod;

    @Enumerated(EnumType.STRING)
    @Column(name = "severity", nullable = false, length = 10)
    private SeverityLevel severity;

    @Column(name = "timestamp", nullable = false)
    private LocalDateTime timestamp;

    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;

    @Column(name = "max_retries", nullable = false)
    private Integer maxRetries = 4;

    @Enumerated(EnumType.STRING)
    @Column(name = "reprocess_status")
    @Builder.Default
    private ReprocessStatus reprocessStatus = ReprocessStatus.NEW;

    @Column(name = "reprocess_count")
    private Integer reprocessCount = 0;

    @Column(name = "reprocessed_by")
    private String reprocessedBy;

    @Column(name = "reprocessed_at")
    private LocalDateTime reprocessedAt;

    @Column(name = "alerted")
    @Builder.Default
    private Boolean alerted = false;

    @Column(name = "alert_id")
    private String alertId;

    @Lob
    @Column(name = "resolution_notes", columnDefinition = "TEXT")
    private String resolutionNotes;

    public enum ReprocessStatus {
        NEW, IN_PROGRESS, REPROCESSED, FAILED_AGAIN, SKIPPED, RESOLVED
    }

    @Column(name = "business_identifier", length = 100)
    private String businessIdentifier;

    /**
     * Processing event ID that links this error to the originating audit entry.
     * Format: SOURCE:identifier:key:timestamp (e.g., KAFKA:pos-log-topic:ORDER123:1727258400000)
     */
    @Column(name = "processing_event_id", length = 100)
    @Builder.Default
    private String processingEventId = "LEGACY";

    /**
     * Searchable marker in source code that identifies the processing origin.
     * Used for code traceability (e.g., INVENTORY_UPDATE_FROM_SAP).
     */
    @Column(name = "origin_marker", length = 100)
    private String originMarker;

    /**
     * Error policy code that defines the handling strategy for this error type.
     * Links to ErrorHandlingPolicy definitions (e.g., ERROR_SKU_UPDATE).
     */
    @Column(name = "error_policy_code", length = 50)
    private String errorPolicyCode;

    /**
     * Escalation level from the error handling policy (P1, P2, P3).
     * Determines the urgency and response procedures for this error.
     */
    @Column(name = "escalation_level", length = 20)
    private String escalationLevel;

    /**
     * Team responsible for handling this error type based on the policy.
     * Enables automatic routing and notification to the correct team.
     */
    @Column(name = "responsible_team", length = 100)
    private String responsibleTeam;
}