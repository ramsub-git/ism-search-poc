package com.sephora.services.ismsearchpoc.processing;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "processing_audit_log", indexes = {
        @Index(name = "idx_processing_audit_source_timestamp", columnList = "source, timestamp"),
        @Index(name = "idx_processing_audit_outcome", columnList = "processingOutcome"),
        @Index(name = "idx_processing_audit_service", columnList = "processedBy")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessingAuditLogEntry {

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

    @Lob
    @Column(name = "response_content", columnDefinition = "TEXT")
    private String responseContent;

    @Enumerated(EnumType.STRING)
    @Column(name = "processing_outcome", nullable = false, length = 20)
    private ProcessingOutcome processingOutcome;

    @Column(name = "comment", columnDefinition = "TEXT")
    private String comment;

    @Column(name = "processing_method", length = 100)
    private String processingMethod;

    @Column(name = "processed_by", length = 100)
    private String processedBy;

    @Column(name = "processing_time_ms")
    private Long processingTimeMs;

    @Column(name = "timestamp", nullable = false)
    private LocalDateTime timestamp;

    // =====================================================================================
    // NEW FIELD - PROCESSING EVENT ID FOR LINKING
    // =====================================================================================

    /**
     * Unique processing event ID that links this audit entry to related business errors.
     * Format: SOURCE:identifier:key:timestamp (e.g., KAFKA:pos-log-topic:ORDER123:1727258400000)
     * This enables complete traceability from audit entries to business error details.
     */
    @Column(name = "processing_event_id", length = 100)
    @Builder.Default
    private String processingEventId = "LEGACY";
}