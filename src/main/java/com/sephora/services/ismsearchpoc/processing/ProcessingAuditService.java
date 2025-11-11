package com.sephora.services.ismsearchpoc.processing;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProcessingAuditService {

    private final ProcessingAuditLogRepository auditLogRepository;
    private final ProcessingLoggingProperties properties;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    private void logProcessingOutcome(ProcessingContext context, ProcessingOutcome outcome, String comment,
                                      Long processingTime, Object responseData) {
        if (!properties.isAuditEnabled()) {
            return;
        }

        try {
            ProcessingAuditLogEntry entry = ProcessingAuditLogEntry.builder()
                    .source(context.getSource())
                    .identifier(context.getIdentifier())
                    // .subIdentifier(context.getSubIdentifier())
                    .subIdentifier(determineSubIdentifier(context))  // <-- Key if you want to get the business identifiers
                    .content(ProcessingUtils.safeToString(context.getContent()))
                    .responseContent(responseData != null ?
                            ProcessingUtils.safeToString(responseData) : null)
                    .processingOutcome(outcome)
                    .comment(comment)
                    .processingMethod(context.getProcessingMethod())
                    .processedBy(context.getServiceName())
                    .processingTimeMs(processingTime)
                    .timestamp(LocalDateTime.now())
                    .processingEventId(context.getProcessingEventId())
                    .build();

            auditLogRepository.save(entry);
            auditLogRepository.flush();  // <-- This is super important. If this is not there - then tests will fail.
            log.debug("Audit logged: {} - {} - {}", outcome, context.getSource(), context.getIdentifier());

        } catch (Exception ex) {
            log.error("CRITICAL PAGER LEVEL ERROR: Failed to save audit log entry", ex);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logSuccess(ProcessingContext context, long processingTime, Object responseData) {
        logProcessingOutcome(context, ProcessingOutcome.SUCCESS, null, processingTime, responseData);
    }


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logSuccess(ProcessingContext context, long processingTimeMs) {
        logProcessingOutcome(context, ProcessingOutcome.SUCCESS, "Processing completed successfully", processingTimeMs, null);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logSuccess(ProcessingContext context, String comment) {
        logProcessingOutcome(context, ProcessingOutcome.SUCCESS, comment, null, null);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logSkipped(ProcessingContext context, String reason) {
        logProcessingOutcome(context, ProcessingOutcome.SKIPPED, reason, null, null);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logFailure(ProcessingContext context, String reason) {
        logProcessingOutcome(context, ProcessingOutcome.FAILED, reason, null, null);
    }

    /**
     * Determine sub-identifier for audit log entry.
     * <p>
     * Uses accumulated business identifiers if available (populated during error processing),
     * otherwise falls back to original sub-identifier from context.
     * <p>
     * This ensures that failed records are identified in the audit log for troubleshooting,
     * while success cases continue to use the original sub-identifier (typically null or
     * partition key).
     *
     * @param context The processing context
     * @return Sub-identifier to store in audit log
     */
    private String determineSubIdentifier(ProcessingContext context) {
        log.trace("Determining sub-identifier for audit log");

        try {
            // Prefer accumulated business IDs (collected during error processing)
            String accumulatedIds = context.getAccumulatedBusinessIdentifiers();
            if (accumulatedIds != null && !accumulatedIds.trim().isEmpty()) {
                log.debug("Using accumulated business identifiers for sub_identifier: {}",
                        accumulatedIds);
                return accumulatedIds;
            }

            // Fallback to original sub-identifier (typically null for success cases)
            String originalSubId = context.getSubIdentifier();
            log.trace("No accumulated business IDs, using original sub_identifier: {}",
                    originalSubId);
            return originalSubId;

        } catch (Exception ex) {
            // CRITICAL: Never let sub-identifier determination break audit logging
            log.error("CRITICAL: Failed to determine sub-identifier, using fallback: {}",
                    ex.getMessage(), ex);
            return context.getSubIdentifier(); // Safe fallback
        }
    }

}