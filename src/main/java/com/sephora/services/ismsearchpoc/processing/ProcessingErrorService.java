package com.sephora.services.ismsearchpoc.processing;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProcessingErrorService {

    private final ProcessingErrorLogRepository errorLogRepository;
    private final ProcessingLoggingProperties properties;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logError(ProcessingContext context, Exception ex, int retryCount) {
        if (!properties.isErrorLoggingEnabled()) {
            return;
        }

        try {
            SeverityLevel severity = determineSeverity(ex, context);

            ProcessingErrorLogEntry entry = ProcessingErrorLogEntry.builder()
                    .source(context.getSource())
                    .identifier(context.getIdentifier())
                    .subIdentifier(context.getSubIdentifier())
                    .content(ProcessingUtils.safeToString(context.getContent()))
                    .exceptionClass(ex.getClass().getName())
                    .exceptionMessage(ex.getMessage())
                    .stackTrace(ExceptionUtils.getStackTrace(ex))
                    .metadata(ProcessingUtils.objectToJson(context.getMetadata()))
                    .serviceName(properties.getServiceName())
                    .processingMethod(context.getProcessingMethod())
                    .severity(severity)
                    .timestamp(LocalDateTime.now())
                    .retryCount(retryCount)
                    .maxRetries(properties.getRetry().getMaxRetries())
                    .reprocessStatus(ProcessingErrorLogEntry.ReprocessStatus.NEW)
                    .alerted(false)
                    .build();

            errorLogRepository.save(entry);

            log.error("Processing error logged: {} - {} - {} attempts",
                    context.getSource() + ":" + context.getIdentifier(),
                    ex.getClass().getSimpleName(),
                    retryCount + 1);

        } catch (Exception loggingEx) {
            // CRITICAL: Never let logging failures break business logic
            log.error("CRITICAL PAGER LEVEL ERROR: Failed to log error to database. Original error: {} - Logging error: {}",
                    ex.getMessage(), loggingEx.getMessage(), loggingEx);

            // Consider: Alert monitoring system about logging failure
            // Consider: Fall back to file logging or external system
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logError(ProcessingContext context, Exception ex) {
        logError(context, ex, 0);
    }

    private SeverityLevel determineSeverity(Exception ex, ProcessingContext context) {
        // Financial/inventory operations are critical
        if (context.getProcessingMethod() != null &&
                (context.getProcessingMethod().toLowerCase().contains("financial") ||
                        context.getProcessingMethod().toLowerCase().contains("inventory") ||
                        context.getProcessingMethod().toLowerCase().contains("order"))) {
            return SeverityLevel.P0;
        }

        // Data integrity issues are critical
        if (ex instanceof org.springframework.dao.DataIntegrityViolationException ||
                ex instanceof jakarta.persistence.PersistenceException) {
            return SeverityLevel.P0;
        }

        // Database issues are high priority
        if (ex instanceof org.springframework.dao.DataAccessException) {
            return SeverityLevel.P0;
        }

        return properties.getDefaultSeverity();
    }

    /**
     * NEW METHOD: Log error with enhanced business context (for contextual framework).
     * Used by ContextualStepGuard to include business identifiers and policy information.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logBusinessError(ProcessingContext context, Exception ex, int retryCount,
                                 String businessIdentifier, String processingEventId,
                                 String originMarker, String errorPolicyCode,
                                 String escalationLevel, String responsibleTeam,
                                 SeverityLevel severity) {
        if (!properties.isErrorLoggingEnabled()) {
            return;
        }
        log.error("🔍 DEBUG IN ERROR SERVICE: Received processingEventId parameter = '{}'", processingEventId);

        try {
            ProcessingErrorLogEntry entry = ProcessingErrorLogEntry.builder()
                    .source(context.getSource())
                    .identifier(context.getIdentifier())
                    .subIdentifier(context.getSubIdentifier())
                    .content(ProcessingUtils.safeToString(context.getContent()))
                    .exceptionClass(ex.getClass().getName())
                    .exceptionMessage(ex.getMessage())
                    .stackTrace(ExceptionUtils.getStackTrace(ex))
                    .metadata(ProcessingUtils.objectToJson(context.getMetadata()))
                    .serviceName(properties.getServiceName())
                    .processingMethod(context.getProcessingMethod())
                    .severity(severity)
                    .timestamp(LocalDateTime.now())
                    .retryCount(retryCount)
                    .maxRetries(properties.getRetry().getMaxRetries())
                    .reprocessStatus(ProcessingErrorLogEntry.ReprocessStatus.NEW)
                    .alerted(false)
                    // NEW FIELDS for business context
                    .businessIdentifier(businessIdentifier)
                    .processingEventId(processingEventId)
                    .originMarker(originMarker)
                    .errorPolicyCode(errorPolicyCode)
                    .escalationLevel(escalationLevel)
                    .responsibleTeam(responsibleTeam)
                    .build();

            log.error("🔍 DEBUG: Entry processingEventId before save = '{}'", entry.getProcessingEventId());

            errorLogRepository.save(entry);
            errorLogRepository.flush(); // <-- This is super important. If this is not there - then tests will fail.
            log.error("Business error logged: {} - {} - {} attempts, Policy: {}, BusinessID: {}",
                    context.getSource() + ":" + context.getIdentifier(),
                    ex.getClass().getSimpleName(),
                    retryCount + 1,
                    errorPolicyCode,
                    businessIdentifier);

        } catch (Exception loggingEx) {
            log.error("CRITICAL PAGER LEVEL ERROR: Failed to log error to database. " +
                            "Original error: {}, Logging error: {}",
                    ex.getMessage(), loggingEx.getMessage(), loggingEx);
        }
    }

    
}