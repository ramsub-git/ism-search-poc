package com.sephora.services.ismsearchpoc.processing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.Supplier;


/**
 * Simplified ProcessingAuditHelper with eliminated redundancy.
 * <p>
 * Design:
 * - ONE generic method handles all core logic
 * - Simple wrapper methods provide developer-friendly API
 * - Maintains 100% backward compatibility
 * - Reduces 600+ lines to ~150 lines
 */

@Component
@Slf4j
public class ProcessingAuditHelper {

    @Autowired
    private ProcessingAuditService auditService;

    @Autowired
    private ProcessingErrorService errorService;

    @Autowired
    private ProcessingLoggingProperties properties;

    // =====================================================================================
    // CORE GENERIC METHODS (All logic consolidated here)
    // =====================================================================================

    /**
     * Generic audit processing method - all variants funnel through here.
     * This is the ONLY method with actual logic.
     */
    private void auditProcessing(ProcessingSource source, String identifier, String content,
                                 Runnable businessLogic, String processingMethod, ProcessingHooks hooks) {
        ProcessingContext context = ProcessingContext.builder()
                .source(source)
                .identifier(identifier)
                .content(content)
                .processingEventId("LEGACY")  // explicitly setting to LEGACY even though we have the defaults in place
                .serviceName(properties.getServiceName())
                .processingMethod(processingMethod)
                .build();

        if (hooks != null) {
            processWithAuditAndHooks(context, businessLogic, hooks);
        } else {
            processWithAudit(context, businessLogic);
        }
    }

    /**
     * Generic audit processing with return value.
     */
    private <T> T auditProcessingWithReturn(ProcessingSource source, String identifier, String content,
                                            Supplier<T> businessLogic, String processingMethod, ProcessingHooks hooks) {
        ProcessingContext context = ProcessingContext.builder()
                .source(source)
                .identifier(identifier)
                .content(content)
                .processingEventId("LEGACY") // explicitly setting to LEGACY even though we have the defaults in place
                .serviceName(properties.getServiceName())
                .processingMethod(processingMethod)
                .build();

        if (hooks != null) {
            return processWithAuditAndHooksAndReturn(context, businessLogic, hooks);
        } else {
            return processWithAuditAndReturn(context, businessLogic);
        }
    }

    // =====================================================================================
    // KAFKA PROCESSING - Simple wrappers (1-2 lines each)
    // =====================================================================================

    public void auditKafkaProcessing(String topic, String message, Runnable businessLogic, String processingMethod) {
        auditProcessing(ProcessingSource.KAFKA, topic, message, businessLogic, processingMethod, null);
    }

    public void auditKafkaProcessing(String topic, String message, Runnable businessLogic,
                                     String processingMethod, ProcessingHooks hooks) {
        auditProcessing(ProcessingSource.KAFKA, topic, message, businessLogic, processingMethod, hooks);
    }

    public <T> T auditKafkaProcessingWithReturn(String topic, String message, Supplier<T> businessLogic,
                                                String processingMethod) {
        return auditProcessingWithReturn(ProcessingSource.KAFKA, topic, message, businessLogic, processingMethod, null);
    }

    public <T> T auditKafkaProcessingWithReturn(String topic, String message, Supplier<T> businessLogic,
                                                String processingMethod, ProcessingHooks hooks) {
        return auditProcessingWithReturn(ProcessingSource.KAFKA, topic, message, businessLogic, processingMethod, hooks);
    }

    // =====================================================================================
    // SCHEDULER PROCESSING - Simple wrappers
    // =====================================================================================

    public void auditSchedulerProcessing(String jobName, String jobData, Runnable businessLogic, String processingMethod) {
        auditProcessing(ProcessingSource.SCHEDULER, jobName, jobData, businessLogic, processingMethod, null);
    }

    public void auditSchedulerProcessing(String jobName, String jobData, Runnable businessLogic,
                                         String processingMethod, ProcessingHooks hooks) {
        auditProcessing(ProcessingSource.SCHEDULER, jobName, jobData, businessLogic, processingMethod, hooks);
    }

    public <T> T auditSchedulerProcessingWithReturn(String jobName, String jobData, Supplier<T> businessLogic,
                                                    String processingMethod) {
        return auditProcessingWithReturn(ProcessingSource.SCHEDULER, jobName, jobData, businessLogic, processingMethod, null);
    }

    public <T> T auditSchedulerProcessingWithReturn(String jobName, String jobData, Supplier<T> businessLogic,
                                                    String processingMethod, ProcessingHooks hooks) {
        return auditProcessingWithReturn(ProcessingSource.SCHEDULER, jobName, jobData, businessLogic, processingMethod, hooks);
    }

    // =====================================================================================
    // FILE PROCESSING - Simple wrappers
    // =====================================================================================

    public void auditFileProcessing(String fileName, String fileData, Runnable businessLogic, String processingMethod) {
        auditProcessing(ProcessingSource.FILE, fileName, fileData, businessLogic, processingMethod, null);
    }

    public void auditFileProcessing(String fileName, String fileData, Runnable businessLogic,
                                    String processingMethod, ProcessingHooks hooks) {
        auditProcessing(ProcessingSource.FILE, fileName, fileData, businessLogic, processingMethod, hooks);
    }

    public <T> T auditFileProcessingWithReturn(String fileName, String fileData, Supplier<T> businessLogic,
                                               String processingMethod) {
        return auditProcessingWithReturn(ProcessingSource.FILE, fileName, fileData, businessLogic, processingMethod, null);
    }

    public <T> T auditFileProcessingWithReturn(String fileName, String fileData, Supplier<T> businessLogic,
                                               String processingMethod, ProcessingHooks hooks) {
        return auditProcessingWithReturn(ProcessingSource.FILE, fileName, fileData, businessLogic, processingMethod, hooks);
    }

    // =====================================================================================
    // API PROCESSING - Simple wrappers
    // =====================================================================================

    public void auditApiProcessing(String endpoint, String requestData, Runnable businessLogic, String processingMethod) {
        auditProcessing(ProcessingSource.API, endpoint, requestData, businessLogic, processingMethod, null);
    }

    public void auditApiProcessing(String endpoint, String requestData, Runnable businessLogic,
                                   String processingMethod, ProcessingHooks hooks) {
        auditProcessing(ProcessingSource.API, endpoint, requestData, businessLogic, processingMethod, hooks);
    }

    public <T> T auditApiProcessingWithReturn(String endpoint, String requestData, Supplier<T> businessLogic,
                                              String processingMethod) {
        return auditProcessingWithReturn(ProcessingSource.API, endpoint, requestData, businessLogic, processingMethod, null);
    }

    public <T> T auditApiProcessingWithReturn(String endpoint, String requestData, Supplier<T> businessLogic,
                                              String processingMethod, ProcessingHooks hooks) {
        return auditProcessingWithReturn(ProcessingSource.API, endpoint, requestData, businessLogic, processingMethod, hooks);
    }

    // =====================================================================================
    // OUTBOUND PROCESSING - Simple wrappers
    // =====================================================================================

    public void auditOutboundProcessing(String destination, String outboundData, Runnable businessLogic,
                                        String processingMethod) {
        auditProcessing(ProcessingSource.OUTBOUND, destination, outboundData, businessLogic, processingMethod, null);
    }

    public void auditOutboundProcessing(String destination, String outboundData, Runnable businessLogic,
                                        String processingMethod, ProcessingHooks hooks) {
        auditProcessing(ProcessingSource.OUTBOUND, destination, outboundData, businessLogic, processingMethod, hooks);
    }

    public <T> T auditOutboundProcessingWithReturn(String destination, String outboundData, Supplier<T> businessLogic,
                                                   String processingMethod) {
        return auditProcessingWithReturn(ProcessingSource.OUTBOUND, destination, outboundData, businessLogic, processingMethod, null);
    }

    public <T> T auditOutboundProcessingWithReturn(String destination, String outboundData, Supplier<T> businessLogic,
                                                   String processingMethod, ProcessingHooks hooks) {
        return auditProcessingWithReturn(ProcessingSource.OUTBOUND, destination, outboundData, businessLogic, processingMethod, hooks);
    }

    // =====================================================================================
    // CONTEXTUAL FRAMEWORK SUPPORT
    // These methods are used by ContextualAuditLogger to pass pre-built contexts
    // =====================================================================================

    /**
     * Audit processing with pre-built context (for contextual framework).
     * ContextualAuditLogger uses this to pass a context with processingEventId already set.
     */
    public void auditProcessingWithContext(ProcessingContext context, Runnable businessLogic) {
        processWithAudit(context, businessLogic);
    }

    /**
     * Audit processing with pre-built context and return value.
     */
    public <T> T auditProcessingWithContextAndReturn(ProcessingContext context, Supplier<T> businessLogic) {
        return processWithAuditAndReturn(context, businessLogic);
    }

    // =====================================================================================
    // CORE PROCESSING LOGIC (unchanged from original)
    // =====================================================================================

    private void processWithAudit(ProcessingContext context, Runnable businessLogic) {
        long startTime = System.currentTimeMillis();

        try {
            businessLogic.run();
            long processingTime = System.currentTimeMillis() - startTime;
            auditService.logSuccess(context, processingTime);

        } catch (Exception ex) {
            long processingTime = System.currentTimeMillis() - startTime;
            auditService.logFailure(context, ex.getMessage());
            // FIX: Only log error if using legacy API (not contextual API)
            // Contextual API with step guards handles error logging
            if ("LEGACY".equals(context.getProcessingEventId())) {
                errorService.logError(context, ex);
            }
            throw ex;
        }
    }

    private <T> T processWithAuditAndReturn(ProcessingContext context, Supplier<T> businessLogic) {
        long startTime = System.currentTimeMillis();

        try {
            T result = businessLogic.get();
            long processingTime = System.currentTimeMillis() - startTime;
            auditService.logSuccess(context, processingTime, result);
            return result;

        } catch (Exception ex) {
            long processingTime = System.currentTimeMillis() - startTime;
            auditService.logFailure(context, ex.getMessage());
            // FIX: Only log error if using legacy API (not contextual API)
            // Contextual API with step guards handles error logging
            if ("LEGACY".equals(context.getProcessingEventId())) {
                errorService.logError(context, ex);
            }
            throw ex;
        }
    }

    private void processWithAuditAndHooks(ProcessingContext context, Runnable businessLogic, ProcessingHooks hooks) {
        // Execute pre-processing hook (safe - exceptions handled inside)
        boolean shouldContinue = hooks.executePreProcessing();

        if (!shouldContinue) {
            // Pre-processing returned false - skip processing and log as SKIPPED
            auditService.logSkipped(context, "Pre-processing hook returned false");
            // CRITICAL: postProcessing MUST run even when skipped
            hooks.executePostProcessing();
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            businessLogic.run();
            long processingTime = System.currentTimeMillis() - startTime;

            // Execute success hook (safe - exceptions handled inside)
            hooks.executeOnSuccess(null);

            auditService.logSuccess(context, processingTime);

        } catch (Exception ex) {
            long processingTime = System.currentTimeMillis() - startTime;

            // Execute error hook (safe - exceptions handled inside)
            hooks.executeOnError(ex);

            auditService.logFailure(context, ex.getMessage());
            // FIX: Only log error if using legacy API (not contextual API)
            // Contextual API with step guards handles error logging
            if ("LEGACY".equals(context.getProcessingEventId())) {
                errorService.logError(context, ex);
            }
            throw ex; // Re-throw to maintain existing error handling behavior

        } finally {
            // CRITICAL: postProcessing ALWAYS runs (like finally block)
            hooks.executePostProcessing();
        }
    }

    private <T> T processWithAuditAndHooksAndReturn(ProcessingContext context, Supplier<T> businessLogic,
                                                    ProcessingHooks hooks) {
        // Execute pre-processing hook (safe - exceptions handled inside)
        boolean shouldContinue = hooks.executePreProcessing();

        if (!shouldContinue) {
            // Pre-processing returned false - skip processing and log as SKIPPED
            auditService.logSkipped(context, "Pre-processing hook returned false");
            // CRITICAL: postProcessing MUST run even when skipped
            hooks.executePostProcessing();
            return null;
        }

        long startTime = System.currentTimeMillis();

        try {
            T result = businessLogic.get();
            long processingTime = System.currentTimeMillis() - startTime;

            // Execute success hook with actual result (safe - exceptions handled inside)
            hooks.executeOnSuccess(result);

            auditService.logSuccess(context, processingTime, result);

            return result;

        } catch (Exception ex) {
            long processingTime = System.currentTimeMillis() - startTime;

            // Execute error hook (safe - exceptions handled inside)
            hooks.executeOnError(ex);

            auditService.logFailure(context, ex.getMessage());
            // FIX: Only log error if using legacy API (not contextual API)
            // Contextual API with step guards handles error logging
            if ("LEGACY".equals(context.getProcessingEventId())) {
                errorService.logError(context, ex);
            }
            throw ex; // Re-throw to maintain existing error handling behavior

        } finally {
            // CRITICAL: postProcessing ALWAYS runs (like finally block)
            hooks.executePostProcessing();
        }
    }
}