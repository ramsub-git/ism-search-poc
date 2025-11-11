package com.sephora.services.ismsearchpoc.processing;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Optional lifecycle hooks for processing framework operations.
 * All hooks are optional and null-safe.
 * <p>
 * Usage Example:
 * <pre>
 * ProcessingHooks hooks = ProcessingHooks.builder()
 *     .preProcessing(() -> validateInput())      // returns boolean
 *     .postProcessing(() -> cleanup())           // always runs
 *     .onSuccess(result -> notifySuccess())      // on success only
 *     .onError(ex -> handleError(ex))           // on error only
 *     .build();
 *
 * processingAuditHelper.auditKafkaProcessing(topic, message, businessLogic, "Operation", hooks);
 * </pre>
 */
@Builder
@Data
@Slf4j
public class ProcessingHooks {

    /**
     * Pre-processing validation/setup hook.
     * <p>
     * Flow Control:
     * - If returns FALSE: processing is skipped, outcome logged as SKIPPED
     * - If returns TRUE or NULL: processing continues normally
     * - If throws exception: exception is logged, processing continues
     * <p>
     * Use Cases:
     * - Input validation that should skip processing
     * - Resource availability checks
     * - Business rule pre-conditions
     *
     * @return Boolean - true/null to continue, false to skip processing
     */
    private Supplier<Boolean> preProcessing;

    /**
     * Post-processing cleanup hook.
     * <p>
     * Execution: Always runs (like finally block) regardless of:
     * - Success or failure of business logic
     * - Success or failure of other hooks
     * - Whether processing was skipped due to preProcessing
     * <p>
     * Use Cases:
     * - Resource cleanup (connections, files, etc.)
     * - Metric updates
     * - Cache invalidation
     * - Notification cleanup
     * <p>
     * Note: Exceptions in postProcessing are logged but do not affect main flow
     */
    private Runnable postProcessing;

    /**
     * Success notification hook.
     * <p>
     * Execution: Called ONLY when business logic completes successfully
     * Parameter:
     * - For methods with return values: receives the actual return value
     * - For void methods: receives null
     * <p>
     * Use Cases:
     * - Success notifications/alerts
     * - Downstream system updates
     * - Cache updates with successful results
     * - Metrics for successful operations
     * <p>
     * Note: Exceptions in onSuccess are logged but do not affect main flow
     */
    private Consumer<Object> onSuccess;

    /**
     * Error notification hook.
     * <p>
     * Execution: Called ONLY when business logic throws an exception
     * Parameter: The actual exception thrown by business logic
     * <p>
     * Use Cases:
     * - Custom error notifications/alerts
     * - Error-specific recovery actions
     * - Additional error logging/metrics
     * - Fallback data updates
     * <p>
     * Note:
     * - Exceptions in onError are logged but do not affect main flow
     * - Original business exception is always re-thrown regardless of onError outcome
     */
    private Consumer<Exception> onError;

    /**
     * Safely execute pre-processing hook with comprehensive error handling.
     *
     * @return true to continue processing, false to skip processing
     */
    public boolean executePreProcessing() {
        if (preProcessing == null) {
            return true;
        }

        try {
            Boolean result = preProcessing.get();
            if (result == null) {
                log.debug("PreProcessing hook returned null, treating as true (continue processing)");
                return true;
            }

            if (!result) {
                log.debug("PreProcessing hook returned false, skipping processing");
            }

            return result;

        } catch (Exception ex) {
            log.warn("PreProcessing hook failed with exception, continuing with processing: {}", ex.getMessage(), ex);
            return true; // On hook failure, err on side of continuing processing
        }
    }

    /**
     * Safely execute post-processing hook with comprehensive error handling.
     * Always runs regardless of other processing outcomes.
     */
    public void executePostProcessing() {
        if (postProcessing == null) {
            return;
        }

        try {
            postProcessing.run();
            log.debug("PostProcessing hook completed successfully");

        } catch (Exception ex) {
            log.warn("PostProcessing hook failed with exception (will not affect main processing flow): {}",
                    ex.getMessage(), ex);
            // Continue - postProcessing failures should never affect main flow
        }
    }

    /**
     * Safely execute success hook with comprehensive error handling.
     *
     * @param result The return value from successful business logic execution (null for void methods)
     */
    public void executeOnSuccess(Object result) {
        if (onSuccess == null) {
            return;
        }

        try {
            onSuccess.accept(result);
            log.debug("OnSuccess hook completed successfully for result: {}",
                    result != null ? result.getClass().getSimpleName() : "null");

        } catch (Exception ex) {
            log.warn("OnSuccess hook failed with exception (will not affect main processing flow): {}",
                    ex.getMessage(), ex);
            // Continue - success hook failures should never affect main flow
        }
    }

    /**
     * Safely execute error hook with comprehensive error handling.
     *
     * @param exception The original exception thrown by business logic
     */
    public void executeOnError(Exception exception) {
        if (onError == null) {
            return;
        }

        try {
            onError.accept(exception);
            log.debug("OnError hook completed successfully for exception: {}", exception.getClass().getSimpleName());

        } catch (Exception ex) {
            log.warn("OnError hook failed with exception (will not affect main processing flow): {}",
                    ex.getMessage(), ex);
            // Continue - error hook failures should never affect main flow or original exception
        }
    }

    /**
     * Utility method to check if any hooks are configured.
     * Useful for optimization - can skip hook processing entirely if no hooks present.
     *
     * @return true if at least one hook is configured, false if all hooks are null
     */
    public boolean hasAnyHooks() {
        return preProcessing != null || postProcessing != null || onSuccess != null || onError != null;
    }

    /**
     * Utility method for debugging - returns summary of configured hooks.
     *
     * @return String describing which hooks are configured
     */
    public String getHooksSummary() {
        StringBuilder summary = new StringBuilder("ProcessingHooks[");
        boolean hasAny = false;

        if (preProcessing != null) {
            summary.append("preProcessing");
            hasAny = true;
        }

        if (postProcessing != null) {
            if (hasAny) summary.append(", ");
            summary.append("postProcessing");
            hasAny = true;
        }

        if (onSuccess != null) {
            if (hasAny) summary.append(", ");
            summary.append("onSuccess");
            hasAny = true;
        }

        if (onError != null) {
            if (hasAny) summary.append(", ");
            summary.append("onError");
            hasAny = true;
        }

        if (!hasAny) {
            summary.append("none");
        }

        summary.append("]");
        return summary.toString();
    }
}