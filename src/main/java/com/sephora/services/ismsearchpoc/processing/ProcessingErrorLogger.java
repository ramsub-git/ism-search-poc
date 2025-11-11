package com.sephora.services.ismsearchpoc.processing;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for simplified business error logging.
 * Provides one-line error logging with automatic exception handling.
 * <p>
 * Usage:
 * <pre>
 * ProcessingErrorLogger.logError("CONTEXT_NAME", "GUARD_NAME", entity,
 *                                "Error message", "methodName");
 * </pre>
 */
@Slf4j
public class ProcessingErrorLogger {

    /**
     * Log business error with entity, message, and method name.
     * Handles all exceptions internally - never throws.
     *
     * @param contextName    Processing context name (e.g., "SCM_PUBLISH_SCHEDULER")
     * @param guardName      Step guard name (e.g., "SCHEDULER_EXECUTION")
     * @param businessEntity Business entity to add to context (can be null)
     * @param errorMessage   Error message to log
     * @param methodName     Method where error occurred
     */
    public static void logError(String contextName, String guardName,
                                Object businessEntity, String errorMessage,
                                String methodName) {
        try {
            log.trace("ENTRY: logError - context: {}, guard: {}, method: {}",
                    contextName, guardName, methodName);

            // Add business entity if provided
            if (businessEntity != null) {
                ProcessingContextManager.addBusinessEntity(businessEntity);
            }

            // Get guard and log error
            var guard = AppProcessingContext.getContext(contextName)
                    .getStepGuard(guardName);

            guard.logError(errorMessage, methodName);

            log.trace("EXIT: logError - SUCCESS");

        } catch (Exception ex) {
            log.warn("Failed to log business error for context '{}', guard '{}': {}",
                    contextName, guardName, ex.getMessage());
        }
    }

    /**
     * Log business error with entity and message.
     * Method name is auto-detected from stack trace.
     *
     * @param contextName    Processing context name
     * @param guardName      Step guard name
     * @param businessEntity Business entity to add to context (can be null)
     * @param errorMessage   Error message to log
     */
    public static void logError(String contextName, String guardName,
                                Object businessEntity, String errorMessage) {
        String methodName = getCallingMethodName();
        logError(contextName, guardName, businessEntity, errorMessage, methodName);
    }

    /**
     * Log business error with message only (no entity).
     * Method name is auto-detected from stack trace.
     *
     * @param contextName  Processing context name
     * @param guardName    Step guard name
     * @param errorMessage Error message to log
     */
    public static void logError(String contextName, String guardName, String errorMessage) {
        logError(contextName, guardName, null, errorMessage);
    }

    /**
     * Log business error from exception.
     * Automatically extracts exception message and calling method.
     *
     * @param contextName    Processing context name
     * @param guardName      Step guard name
     * @param businessEntity Business entity to add to context (can be null)
     * @param exception      Exception to log
     */
    public static void logError(String contextName, String guardName,
                                Object businessEntity, Exception exception) {
        String methodName = getCallingMethodName();
        String errorMessage = exception.getClass().getSimpleName() + ": " + exception.getMessage();
        logError(contextName, guardName, businessEntity, errorMessage, methodName);
    }

    /**
     * Log business error from exception (no entity).
     *
     * @param contextName Processing context name
     * @param guardName   Step guard name
     * @param exception   Exception to log
     */
    public static void logError(String contextName, String guardName, Exception exception) {
        logError(contextName, guardName, null, exception);
    }

    /**
     * Extract calling method name from stack trace.
     * Skips over this utility class to find the actual caller.
     */
    private static String getCallingMethodName() {
        try {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

            // Skip: getStackTrace, getCallingMethodName, logError (this class)
            // Return the first method outside this class
            for (int i = 1; i < stackTrace.length; i++) {
                String className = stackTrace[i].getClassName();
                if (!className.equals(ProcessingErrorLogger.class.getName())) {
                    return stackTrace[i].getMethodName();
                }
            }

            return "unknown";

        } catch (Exception ex) {
            log.trace("Could not determine calling method name: {}", ex.getMessage());
            return "unknown";
        }
    }
}