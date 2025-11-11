package com.sephora.services.ismsearchpoc.processing;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

/**
 * Self-contained audit logger that knows everything about its processing context and handles
 * automatic event ID generation, thread-local context management, and audit logging.
 * <p>
 * This class eliminates manual plumbing by encapsulating all context-aware behavior:
 * - Automatic processing event ID generation based on source type and context
 * - Thread-local context management for downstream business error logging inheritance
 * - Integration with existing ProcessingAuditHelper while adding context awareness
 * - Source-specific logging methods that understand their data patterns
 * <p>
 * The logger is created during context registration and bound to a specific ProcessingContextDefinition,
 * giving it knowledge of origin markers, audit sources, and context metadata that would otherwise
 * require manual setup by developers.
 * <p>
 * Example usage (the elegant one-line pattern):
 * <pre>
 * // Get context-aware audit logger - it knows everything about "POSLOG" context
 * var auditLogger = AppProcessingContext.getContext("POSLOG").getAudit();
 *
 * // One line - automatic event ID generation, context management, everything
 * auditLogger.logKafka(message, kafkaKey, () -&gt; {
 *     PosMessage posMessage = parseMessage(message);
 *     processEntirePosMessage(posMessage);
 * }, "processPosLogMessage");
 *
 * // Behind the scenes:
 * // 1. Generates event ID: "KAFKA:pos-log-topic:{kafkaKey}:{timestamp}"
 * // 2. Sets thread-local context with event ID for downstream step guards
 * // 3. Calls existing ProcessingAuditHelper with proper ProcessingContext
 * // 4. Ensures step guards inherit processing event ID automatically
 * // 5. Clears thread-local context when processing completes
 * </pre>
 * <p>
 * Thread safety: Immutable after construction, uses thread-local storage for context inheritance.
 * The underlying ProcessingAuditHelper handles concurrent access and transaction management.
 *
 * @author ISM Processing Framework
 * @see ProcessingContextDefinition
 * @see ProcessingAuditHelper
 * @see ProcessingContextManager
 * @since 1.0
 */
@Slf4j
@RequiredArgsConstructor
public class ContextualAuditLogger {

    private final ProcessingContextDefinition contextDefinition;
    private final ProcessingAuditHelper processingAuditHelper;
    private final String serviceName;

    // =====================================================================================
    // KAFKA PROCESSING
    // =====================================================================================

    /**
     * Logs Kafka message processing with automatic event ID generation.
     */
    public void logKafka(String message, String kafkaKey, Runnable businessLogic, String processingMethod) {
        String processingEventId = generateKafkaEventId(kafkaKey);
        executeWithContextManagement(message, processingEventId, businessLogic, processingMethod, ProcessingSource.KAFKA);
    }

    /**
     * Logs Kafka processing with return value support.
     */
    public <T> T logKafkaWithReturn(String message, String kafkaKey, Supplier<T> businessLogic, String processingMethod) {
        String processingEventId = generateKafkaEventId(kafkaKey);
        return executeWithContextManagementAndReturn(message, processingEventId, businessLogic, processingMethod, ProcessingSource.KAFKA);
    }

    // =====================================================================================
    // API PROCESSING
    // =====================================================================================

    /**
     * Logs API processing with automatic event ID generation.
     */
    public void logApi(String requestData, String batchId, Runnable businessLogic, String processingMethod) {
        String processingEventId = generateApiEventId(batchId);
        executeWithContextManagement(requestData, processingEventId, businessLogic, processingMethod, ProcessingSource.API);
    }

    /**
     * Logs API processing with return value support.
     */
    public <T> T logApiWithReturn(String requestData, String batchId, Supplier<T> businessLogic, String processingMethod) {
        String processingEventId = generateApiEventId(batchId);
        return executeWithContextManagementAndReturn(requestData, processingEventId, businessLogic, processingMethod, ProcessingSource.API);
    }

    // =====================================================================================
    // FILE PROCESSING
    // =====================================================================================

    /**
     * Logs file processing with automatic event ID generation.
     */
    public void logFile(String fileDescription, String fileName, Runnable businessLogic, String processingMethod) {
        String processingEventId = generateFileEventId(fileName);
        executeWithContextManagement(fileDescription, processingEventId, businessLogic, processingMethod, ProcessingSource.FILE);
    }

    /**
     * Logs file processing with return value support.
     */
    public <T> T logFileWithReturn(String fileDescription, String fileName, Supplier<T> businessLogic, String processingMethod) {
        String processingEventId = generateFileEventId(fileName);
        return executeWithContextManagementAndReturn(fileDescription, processingEventId, businessLogic, processingMethod, ProcessingSource.FILE);
    }

    // =====================================================================================
    // SCHEDULER PROCESSING
    // =====================================================================================

    /**
     * Logs scheduler job processing with automatic event ID generation.
     */
    public void logScheduler(String jobDescription, String jobKey, Runnable businessLogic, String processingMethod) {
        String processingEventId = generateSchedulerEventId(jobKey);
        executeWithContextManagement(jobDescription, processingEventId, businessLogic, processingMethod, ProcessingSource.SCHEDULER);
    }

    /**
     * Logs scheduler processing with return value support.
     */
    public <T> T logSchedulerWithReturn(String jobDescription, String jobKey, Supplier<T> businessLogic, String processingMethod) {
        String processingEventId = generateSchedulerEventId(jobKey);
        return executeWithContextManagementAndReturn(jobDescription, processingEventId, businessLogic, processingMethod, ProcessingSource.SCHEDULER);
    }

    // =====================================================================================
    // CORE EXECUTION WITH CONTEXT MANAGEMENT
    // =====================================================================================

    private void executeWithContextManagement(String content, String processingEventId,
                                              Runnable businessLogic, String processingMethod,
                                              ProcessingSource source) {

        // Create a complete ProcessingContext for thread-local storage
        ProcessingContext threadLocalContext = ProcessingContext.contextual(
                source,
                contextDefinition.getAuditSource(),
                content,
                serviceName,
                processingMethod,
                processingEventId,
                contextDefinition.getOriginMarker(),
                contextDefinition.getContextName()
        );

        // Set thread-local context for downstream step guards
        ProcessingContextManager.setCurrentContext(threadLocalContext);

        try {
            // Delegate to existing ProcessingAuditHelper with the same context
            processingAuditHelper.auditProcessingWithContext(threadLocalContext, businessLogic);
        } finally {
            ProcessingContextManager.clearContext();
        }
    }

    private <T> T executeWithContextManagementAndReturn(String content, String processingEventId,
                                                        Supplier<T> businessLogic, String processingMethod,
                                                        ProcessingSource source) {
        // Create a complete ProcessingContext for thread-local storage
        ProcessingContext threadLocalContext = ProcessingContext.contextual(
                source,
                contextDefinition.getAuditSource(),
                content,
                serviceName,
                processingMethod,
                processingEventId,
                contextDefinition.getOriginMarker(),
                contextDefinition.getContextName()
        );


        // Set thread-local context for downstream step guards
        ProcessingContextManager.setCurrentContext(threadLocalContext);

        try {
            // Delegate to existing ProcessingAuditHelper with the same context
            return processingAuditHelper.auditProcessingWithContextAndReturn(threadLocalContext, businessLogic);
        } finally {
            ProcessingContextManager.clearContext();
        }
    }

    // =====================================================================================
    // EVENT ID GENERATION
    // =====================================================================================
// Replaced these with more atomic event id generators : Ram

//    private String generateKafkaEventId(String kafkaKey) {
//        return String.format("KAFKA:%s:%s:%d",
//                contextDefinition.getAuditSource(),
//                kafkaKey != null ? kafkaKey : "unknown",
//                System.currentTimeMillis());
//    }
//
//    private String generateApiEventId(String batchId) {
//        return String.format("API:%s:%s:%d",
//                contextDefinition.getAuditSource(),
//                batchId != null ? batchId : "request",
//                System.currentTimeMillis());
//    }
//
//    private String generateFileEventId(String fileName) {
//        return String.format("FILE:%s:%d",
//                fileName != null ? fileName : "unknown-file",
//                System.currentTimeMillis());
//    }
//
//    private String generateSchedulerEventId(String jobKey) {
//        return String.format("SCHEDULER:%s:%s:%d",
//                contextDefinition.getContextName(),
//                jobKey != null ? jobKey : "job",
//                System.currentTimeMillis());
//    }

    private String generateKafkaEventId(String kafkaKey) {
        long currentTimeMillis = System.currentTimeMillis();
        long nanoTime = System.nanoTime();

        return String.format("KAFKA:%s:%s:%d-%06d",
                contextDefinition.getAuditSource(),
                kafkaKey != null ? kafkaKey : "unknown",
                currentTimeMillis,
                nanoTime % 1000000);  // Last 6 digits ensures uniqueness
    }

    private String generateApiEventId(String batchId) {
        long currentTimeMillis = System.currentTimeMillis();
        long nanoTime = System.nanoTime();

        return String.format("API:%s:%s:%d-%06d",
                contextDefinition.getAuditSource(),
                batchId != null ? batchId : "request",
                currentTimeMillis,
                nanoTime % 1000000);
    }

    private String generateFileEventId(String fileName) {
        long currentTimeMillis = System.currentTimeMillis();
        long nanoTime = System.nanoTime();

        return String.format("FILE:%s:%d-%06d",
                fileName != null ? fileName : "unknown-file",
                currentTimeMillis,
                nanoTime % 1000000);
    }

    private String generateSchedulerEventId(String jobKey) {
        long currentTimeMillis = System.currentTimeMillis();
        long nanoTime = System.nanoTime();

        return String.format("SCHEDULER:%s:%s:%d-%06d",
                contextDefinition.getContextName(),
                jobKey != null ? jobKey : "job",
                currentTimeMillis,
                nanoTime % 1000000);
    }


}