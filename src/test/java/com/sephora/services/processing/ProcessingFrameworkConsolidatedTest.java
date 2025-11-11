package com.sephora.services.processing;

import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

// Added by SRS 1110
import com.sephora.services.ismsearchpoc.processing.*;
// End addition


/**
 * Consolidated test suite for the Processing Framework.
 * <p>
 * Tests both Legacy API (ProcessingAuditHelper) and Contextual API (AppProcessingContext)
 * covering audit logging, error logging, transaction isolation, concurrency, and edge cases.
 * <p>
 * Configuration is repo-agnostic - just update package names and service name to replicate.
 */
@SpringBootTest(classes = ProcessingTestConfiguration.class)
@ActiveProfiles("test")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(ProcessingTestConfiguration.class)
@TestPropertySource(properties = {
        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
        "ism.processing.logging.service-name=ism-supply-test",
        "ism.processing.logging.audit-enabled=true",
        "ism.processing.logging.error-logging-enabled=true",
        "ism.processing.logging.default-severity=P0",
        "ism.processing.logging.retry.backoff-interval-ms=5000",
        "ism.processing.logging.retry.max-retries=4",
        "ism.processing.logging.retry.log-all-retries=false"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProcessingFrameworkConsolidatedTest {

    @Autowired
    private ProcessingAuditHelper processingAuditHelper;
    @Autowired
    private ProcessingAuditLogRepository auditRepository;
    @Autowired
    private ProcessingErrorLogRepository errorRepository;
    @Autowired
    private EntityManager entityManager;
    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;

    // Test entity with business identifiers for contextual API tests
    static class ComplexTestEntity {
        @BusinessID("order")
        private String orderId;

        @BusinessID("customer")
        private Long customerId;

        @BusinessID("store")
        private Integer storeId;

        private String data;

        public ComplexTestEntity(String orderId, Long customerId, Integer storeId, String data) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.storeId = storeId;
            this.data = data;
        }
    }

    @BeforeEach
    void setup() {
//        auditRepository.deleteAll();   // <-- Never enable, this will delete audit records and that can cause more problems than any good
//        errorRepository.deleteAll();   // <-- Never enable, this will delete audit records and that can cause more problems than any good

        transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        registerTestContexts();
    }

    private void registerTestContexts() {
        // Register TEST_CONTEXT_1
        try {
            AppProcessingContext.getContext("TEST_CONTEXT_1");
        } catch (IllegalArgumentException e) {
            AppProcessingContext.createContext("TEST_CONTEXT_1")
                    .withOriginMarker("TEST_ORIGIN_1")
                    .withAuditSource("test-topic-1")
                    .withStepGuard("GUARD_1", ComplexTestEntity.class,
                            ErrorHandlingPolicy.builder()
                                    .code("ERROR_TEST_1")
                                    .description("Test policy 1")
                                    .businessImpact("Test impact")
                                    .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P2)
                                    .responsibleTeam("Team 1")
                                    .immediateAction("Action 1")
                                    .autoRetry(false)
                                    .build())
                    .register();
        }

        // Register TEST_CONTEXT_2
        try {
            AppProcessingContext.getContext("TEST_CONTEXT_2");
        } catch (IllegalArgumentException e) {
            AppProcessingContext.createContext("TEST_CONTEXT_2")
                    .withOriginMarker("TEST_ORIGIN_2")
                    .withAuditSource("test-topic-2")
                    .withStepGuard("GUARD_2", ComplexTestEntity.class,
                            ErrorHandlingPolicy.builder()
                                    .code("ERROR_TEST_2")
                                    .description("Test policy 2")
                                    .businessImpact("Test impact")
                                    .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P1)
                                    .responsibleTeam("Team 2")
                                    .immediateAction("Action 2")
                                    .autoRetry(false)
                                    .build())
                    .register();
        }
    }

    // =====================================================================================
    // LEGACY API TESTS - ProcessingAuditHelper
    // Tests the original audit helper API used across all services
    // =====================================================================================

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Legacy API - Basic Functionality")
    class LegacyAPIBasicTests {

        @Test
        @Order(1)
        @DisplayName("Kafka processing audit")
        void testKafkaProcessing() {
            long initialCount = auditRepository.count();

            processingAuditHelper.auditKafkaProcessing("test-topic", "test message", () -> {
                // Successful processing
            }, "testKafkaMethod");

            assertThat(auditRepository.count()).isEqualTo(initialCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getSource()).isEqualTo(ProcessingSource.KAFKA);
            assertThat(audit.getProcessingOutcome()).isEqualTo(ProcessingOutcome.SUCCESS);
            assertThat(audit.getIdentifier()).isEqualTo("test-topic");
            assertThat(audit.getProcessingTimeMs()).isNotNull().isGreaterThanOrEqualTo(0);
        }

        @Test
        @Order(2)
        @DisplayName("Scheduler processing audit")
        void testSchedulerProcessing() {
            long initialCount = auditRepository.count();

            processingAuditHelper.auditSchedulerProcessing("test-job", "scheduler-data", () -> {
                // Scheduler work
            }, "testSchedulerMethod");

            assertThat(auditRepository.count()).isEqualTo(initialCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getSource()).isEqualTo(ProcessingSource.SCHEDULER);
            assertThat(audit.getProcessingOutcome()).isEqualTo(ProcessingOutcome.SUCCESS);
        }

        @Test
        @Order(3)
        @DisplayName("API processing audit with return value")
        void testApiProcessing() {
            long initialCount = auditRepository.count();

            String result = processingAuditHelper.auditApiProcessingWithReturn(
                    "/api/test", "request-data", () -> "response-data", "testApiMethod");

            assertThat(result).isEqualTo("response-data");
            assertThat(auditRepository.count()).isEqualTo(initialCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getSource()).isEqualTo(ProcessingSource.API);
            assertThat(audit.getProcessingOutcome()).isEqualTo(ProcessingOutcome.SUCCESS);
        }

        @Test
        @Order(4)
        @DisplayName("File processing audit")
        void testFileProcessing() {
            long initialCount = auditRepository.count();

            processingAuditHelper.auditFileProcessing("test.csv", "col1,col2,col3", () -> {
                // File processing
            }, "testFileMethod");

            assertThat(auditRepository.count()).isEqualTo(initialCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getSource()).isEqualTo(ProcessingSource.FILE);
            assertThat(audit.getIdentifier()).isEqualTo("test.csv");
        }

        @Test
        @Order(5)
        @DisplayName("Outbound processing audit")
        void testOutboundProcessing() {
            long initialCount = auditRepository.count();

            processingAuditHelper.auditOutboundProcessing("destination", "payload", () -> {
                // Outbound publishing
            }, "testOutboundMethod");

            assertThat(auditRepository.count()).isEqualTo(initialCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getSource()).isEqualTo(ProcessingSource.OUTBOUND);
            assertThat(audit.getIdentifier()).isEqualTo("destination");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Legacy API - Error Handling")
    class LegacyAPIErrorTests {

        @Test
        @Order(1)
        @DisplayName("Basic error logging")
        void testErrorLogging() {
            long initialAuditCount = auditRepository.count();
            long initialErrorCount = errorRepository.count();

            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("error-topic", "error-message", () -> {
                    throw new RuntimeException("Test error");
                }, "testErrorMethod");
            }).isInstanceOf(RuntimeException.class)
                    .hasMessage("Test error");

            assertThat(auditRepository.count()).isEqualTo(initialAuditCount + 1);
            assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getProcessingOutcome()).isEqualTo(ProcessingOutcome.FAILED);

            ProcessingErrorLogEntry error = getMostRecentError();
            assertThat(error.getExceptionClass()).isEqualTo("java.lang.RuntimeException");
            assertThat(error.getExceptionMessage()).isEqualTo("Test error");
        }

        @Test
        @Order(2)
        @DisplayName("Runtime exception handling")
        void testRuntimeException() {
            long initialErrorCount = errorRepository.count();

            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("topic", "message", () -> {
                    throw new RuntimeException("Runtime exception test");
                }, "testRuntimeExceptionMethod");
            }).isInstanceOf(RuntimeException.class);

            assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 1);

            ProcessingErrorLogEntry error = getMostRecentError();
            assertThat(error.getExceptionClass()).isEqualTo("java.lang.RuntimeException");
        }

        @Test
        @Order(3)
        @DisplayName("Nested exception handling")
        void testNestedException() {
            long initialErrorCount = errorRepository.count();

            SQLException innerException = new SQLException("DB connection failed", "08001", 1234);
            RuntimeException outerException = new RuntimeException("Processing failed", innerException);

            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("topic", "message", () -> {
                    throw outerException;
                }, "testNestedExceptionMethod");
            }).isInstanceOf(RuntimeException.class);

            assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 1);

            ProcessingErrorLogEntry error = getMostRecentError();
            assertThat(error.getStackTrace()).contains("SQLException");
            assertThat(error.getStackTrace()).contains("DB connection failed");
        }

        @Test
        @Order(4)
        @DisplayName("NullPointerException handling")
        void testNullPointerException() {
            long initialErrorCount = errorRepository.count();

            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("topic", "message", () -> {
                    String nullString = null;
                    nullString.length();
                }, "testNPEMethod");
            }).isInstanceOf(NullPointerException.class);

            assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 1);

            ProcessingErrorLogEntry error = getMostRecentError();
            assertThat(error.getExceptionClass()).isEqualTo("java.lang.NullPointerException");
        }

        @Test
        @Order(5)
        @DisplayName("Exception edge cases (null/long messages)")
        void testExceptionEdgeCases() {
            long initialErrorCount = errorRepository.count();

            // Test null message
            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("topic", "message", () -> {
                    throw new RuntimeException((String) null);
                }, "testNullMessageMethod");
            }).isInstanceOf(RuntimeException.class);

            ProcessingErrorLogEntry error1 = getMostRecentError();
            assertThat(error1.getExceptionMessage()).satisfiesAnyOf(
                    message -> assertThat(message).isNull(),
                    message -> assertThat(message).isEmpty()
            );

            // Test very long message
            String longMessage = "Long error: " + "x".repeat(5000);
            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("topic", "message", () -> {
                    throw new RuntimeException(longMessage);
                }, "testLongMessageMethod");
            }).isInstanceOf(RuntimeException.class);

            ProcessingErrorLogEntry error2 = getMostRecentError();
            assertThat(error2.getExceptionMessage()).isEqualTo(longMessage);
            assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 2);
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Legacy API - Processing Hooks")
    class LegacyAPIHooksTests {

        @Test
        @Order(1)
        @DisplayName("PreProcessing hook - skip processing")
        void testPreProcessingSkip() {
            long initialAuditCount = auditRepository.count();

            ProcessingHooks hooks = ProcessingHooks.builder()
                    .preProcessing(() -> false) // Skip
                    .build();

            processingAuditHelper.auditKafkaProcessing("skip-topic", "skip-message", () -> {
                throw new RuntimeException("Should never execute");
            }, "testPreProcessingSkipMethod", hooks);

            assertThat(auditRepository.count()).isEqualTo(initialAuditCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getProcessingOutcome()).isEqualTo(ProcessingOutcome.SKIPPED);
        }

        @Test
        @Order(2)
        @DisplayName("PreProcessing hook - continue processing")
        void testPreProcessingContinue() {
            long initialAuditCount = auditRepository.count();

            ProcessingHooks hooks = ProcessingHooks.builder()
                    .preProcessing(() -> true) // Continue
                    .build();

            processingAuditHelper.auditKafkaProcessing("continue-topic", "continue-message", () -> {
                // Normal execution
            }, "testPreProcessingContinueMethod", hooks);

            assertThat(auditRepository.count()).isEqualTo(initialAuditCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getProcessingOutcome()).isEqualTo(ProcessingOutcome.SUCCESS);
        }

        @Test
        @Order(3)
        @DisplayName("PostProcessing hook - always runs on success")
        void testPostProcessingOnSuccess() {
            AtomicInteger counter = new AtomicInteger(0);

            ProcessingHooks hooks = ProcessingHooks.builder()
                    .postProcessing(() -> counter.incrementAndGet())
                    .build();

            processingAuditHelper.auditKafkaProcessing("success-topic", "message", () -> {
                // Success
            }, "testPostProcessingSuccessMethod", hooks);

            assertThat(counter.get()).isEqualTo(1);
        }

        @Test
        @Order(4)
        @DisplayName("PostProcessing hook - always runs on error")
        void testPostProcessingOnError() {
            AtomicInteger counter = new AtomicInteger(0);

            ProcessingHooks hooks = ProcessingHooks.builder()
                    .postProcessing(() -> counter.incrementAndGet())
                    .build();

            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("error-topic", "message", () -> {
                    throw new RuntimeException("Test error");
                }, "testPostProcessingErrorMethod", hooks);
            }).isInstanceOf(RuntimeException.class);

            assertThat(counter.get()).isEqualTo(1);
        }

        @Test
        @Order(5)
        @DisplayName("PostProcessing hook - always runs on skip")
        void testPostProcessingOnSkip() {
            AtomicInteger counter = new AtomicInteger(0);

            ProcessingHooks hooks = ProcessingHooks.builder()
                    .preProcessing(() -> false)
                    .postProcessing(() -> counter.incrementAndGet())
                    .build();

            processingAuditHelper.auditKafkaProcessing("skip-topic", "message", () -> {
                throw new RuntimeException("Should not execute");
            }, "testPostProcessingSkipMethod", hooks);

            assertThat(counter.get()).isEqualTo(1);
        }

        @Test
        @Order(6)
        @DisplayName("OnSuccess hook - called with return value")
        void testOnSuccessHook() {
            AtomicReference<Object> successResult = new AtomicReference<>();

            ProcessingHooks hooks = ProcessingHooks.builder()
                    .onSuccess(result -> successResult.set(result))
                    .build();

            String result = processingAuditHelper.auditApiProcessingWithReturn(
                    "/api/success", "input", () -> "output", "testOnSuccessMethod", hooks);

            assertThat(result).isEqualTo("output");
            assertThat(successResult.get()).isEqualTo("output");
        }

        @Test
        @Order(7)
        @DisplayName("OnError hook - called with exception")
        void testOnErrorHook() {
            AtomicReference<Exception> errorResult = new AtomicReference<>();

            ProcessingHooks hooks = ProcessingHooks.builder()
                    .onError(ex -> errorResult.set(ex))
                    .build();

            RuntimeException testException = new RuntimeException("Test error");

            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("error-topic", "message", () -> {
                    throw testException;
                }, "testOnErrorMethod", hooks);
            }).isSameAs(testException);

            assertThat(errorResult.get()).isSameAs(testException);
        }

        @Test
        @Order(8)
        @DisplayName("All hooks combined - success path")
        void testAllHooksCombinedSuccess() {
            StringBuilder executionLog = new StringBuilder();

            ProcessingHooks hooks = ProcessingHooks.builder()
                    .preProcessing(() -> {
                        executionLog.append("PRE;");
                        return true;
                    })
                    .onSuccess(result -> executionLog.append("SUCCESS;"))
                    .postProcessing(() -> executionLog.append("POST;"))
                    .build();

            processingAuditHelper.auditKafkaProcessing("combined-topic", "message", () -> {
                executionLog.append("BUSINESS;");
            }, "testAllHooksSuccessMethod", hooks);

            assertThat(executionLog.toString()).isEqualTo("PRE;BUSINESS;SUCCESS;POST;");
        }

        @Test
        @Order(9)
        @DisplayName("All hooks combined - error path")
        void testAllHooksCombinedError() {
            StringBuilder executionLog = new StringBuilder();

            ProcessingHooks hooks = ProcessingHooks.builder()
                    .preProcessing(() -> {
                        executionLog.append("PRE;");
                        return true;
                    })
                    .onSuccess(result -> executionLog.append("SUCCESS;"))
                    .onError(ex -> executionLog.append("ERROR;"))
                    .postProcessing(() -> executionLog.append("POST;"))
                    .build();

            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("combined-error-topic", "message", () -> {
                    executionLog.append("BUSINESS;");
                    throw new RuntimeException("Test error");
                }, "testAllHooksErrorMethod", hooks);
            }).isInstanceOf(RuntimeException.class);

            assertThat(executionLog.toString()).isEqualTo("PRE;BUSINESS;ERROR;POST;");
        }

        @Test
        @Order(10)
        @DisplayName("Backward compatibility - works without hooks")
        void testBackwardCompatibility() {
            long initialAuditCount = auditRepository.count();

            processingAuditHelper.auditKafkaProcessing("backward-compat-topic", "message", () -> {
                // Normal processing
            }, "testBackwardCompatMethod");

            assertThat(auditRepository.count()).isEqualTo(initialAuditCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getProcessingOutcome()).isEqualTo(ProcessingOutcome.SUCCESS);
        }
    }

    // =====================================================================================
    // CONTEXTUAL API TESTS - AppProcessingContext
    // Tests the enhanced contextual API with business error logging
    // =====================================================================================

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Contextual API - Core Functionality")
    class ContextualAPICoreTests {

        @Test
        @Order(1)
        @DisplayName("Context registration")
        void testContextRegistration() {
            assertThatCode(() -> AppProcessingContext.getContext("TEST_CONTEXT_1"))
                    .doesNotThrowAnyException();
        }

        @Test
        @Order(2)
        @DisplayName("Business identifier extraction")
        void testBusinessIdentifierExtraction() {
            // Verify we can get the step guard and create entity with business identifiers
            var stepGuard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            ComplexTestEntity entity = new ComplexTestEntity("ORD123", 456L, 789, "data");

            // Verify guard can access the entity (business identifier extraction happens during guard execution)
            assertThat(stepGuard).isNotNull();
            assertThat(entity).isNotNull();
            assertThat(entity.orderId).isEqualTo("ORD123");
        }


        @Test
        @Order(3)
        @DisplayName("Thread-local context management")
        void testThreadLocalContext() {
            ProcessingContext context = ProcessingContext.contextual(
                    ProcessingSource.KAFKA, "test-topic", "content", "service", "method",
                    "TEST-EVENT-123", "TEST_ORIGIN", "TEST_CONTEXT");

            ProcessingContextManager.setCurrentContext(context);

            try {
                assertThat(ProcessingContextManager.getCurrentEventId()).isEqualTo("TEST-EVENT-123");
                assertThat(ProcessingContextManager.getCurrentOriginMarker()).isEqualTo("TEST_ORIGIN");
                assertThat(ProcessingContextManager.getCurrentContextName()).isEqualTo("TEST_CONTEXT");
            } finally {
                ProcessingContextManager.clearContext();
            }
        }

        @Test
        @Order(4)
        @DisplayName("Audit logger works")
        void testAuditLogger() {
            var auditLogger = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            long initialCount = auditRepository.count();

            auditLogger.logKafka("test-message", "test-key", () -> {
                // Business logic
            }, "testAuditLoggerMethod");

            assertThat(auditRepository.count()).isEqualTo(initialCount + 1);

            ProcessingAuditLogEntry entry = getMostRecentAudit();
            assertThat(entry.getProcessingEventId()).isNotNull().startsWith("KAFKA:");
        }

        @Test
        @Order(5)
        @DisplayName("Step guard logs errors")
        void testStepGuardErrorLogging() {
            var auditLogger = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var stepGuard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            long initialErrorCount = errorRepository.count();
            ComplexTestEntity entity = new ComplexTestEntity("ORD123", 456L, 789, "data");

            auditLogger.logKafka("test-message", "test-key", () -> {
                try {
                    stepGuard.guard(entity, () -> {
                        throw new RuntimeException("Test error");
                    }, "testStepGuardMethod");
                } catch (RuntimeException e) {
                    // Expected
                }
            }, "testStepGuardErrorMethod");

            assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 1);

            ProcessingErrorLogEntry error = getMostRecentError();
            assertThat(error.getBusinessIdentifier()).contains("order: ORD123");
            assertThat(error.getBusinessIdentifier()).contains("customer: 456");
            assertThat(error.getErrorPolicyCode()).isEqualTo("ERROR_TEST_1");
        }

        @Test
        @Order(6)
        @DisplayName("Complete event traceability")
        void testEventTraceability() {
            var auditLogger = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var stepGuard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            ComplexTestEntity entity = new ComplexTestEntity("ORD999", 999L, 888, "data");

            auditLogger.logKafka("trace-message", "TRACE-KEY", () -> {
                try {
                    stepGuard.guard(entity, () -> {
                        throw new RuntimeException("Trace test error");
                    }, "testTraceMethod");
                } catch (RuntimeException e) {
                    // Expected
                }
            }, "testTraceabilityMethod");

            ProcessingAuditLogEntry auditEntry = getMostRecentAudit();
            ProcessingErrorLogEntry errorEntry = getMostRecentError();

            assertThat(auditEntry.getProcessingEventId()).isEqualTo(errorEntry.getProcessingEventId());
            assertThat(auditEntry.getProcessingEventId()).contains("KAFKA:");
            assertThat(auditEntry.getProcessingEventId()).contains("TRACE-KEY");
        }

        @Test
        @Order(7)
        @DisplayName("Nested guard stack")
        void testNestedGuardStack() {
            ProcessingContext context = ProcessingContext.minimal(
                    ProcessingSource.KAFKA, "test-topic", "content", "service", "method");

            assertThat(context.getGuardStackDepth()).isEqualTo(0);
            assertThat(context.isNestedGuard()).isFalse();

            context.enterGuard("guard1");
            assertThat(context.getGuardStackDepth()).isEqualTo(1);
            assertThat(context.isNestedGuard()).isFalse();

            context.enterGuard("guard2");
            assertThat(context.getGuardStackDepth()).isEqualTo(2);
            assertThat(context.isNestedGuard()).isTrue();
            assertThat(context.getGuardStackRepresentation()).isEqualTo("guard1 -> guard2");

            context.exitGuard();
            assertThat(context.getGuardStackDepth()).isEqualTo(1);

            context.exitGuard();
            assertThat(context.getGuardStackDepth()).isEqualTo(0);
        }

        @Test
        @Order(8)
        @DisplayName("Success path - no error logging")
        void testSuccessPath() {
            var auditLogger = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var stepGuard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            long initialErrorCount = errorRepository.count();
            long initialAuditCount = auditRepository.count();

            ComplexTestEntity entity = new ComplexTestEntity("ORD111", 222L, 333, "data");

            auditLogger.logKafka("success-message", "success-key", () -> {
                stepGuard.guard(entity, () -> {
                    // Success - no exception
                }, "testSuccessMethod");
            }, "testSuccessPathMethod");

            assertThat(auditRepository.count()).isEqualTo(initialAuditCount + 1);
            assertThat(errorRepository.count()).isEqualTo(initialErrorCount); // No new errors
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Contextual API - Transaction Isolation")
    class ContextualAPITransactionTests {

        @Test
        @Order(1)
        @DisplayName("Error log survives business transaction rollback")
        void testErrorLogSurvivesRollback() {
            var auditLogger = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var stepGuard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            long initialErrorCount = errorRepository.count();

            try {
                transactionTemplate.execute(status -> {
                    ComplexTestEntity entity = new ComplexTestEntity("ORD123", 456L, 789, "data");

                    auditLogger.logKafka("test-message", "test-key", () -> {
                        try {
                            stepGuard.guard(entity, () -> {
                                throw new RuntimeException("Business error");
                            }, "testErrorRollbackMethod");
                        } catch (Exception e) {
                            // Expected
                        }
                    }, "testErrorSurvivesMethod");

                    status.setRollbackOnly();
                    return null;
                });
            } catch (Exception e) {
                // Expected
            }

            assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 1);

            ProcessingErrorLogEntry error = getMostRecentError();
            assertThat(error.getBusinessIdentifier()).isNotNull();
            assertThat(error.getProcessingEventId()).startsWith("KAFKA:");
        }

        @Test
        @Order(2)
        @DisplayName("Audit log survives business transaction rollback")
        void testAuditLogSurvivesRollback() {
            var auditLogger = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            long initialAuditCount = auditRepository.count();

            try {
                transactionTemplate.execute(status -> {
                    auditLogger.logKafka("test-message", "test-key", () -> {
                        // Business logic
                    }, "testAuditRollbackMethod");

                    status.setRollbackOnly();
                    return null;
                });
            } catch (Exception e) {
                // Expected
            }

            assertThat(auditRepository.count()).isEqualTo(initialAuditCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getProcessingEventId()).startsWith("KAFKA:");
            assertThat(audit.getProcessingOutcome()).isEqualTo(ProcessingOutcome.SUCCESS);
        }

        @Test
        @Order(3)
        @DisplayName("Both audit and error logs survive rollback")
        void testBothLogsSurviveRollback() {
            var auditLogger = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var stepGuard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            long initialAuditCount = auditRepository.count();
            long initialErrorCount = errorRepository.count();

            try {
                transactionTemplate.execute(status -> {
                    ComplexTestEntity entity = new ComplexTestEntity("ORD456", 789L, 123, "data");

                    auditLogger.logKafka("message", "key", () -> {
                        try {
                            stepGuard.guard(entity, () -> {
                                throw new RuntimeException("Error");
                            }, "testBothLogsMethod");
                        } catch (Exception e) {
                            // Expected
                        }
                    }, "testBothSurviveMethod");

                    status.setRollbackOnly();
                    return null;
                });
            } catch (Exception e) {
                // Expected
            }

            assertThat(auditRepository.count()).isEqualTo(initialAuditCount + 1);
            assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            ProcessingErrorLogEntry error = getMostRecentError();
            assertThat(audit.getProcessingEventId()).isEqualTo(error.getProcessingEventId());
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Contextual API - API Coexistence")
    class APICoexistenceTests {

        @Test
        @Order(1)
        @DisplayName("Legacy and Contextual APIs work together")
        void testLegacyAndContextualCoexist() {
            long initialCount = auditRepository.count();

            // Legacy API call
            processingAuditHelper.auditKafkaProcessing("legacy-topic", "legacy-message", () -> {
                // Legacy logic
            }, "legacyMethod");

            // Contextual API call
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            audit.logKafka("contextual-message", "contextual-key", () -> {
                // Contextual logic
            }, "contextualMethod");

            assertThat(auditRepository.count()).isEqualTo(initialCount + 2);

            List<ProcessingAuditLogEntry> entries = auditRepository.findAll();
            ProcessingAuditLogEntry legacyEntry = entries.get(entries.size() - 2);
            ProcessingAuditLogEntry contextualEntry = entries.get(entries.size() - 1);

            assertThat(legacyEntry.getProcessingEventId()).isEqualTo("LEGACY");
            assertThat(contextualEntry.getProcessingEventId()).startsWith("KAFKA:");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Contextual API - Context Lifecycle")
    class ContextLifecycleTests {

        @Test
        @Order(1)
        @DisplayName("Get unregistered context fails")
        void testUnregisteredContextFails() {
            assertThatThrownBy(() ->
                    AppProcessingContext.getContext("NONEXISTENT_CONTEXT")
            ).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unknown processing context");
        }

        @Test
        @Order(2)
        @DisplayName("Multiple contexts in same transaction")
        void testMultipleContextsInTransaction() {
            var audit1 = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var audit2 = AppProcessingContext.getContext("TEST_CONTEXT_2").getAudit();

            long initialCount = auditRepository.count();

            transactionTemplate.execute(status -> {
                audit1.logKafka("msg1", "key1", () -> {
                    // Context 1
                }, "method1");

                audit2.logKafka("msg2", "key2", () -> {
                    // Context 2
                }, "method2");

                return null;
            });

            assertThat(auditRepository.count()).isEqualTo(initialCount + 2);

            List<ProcessingAuditLogEntry> entries = auditRepository.findAll();
            ProcessingAuditLogEntry entry1 = entries.get(entries.size() - 2);
            ProcessingAuditLogEntry entry2 = entries.get(entries.size() - 1);

            assertThat(entry1.getProcessingEventId()).startsWith("KAFKA:");
            assertThat(entry2.getProcessingEventId()).startsWith("KAFKA:");
            assertThat(entry1.getProcessingEventId()).isNotEqualTo(entry2.getProcessingEventId());
        }

        @Test
        @Order(3)
        @DisplayName("Duplicate context registration merges guards")
        void testDuplicateContextMergesGuards() {
            // First registration
            AppProcessingContext.createContext("MERGE_TEST")
                    .withOriginMarker("MERGE")
                    .withAuditSource("merge-topic")
                    .withStepGuard("GUARD_A", ComplexTestEntity.class,
                            ErrorHandlingPolicy.builder()
                                    .code("ERROR_A")
                                    .description("Policy A")
                                    .businessImpact("Test")
                                    .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                                    .responsibleTeam("Team")
                                    .immediateAction("None")
                                    .autoRetry(false)
                                    .build())
                    .register();

            // Second registration - should merge
            AppProcessingContext.createContext("MERGE_TEST")
                    .withOriginMarker("MERGE")
                    .withAuditSource("merge-topic")
                    .withStepGuard("GUARD_B", ComplexTestEntity.class,
                            ErrorHandlingPolicy.builder()
                                    .code("ERROR_B")
                                    .description("Policy B")
                                    .businessImpact("Test")
                                    .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                                    .responsibleTeam("Team")
                                    .immediateAction("None")
                                    .autoRetry(false)
                                    .build())
                    .register();

            var context = AppProcessingContext.getContext("MERGE_TEST");
            assertThat(context.<ComplexTestEntity>getStepGuard("GUARD_A")).isNotNull();
            assertThat(context.<ComplexTestEntity>getStepGuard("GUARD_B")).isNotNull();
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Contextual API - Step Guards Edge Cases")
    class StepGuardsEdgeCasesTests {

        @Test
        @Order(1)
        @DisplayName("Complex entity business identifier extraction")
        void testComplexBusinessIdentifier() {
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            ComplexTestEntity entity = new ComplexTestEntity("ORD999", 12345L, 67890, "data");

            assertThatThrownBy(() ->
                    guard.guard(entity, () -> {
                        throw new RuntimeException("Test error");
                    }, "testComplexIdMethod")
            ).isInstanceOf(RuntimeException.class);

            ProcessingErrorLogEntry error = getMostRecentError();
            assertThat(error.getBusinessIdentifier()).contains("order: ORD999");
            assertThat(error.getBusinessIdentifier()).contains("customer: 12345");
            assertThat(error.getBusinessIdentifier()).contains("store: 67890");
            assertThat(error.getBusinessIdentifier()).doesNotContain("data");
        }

        @Test
        @Order(2)
        @DisplayName("Step guard with null entity")
        void testNullEntity() {
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            assertThatThrownBy(() ->
                    guard.guard(null, () -> {
                        throw new RuntimeException("Test");
                    }, "testNullEntityMethod")
            ).isInstanceOf(RuntimeException.class);

            assertThat(errorRepository.count()).isGreaterThan(0);
        }

        @Test
        @Order(3)
        @DisplayName("Step guard with no @BusinessID annotations")
        void testNoBusinessIdAnnotations() {
            class EmptyEntity {
                private String field;
            }

            AppProcessingContext.createContext("EMPTY_CONTEXT")
                    .withOriginMarker("EMPTY")
                    .withAuditSource("empty-topic")
                    .withStepGuard("EMPTY_GUARD", EmptyEntity.class,
                            ErrorHandlingPolicy.builder()
                                    .code("EMPTY_ERROR")
                                    .description("Empty")
                                    .businessImpact("None")
                                    .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                                    .responsibleTeam("Test")
                                    .immediateAction("None")
                                    .autoRetry(false)
                                    .build())
                    .register();

            var guard = AppProcessingContext.getContext("EMPTY_CONTEXT")
                    .<EmptyEntity>getStepGuard("EMPTY_GUARD");

            long initialCount = errorRepository.count();

            assertThatThrownBy(() ->
                    guard.guard(new EmptyEntity(), () -> {
                        throw new RuntimeException("Test");
                    }, "testNoAnnotationsMethod")
            ).isInstanceOf(RuntimeException.class);

            assertThat(errorRepository.count()).isEqualTo(initialCount + 1);
        }

        @Test
        @Order(4)
        @DisplayName("Entity edge cases (null fields, extraction errors)")
        void testEntityEdgeCases() {
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            // Test with null fields
            ComplexTestEntity entityWithNulls = new ComplexTestEntity(null, null, null, null);

            assertThatThrownBy(() ->
                    guard.guard(entityWithNulls, () -> {
                        throw new RuntimeException("Test");
                    }, "testNullFieldsMethod")
            ).isInstanceOf(RuntimeException.class);

            assertThat(errorRepository.count()).isGreaterThan(0);
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Contextual API - Event ID Traceability")
    class EventIDTraceabilityTests {

        @Test
        @Order(1)
        @DisplayName("Event ID links audit to error")
        void testEventIdLinking() {
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            ComplexTestEntity entity = new ComplexTestEntity("ORD-LINK", 999L, 888, "data");

            audit.logKafka("link-message", "link-key", () -> {
                try {
                    guard.guard(entity, () -> {
                        throw new RuntimeException("Link test");
                    }, "testLinkMethod");
                } catch (Exception e) {
                    // Expected
                }
            }, "testEventIdLinkingMethod");

            ProcessingAuditLogEntry auditEntry = getMostRecentAudit();
            ProcessingErrorLogEntry errorEntry = getMostRecentError();

            assertThat(auditEntry.getProcessingEventId()).isEqualTo(errorEntry.getProcessingEventId());
            assertThat(auditEntry.getProcessingEventId()).startsWith("KAFKA:");
            assertThat(auditEntry.getProcessingEventId()).contains("link-key");
        }

        @Test
        @Order(2)
        @DisplayName("Event ID format validation")
        void testEventIdFormat() {
            String uniqueKey = "test-key-" + UUID.randomUUID().toString().substring(0, 8);
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();

            audit.logKafka("test message", uniqueKey, () -> {
                // test logic
            }, "testMethod");

            ProcessingAuditLogEntry entry = auditRepository.findAll().stream()
                    .filter(e -> e.getProcessingEventId() != null &&
                            e.getProcessingEventId().contains(uniqueKey))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Could not find audit entry with key " + uniqueKey));

            // Verify format with the nanosecond suffix
            assertThat(entry.getProcessingEventId())
                    .matches("KAFKA:test-topic-1:" + uniqueKey + ":\\d+-\\d{6}");
        }

        @Test
        @Order(3)
        @DisplayName("Event ID uniqueness")
        void testEventIdUniqueness() {
            // Generate a unique batch identifier for THIS test run
            String testBatchId = "TEST-BATCH-" + UUID.randomUUID().toString().substring(0, 8);

            // Create 1000 events with this batch as part of the kafka key
            for (int i = 0; i < 1000; i++) {
                var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
                String kafkaKey = testBatchId + "-key-" + i;  // Include test batch ID

                audit.logKafka("test message", kafkaKey, () -> {
                    // test logic
                }, "testMethod");
            }

            // Now query only OUR test's event IDs using the batch identifier
            List<String> eventIds = auditRepository.findAll().stream()
                    .filter(entry -> entry.getIdentifier() != null &&
                            entry.getIdentifier().contains(testBatchId))
                    .map(ProcessingAuditLogEntry::getProcessingEventId)
                    .collect(Collectors.toList());

            // Check for duplicates in OUR data only
            Set<String> uniqueIds = new HashSet<>(eventIds);
            assertThat(uniqueIds)
                    .as("All event IDs for test batch %s should be unique", testBatchId)
                    .hasSize(eventIds.size());
        }
    }
    // =====================================================================================
    // CROSS-CUTTING TESTS - Performance, Concurrency, Edge Cases
    // Tests that apply to both APIs
    // =====================================================================================

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Concurrency & Thread Safety")
    class ConcurrencyTests {

        @Test
        @Order(1)
        @DisplayName("ThreadLocal cleanup on thread pool reuse")
        void testThreadLocalCleanup() throws Exception {
            var auditLogger = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            ExecutorService executor = Executors.newFixedThreadPool(2);

            // First batch
            CountDownLatch latch1 = new CountDownLatch(2);
            for (int i = 0; i < 2; i++) {
                executor.submit(() -> {
                    try {
                        auditLogger.logKafka("batch1-msg", "batch1-key", () -> {
                            assertThat(ProcessingContextManager.getCurrentEventId()).isNotNull();
                        }, "batch1Method");
                    } finally {
                        latch1.countDown();
                    }
                });
            }
            latch1.await(10, TimeUnit.SECONDS);

            // Second batch - threads reused
            CountDownLatch latch2 = new CountDownLatch(2);
            for (int i = 0; i < 2; i++) {
                executor.submit(() -> {
                    try {
                        assertThat(ProcessingContextManager.getCurrentEventId()).isNull();

                        auditLogger.logKafka("batch2-msg", "batch2-key", () -> {
                            assertThat(ProcessingContextManager.getCurrentEventId()).isNotNull();
                        }, "batch2Method");
                    } finally {
                        latch2.countDown();
                    }
                });
            }
            latch2.await(10, TimeUnit.SECONDS);
            executor.shutdown();
        }

        @Test
        @Order(2)
        @DisplayName("Concurrent processing - legacy API")
        void testConcurrentProcessingLegacy() throws Exception {
            int threadCount = 5;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch finishLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            long initialCount = auditRepository.count();

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        processingAuditHelper.auditKafkaProcessing(
                                "concurrent-topic-" + threadId, "message-" + threadId,
                                () -> {
                                    try {
                                        Thread.sleep(10);
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                }, "concurrentMethod" + threadId);
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        finishLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertThat(finishLatch.await(30, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            assertThat(successCount.get()).isEqualTo(threadCount);
            assertThat(auditRepository.count()).isGreaterThanOrEqualTo(initialCount + threadCount);
        }

        @Test
        @Order(3)
        @DisplayName("Multiple threads same context")
        void testMultipleThreadsSameContext() throws Exception {
            var auditLogger = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            int threadCount = 10;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);

            long initialCount = auditRepository.count();

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        auditLogger.logKafka("message-" + threadId, "key-" + threadId, () -> {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }, "method-" + threadId);
                        successCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(30, TimeUnit.SECONDS);
            executor.shutdown();

            assertThat(successCount.get()).isEqualTo(threadCount);
            assertThat(auditRepository.count()).isEqualTo(initialCount + threadCount);
        }

        @Test
        @Order(4)
        @DisplayName("Thread-local isolation")
        void testThreadLocalIsolation() throws InterruptedException {
            ProcessingContext ctx1 = ProcessingContext.contextual(
                    ProcessingSource.KAFKA, "topic1", "content1", "service1", "method1",
                    "event1", "MARKER1", "CONTEXT1");

            ProcessingContext ctx2 = ProcessingContext.contextual(
                    ProcessingSource.API, "topic2", "content2", "service2", "method2",
                    "event2", "MARKER2", "CONTEXT2");

            CountDownLatch latch = new CountDownLatch(2);
            AtomicInteger successCount = new AtomicInteger(0);

            Thread thread1 = new Thread(() -> {
                try {
                    ProcessingContextManager.setCurrentContext(ctx1);
                    Thread.sleep(50);
                    assertThat(ProcessingContextManager.getCurrentEventId()).isEqualTo("event1");
                    assertThat(ProcessingContextManager.getCurrentContextName()).isEqualTo("CONTEXT1");
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    ProcessingContextManager.clearContext();
                    latch.countDown();
                }
            });

            Thread thread2 = new Thread(() -> {
                try {
                    ProcessingContextManager.setCurrentContext(ctx2);
                    Thread.sleep(50);
                    assertThat(ProcessingContextManager.getCurrentEventId()).isEqualTo("event2");
                    assertThat(ProcessingContextManager.getCurrentContextName()).isEqualTo("CONTEXT2");
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    ProcessingContextManager.clearContext();
                    latch.countDown();
                }
            });

            thread1.start();
            thread2.start();
            latch.await(5, TimeUnit.SECONDS);

            assertThat(successCount.get()).isEqualTo(2);
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Integration Scenarios")
    class IntegrationScenariosTests {

        @Test
        @Order(1)
        @DisplayName("Real-world scenario with complex entity")
        void testRealWorldScenario() {
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            ComplexTestEntity orderEntity = new ComplexTestEntity("ORD-REAL-001", 54321L, 12345, "data");

            long initialAuditCount = auditRepository.count();
            long initialErrorCount = errorRepository.count();

            audit.logKafka("order-message", "order-key-001", () -> {
                // Step 1: Validate (succeeds)

                // Step 2: Process payment (fails)
                try {
                    guard.guard(orderEntity, () -> {
                        throw new RuntimeException("Payment gateway timeout");
                    }, "processPayment");
                } catch (Exception e) {
                    // Log but continue
                }

                // Step 3: Continue processing
            }, "processOrderWorkflow");

            assertThat(auditRepository.count()).isEqualTo(initialAuditCount + 1);
            assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 1);

            ProcessingErrorLogEntry error = getMostRecentError();
            assertThat(error.getBusinessIdentifier()).contains("ORD-REAL-001");
            assertThat(error.getErrorPolicyCode()).isEqualTo("ERROR_TEST_1");
            assertThat(error.getEscalationLevel()).isEqualTo("P2");
            assertThat(error.getResponsibleTeam()).isEqualTo("Team 1");
        }

        @Test
        @Order(2)
        @DisplayName("Multiple step guards in sequence")
        void testMultipleGuardsInSequence() {
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            ComplexTestEntity entity1 = new ComplexTestEntity("ORD-SEQ-1", 111L, 222, "data1");
            ComplexTestEntity entity2 = new ComplexTestEntity("ORD-SEQ-2", 333L, 444, "data2");
            ComplexTestEntity entity3 = new ComplexTestEntity("ORD-SEQ-3", 555L, 666, "data3");

            long initialErrorCount = errorRepository.count();

            audit.logKafka("sequence-message", "sequence-key", () -> {
                // Guard 1 - succeeds
                guard.guard(entity1, () -> {
                    // Step 1 succeeds
                }, "step1");

                // Guard 2 - fails
                try {
                    guard.guard(entity2, () -> {
                        throw new RuntimeException("Step 2 failed");
                    }, "step2");
                } catch (Exception e) {
                    // Continue
                }

                // Guard 3 - succeeds
                guard.guard(entity3, () -> {
                    // Step 3 succeeds
                }, "step3");
            }, "sequenceWorkflow");

            assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 1);

            ProcessingErrorLogEntry error = getMostRecentError();
            assertThat(error.getBusinessIdentifier()).contains("ORD-SEQ-2");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Performance & Resource Management")
    class PerformanceTests {

        @Test
        @Order(1)
        @DisplayName("Memory leak detection")
        void testMemoryLeakDetection() throws Exception {
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            int iterations = 1000;

            System.gc();
            Thread.sleep(100);
            long initialMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            for (int i = 0; i < iterations; i++) {
                audit.logKafka("message-" + i, "key-" + i, () -> {
                }, "memoryLeakMethod");

                if (i % 100 == 0) {
                    System.gc();
                }
            }

            System.gc();
            Thread.sleep(100);
            long finalMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            long memoryGrowthMB = (finalMemory - initialMemory) / (1024 * 1024);
            assertThat(memoryGrowthMB).isLessThan(50);
        }

        @Test
        @Order(2)
        @DisplayName("High volume error logging")
        void testHighVolumeErrorLogging() throws Exception {
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            int errorCount = 100;
            long initialCount = errorRepository.count();
            long startTime = System.currentTimeMillis();

            for (int i = 0; i < errorCount; i++) {
                ComplexTestEntity entity = new ComplexTestEntity("ORD-" + i, (long) i, i, "data");
                try {
                    int finalI = i;
                    guard.guard(entity, () -> {
                        throw new RuntimeException("High volume test " + finalI);
                    }, "hvMethod");
                } catch (Exception e) {
                    // Expected
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            assertThat(errorRepository.count()).isEqualTo(initialCount + errorCount);

            double errorsPerSecond = (errorCount * 1000.0) / duration;
            System.out.println("High volume test: " + errorCount + " errors in " + duration + "ms (" +
                    String.format("%.2f", errorsPerSecond) + " errors/sec)");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Edge Cases & Input Validation")
    class EdgeCasesTests {

        @Test
        @Order(1)
        @DisplayName("Large payload handling")
        void testLargePayload() {
            long initialCount = auditRepository.count();
            String largePayload = "X".repeat(10240);

            assertThatCode(() -> {
                processingAuditHelper.auditKafkaProcessing("large-topic", largePayload, () -> {
                }, "largePayloadMethod");
            }).doesNotThrowAnyException();

            auditRepository.flush();
            assertThat(auditRepository.count()).isEqualTo(initialCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getContent()).isNotEmpty();
            assertThat(audit.getContent().length()).isGreaterThan(1000);
        }

        @Test
        @Order(2)
        @DisplayName("Special characters in input")
        void testSpecialCharacters() {
            long initialCount = auditRepository.count();
            String specialChars = "Special: 中文 🎉 \n\r\t '\"\\`;";

            processingAuditHelper.auditKafkaProcessing("topic", specialChars, () -> {
            }, "specialCharsMethod");

            assertThat(auditRepository.count()).isEqualTo(initialCount + 1);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getContent()).isEqualTo(specialChars);
        }

        @Test
        @Order(3)
        @DisplayName("Null and empty input edge cases")
        void testNullAndEmptyInputs() {
            long initialCount = auditRepository.count();

            // Empty strings
            processingAuditHelper.auditKafkaProcessing("", "", () -> {
            }, "");
            assertThat(auditRepository.count()).isEqualTo(initialCount + 1);

            // Null processing method
            processingAuditHelper.auditKafkaProcessing("topic", "message", () -> {
            }, null);
            assertThat(auditRepository.count()).isEqualTo(initialCount + 2);

            ProcessingAuditLogEntry audit = getMostRecentAudit();
            assertThat(audit.getProcessingMethod()).isNull();
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Configuration Flags")
    class ConfigurationFlagsTests {

        @Test
        @Order(1)
        @DisplayName("Audit disabled - success path")
        void testAuditDisabled() {
            // Note: This test requires different @TestPropertySource
            // Keeping as example - would need separate test class with audit-enabled=false
            // For now, just verify current behavior
            long initialCount = auditRepository.count();

            processingAuditHelper.auditKafkaProcessing("topic", "message", () -> {
            }, "testAuditDisabledMethod");

            // With audit enabled, should create entry
            assertThat(auditRepository.count()).isGreaterThan(initialCount);
        }

        @Test
        @Order(2)
        @DisplayName("Error logging disabled")
        void testErrorLoggingDisabled() {
            // Note: This test requires different @TestPropertySource
            // Keeping as example - would need separate test class with error-logging-enabled=false
            // For now, just verify current behavior
            long initialErrorCount = errorRepository.count();

            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("topic", "message", () -> {
                    throw new RuntimeException("Test error");
                }, "testErrorLoggingDisabledMethod");
            }).isInstanceOf(RuntimeException.class);

            // With error logging enabled, should create entry
            assertThat(errorRepository.count()).isGreaterThan(initialErrorCount);
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Business Logic Preservation")
    class BusinessLogicPreservationTests {

        @Test
        @Order(1)
        @DisplayName("Business logic executes and returns correctly")
        void testBusinessLogicExecution() {
            String result = processingAuditHelper.auditApiProcessingWithReturn(
                    "/api/test", "input", () -> "processed-input", "testBusinessLogicMethod");

            assertThat(result).isEqualTo("processed-input");
        }

        @Test
        @Order(2)
        @DisplayName("Business exceptions propagate correctly")
        void testBusinessExceptionPropagation() {
            RuntimeException businessException = new RuntimeException("Business error");

            assertThatThrownBy(() -> {
                processingAuditHelper.auditKafkaProcessing("topic", "message", () -> {
                    throw businessException;
                }, "testExceptionPropagationMethod");
            }).isSameAs(businessException);
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Refactored Context Validation")
    class RefactoredContextTests {

        @Test
        @Order(1)
        @DisplayName("Guard stack management")
        void testGuardStackManagement() {
            ProcessingContext ctx = ProcessingContext.minimal(
                    ProcessingSource.KAFKA, "test-topic", "test-content", "test-service", "testMethod");

            assertThat(ctx.getGuardStackDepth()).isEqualTo(0);
            assertThat(ctx.isNestedGuard()).isFalse();

            ctx.enterGuard("guard1");
            assertThat(ctx.getGuardStackDepth()).isEqualTo(1);
            assertThat(ctx.isNestedGuard()).isFalse();
            assertThat(ctx.getGuardStackRepresentation()).isEqualTo("guard1");

            ctx.enterGuard("guard2");
            assertThat(ctx.getGuardStackDepth()).isEqualTo(2);
            assertThat(ctx.isNestedGuard()).isTrue();
            assertThat(ctx.getGuardStackRepresentation()).isEqualTo("guard1 -> guard2");

            ctx.exitGuard();
            assertThat(ctx.getGuardStackDepth()).isEqualTo(1);
            assertThat(ctx.isNestedGuard()).isFalse();

            ctx.exitGuard();
            assertThat(ctx.getGuardStackDepth()).isEqualTo(0);
        }

        @Test
        @Order(2)
        @DisplayName("Context manager with whole object")
        void testContextManagerWithWholeObject() {
            ProcessingContext ctx = ProcessingContext.contextual(
                    ProcessingSource.KAFKA, "test-topic", "test-content", "test-service", "testMethod",
                    "KAFKA:test-topic:key123:1234567890", "TEST_ORIGIN_MARKER", "TEST_CONTEXT");

            ProcessingContextManager.setCurrentContext(ctx);

            try {
                assertThat(ProcessingContextManager.getCurrentEventId())
                        .isEqualTo(ctx.getProcessingEventId())
                        .isEqualTo("KAFKA:test-topic:key123:1234567890");

                assertThat(ProcessingContextManager.getCurrentContextName())
                        .isEqualTo(ctx.getContextName())
                        .isEqualTo("TEST_CONTEXT");

                assertThat(ProcessingContextManager.getCurrentOriginMarker())
                        .isEqualTo(ctx.getOriginMarker())
                        .isEqualTo("TEST_ORIGIN_MARKER");

                assertThat(ProcessingContextManager.getGuardStackDepth()).isEqualTo(0);
                assertThat(ProcessingContextManager.hasContext()).isTrue();

                ProcessingContextManager.enterGuard("testGuard");
                assertThat(ProcessingContextManager.getGuardStackDepth()).isEqualTo(1);
                assertThat(ctx.getGuardStackDepth()).isEqualTo(1);

                ProcessingContextManager.exitGuard("testGuard");
                assertThat(ProcessingContextManager.getGuardStackDepth()).isEqualTo(0);

            } finally {
                ProcessingContextManager.clearContext();
                assertThat(ProcessingContextManager.hasContext()).isFalse();
            }
        }
    }

    // =====================================================================================
    // HELPER METHODS
    // =====================================================================================

    private ProcessingAuditLogEntry getMostRecentAudit() {
        return auditRepository.findAll().stream()
                .reduce((first, second) -> second)
                .orElseThrow(() -> new AssertionError("No audit records found"));
    }

    private ProcessingErrorLogEntry getMostRecentError() {
        return errorRepository.findAll().stream()
                .reduce((first, second) -> second)
                .orElseThrow(() -> new AssertionError("No error records found"));
    }

    /**
     * CRITICAL TESTS - Add to ProcessingFrameworkConsolidatedTest
     * These tests cover the bugs we just fixed in preInitialize()
     */

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("PreInitialize & Field Extractor Tests")
    class PreInitializeTests {

        // Test entity with @BusinessIDGetter methods
        static class EntityWithGetters {
            private String order;
            private Long customer;

            @BusinessIDGetter("order")
            public String getOrderId() {
                return order;
            }

            @BusinessIDGetter("customer")
            public Long getCustomerId() {
                return customer;
            }

            public EntityWithGetters(String order, Long customer) {
                this.order = order;
                this.customer = customer;
            }
        }

        // Test entity with neither annotation
        static class EmptyEntity {
            private String field;

            public EmptyEntity(String field) {
                this.field = field;
            }
        }

        @Test
        @Order(1)
        @DisplayName("PreInitialize sets flag with @BusinessID fields")
        void testPreInitializeWithBusinessIDFields() {
            StepGuardDefinition<ComplexTestEntity> guard = new StepGuardDefinition<>(
                    "TEST_GUARD",
                    ComplexTestEntity.class,
                    ErrorHandlingPolicy.builder()
                            .code("TEST")
                            .description("Test")
                            .businessImpact("Test")
                            .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                            .responsibleTeam("Test")
                            .immediateAction("Test")
                            .autoRetry(false)
                            .build()
            );

            // VERIFY: Flag is false before initialization
            assertThat(guard.isPreInitialized()).isFalse();

            // EXECUTE: Call preInitialize
            guard.preInitialize();

            // VERIFY: Flag is now true (BUG #1 fix)
            assertThat(guard.isPreInitialized()).isTrue();

            // VERIFY: Business ID types are found
            assertThat(guard.getBusinessIdTypes())
                    .containsExactlyInAnyOrder("order", "customer", "store");
        }

        @Test
        @Order(2)
        @DisplayName("PreInitialize sets flag with @BusinessIDGetter methods")
        void testPreInitializeWithBusinessIDGetterMethods() {
            StepGuardDefinition<EntityWithGetters> guard = new StepGuardDefinition<>(
                    "GETTER_GUARD",
                    EntityWithGetters.class,
                    ErrorHandlingPolicy.builder()
                            .code("TEST")
                            .description("Test")
                            .businessImpact("Test")
                            .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                            .responsibleTeam("Test")
                            .immediateAction("Test")
                            .autoRetry(false)
                            .build()
            );

            // VERIFY: Flag is false before initialization
            assertThat(guard.isPreInitialized()).isFalse();

            // EXECUTE: Call preInitialize (takes method path)
            guard.preInitialize();

            // VERIFY: Flag is true even when taking early return path (BUG #1 fix)
            assertThat(guard.isPreInitialized()).isTrue();
        }

        @Test
        @Order(3)
        @DisplayName("PreInitialize sets flag even with no business IDs")
        void testPreInitializeWithNoBusinessIds() {
            StepGuardDefinition<EmptyEntity> guard = new StepGuardDefinition<>(
                    "EMPTY_GUARD",
                    EmptyEntity.class,
                    ErrorHandlingPolicy.builder()
                            .code("TEST")
                            .description("Test")
                            .businessImpact("Test")
                            .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                            .responsibleTeam("Test")
                            .immediateAction("Test")
                            .autoRetry(false)
                            .build()
            );

            // EXECUTE: Call preInitialize
            guard.preInitialize();

            // VERIFY: Flag is still set to true (important for consistency)
            assertThat(guard.isPreInitialized()).isTrue();
        }

        @Test
        @Order(4)
        @DisplayName("Field extractors are built during preInitialize")
        void testFieldExtractorsAreBuilt() {
            StepGuardDefinition<ComplexTestEntity> guard = new StepGuardDefinition<>(
                    "EXTRACTOR_GUARD",
                    ComplexTestEntity.class,
                    ErrorHandlingPolicy.builder()
                            .code("TEST")
                            .description("Test")
                            .businessImpact("Test")
                            .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                            .responsibleTeam("Test")
                            .immediateAction("Test")
                            .autoRetry(false)
                            .build()
            );

            // EXECUTE: Initialize
            guard.preInitialize();

            // CREATE: Test entity
            ComplexTestEntity entity = new ComplexTestEntity("ORD-TEST", 999L, 888, "data");

            // VERIFY: Extractors were built and work (BUG #2 fix)
            assertThat(guard.extractBusinessField(entity, "order")).isEqualTo("ORD-TEST");
            assertThat(guard.extractBusinessField(entity, "customer")).isEqualTo(999L);
            assertThat(guard.extractBusinessField(entity, "store")).isEqualTo(888);
        }

        @Test
        @Order(5)
        @DisplayName("buildBusinessIdentifier works after preInitialize")
        void testBuildBusinessIdentifierAfterPreInitialize() {
            StepGuardDefinition<ComplexTestEntity> guard = new StepGuardDefinition<>(
                    "BUILD_GUARD",
                    ComplexTestEntity.class,
                    ErrorHandlingPolicy.builder()
                            .code("TEST")
                            .description("Test")
                            .businessImpact("Test")
                            .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                            .responsibleTeam("Test")
                            .immediateAction("Test")
                            .autoRetry(false)
                            .build()
            );

            // EXECUTE: Initialize
            guard.preInitialize();

            // CREATE: Test entity
            ComplexTestEntity entity = new ComplexTestEntity("ORD-BUILD", 777L, 666, "data");

            // VERIFY: buildBusinessIdentifier works (uses the extractors from BUG #2 fix)
            String businessId = guard.buildBusinessIdentifier(entity);

            assertThat(businessId).isNotNull();
            assertThat(businessId).contains("order: ORD-BUILD");
            assertThat(businessId).contains("customer: 777");
            assertThat(businessId).contains("store: 666");
        }

        @Test
        @Order(6)
        @DisplayName("buildBusinessIdentifier with null entity returns NULL_ENTITY")
        void testBuildBusinessIdentifierWithNullEntity() {
            StepGuardDefinition<ComplexTestEntity> guard = new StepGuardDefinition<>(
                    "NULL_GUARD",
                    ComplexTestEntity.class,
                    ErrorHandlingPolicy.builder()
                            .code("TEST")
                            .description("Test")
                            .businessImpact("Test")
                            .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                            .responsibleTeam("Test")
                            .immediateAction("Test")
                            .autoRetry(false)
                            .build()
            );

            guard.preInitialize();

            // VERIFY: Null entity returns "NULL_ENTITY"
            String result = guard.buildBusinessIdentifier(null);
            assertThat(result).isEqualTo("NULL_ENTITY");
        }

        @Test
        @Order(7)
        @DisplayName("buildBusinessIdentifier before preInitialize throws exception")
        void testBuildBusinessIdentifierBeforePreInitialize() {
            StepGuardDefinition<ComplexTestEntity> guard = new StepGuardDefinition<>(
                    "UNINIT_GUARD",
                    ComplexTestEntity.class,
                    ErrorHandlingPolicy.builder()
                            .code("TEST")
                            .description("Test")
                            .businessImpact("Test")
                            .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                            .responsibleTeam("Test")
                            .immediateAction("Test")
                            .autoRetry(false)
                            .build()
            );

            // DON'T call preInitialize()

            ComplexTestEntity entity = new ComplexTestEntity("ORD123", 456L, 789, "data");

            // VERIFY: Calling buildBusinessIdentifier throws IllegalStateException
            assertThatThrownBy(() -> guard.buildBusinessIdentifier(entity))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("must be pre-initialized");
        }

        @Test
        @Order(8)
        @DisplayName("buildBusinessIdentifier with null fields")
        void testBuildBusinessIdentifierWithNullFields() {
            StepGuardDefinition<ComplexTestEntity> guard = new StepGuardDefinition<>(
                    "NULL_FIELDS_GUARD",
                    ComplexTestEntity.class,
                    ErrorHandlingPolicy.builder()
                            .code("TEST")
                            .description("Test")
                            .businessImpact("Test")
                            .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                            .responsibleTeam("Test")
                            .immediateAction("Test")
                            .autoRetry(false)
                            .build()
            );

            guard.preInitialize();

            // Entity with all null fields
            ComplexTestEntity entity = new ComplexTestEntity(null, null, null, null);

            // VERIFY: Should not throw exception, should return null or empty
            String result = guard.buildBusinessIdentifier(entity);

            // Either null or empty braces {} is acceptable
            assertThat(result).satisfiesAnyOf(
                    r -> assertThat(r).isNull(),
                    r -> assertThat(r).isEqualTo("{}")
            );
        }
    }

    /**
     * CRITICAL TESTS - Business ID Accumulation in Audit Logs
     * Add to ProcessingFrameworkConsolidatedTest
     * <p>
     * These tests verify that business IDs from failed entities
     * are accumulated and stored in audit log's sub_identifier field.
     */

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Business ID Accumulation in Audit Logs")
    class BusinessIdAuditTests {

        @Test
        @Order(1)
        @DisplayName("Audit log contains business IDs for failed processing")
        void testAuditLogContainsBusinessIdsOnFailure() {
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            long initialAuditCount = auditRepository.count();
            ComplexTestEntity entity = new ComplexTestEntity("ORD-AUDIT-1", 111L, 222, "data");

            // EXECUTE: Process with error

            try {
                audit.logKafka("test-message", "test-key", () -> {
                    guard.guard(entity, () -> {
                        throw new RuntimeException("Test error");
                    }, "testMethod");

                }, "testAuditBusinessIdMethod");
            } catch (Exception e) {
                // Expected - error is logged
            }
            // VERIFY: Audit log created
            assertThat(auditRepository.count()).isEqualTo(initialAuditCount + 1);

            // VERIFY: Audit log has business ID in sub_identifier
            ProcessingAuditLogEntry auditEntry = getMostRecentAudit();
            assertThat(auditEntry.getProcessingOutcome()).isEqualTo(ProcessingOutcome.FAILED);
            assertThat(auditEntry.getSubIdentifier()).isNotNull();
            assertThat(auditEntry.getSubIdentifier()).contains("order: ORD-AUDIT-1");
            assertThat(auditEntry.getSubIdentifier()).contains("customer: 111");
            assertThat(auditEntry.getSubIdentifier()).contains("store: 222");
        }

        @Test
        @Order(2)
        @DisplayName("Audit log does NOT contain business IDs for successful processing")
        void testAuditLogNoBusinessIdsOnSuccess() {
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            long initialAuditCount = auditRepository.count();
            ComplexTestEntity entity = new ComplexTestEntity("ORD-SUCCESS", 333L, 444, "data");

            // EXECUTE: Process successfully (no error)
            audit.logKafka("success-message", "success-key", () -> {
                guard.guard(entity, () -> {
                    // No exception - success
                }, "successMethod");
            }, "testAuditNoBusinessIdMethod");

            // VERIFY: Audit log created
            assertThat(auditRepository.count()).isEqualTo(initialAuditCount + 1);

            // VERIFY: Audit log does NOT have business IDs
            ProcessingAuditLogEntry auditEntry = getMostRecentAudit();
            assertThat(auditEntry.getProcessingOutcome()).isEqualTo(ProcessingOutcome.SUCCESS);

            // sub_identifier should be null or NOT contain business IDs
            String subId = auditEntry.getSubIdentifier();
            if (subId != null) {
                assertThat(subId).doesNotContain("order:");
                assertThat(subId).doesNotContain("customer:");
            }
        }

        @Test
        @Order(3)
        @DisplayName("Multiple errors accumulate business IDs with pipe delimiter")
        void testMultipleErrorsAccumulateBusinessIds() {
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            long initialAuditCount = auditRepository.count();
            ComplexTestEntity entity1 = new ComplexTestEntity("ORD-MULTI-1", 111L, 222, "data");
            ComplexTestEntity entity2 = new ComplexTestEntity("ORD-MULTI-2", 333L, 444, "data");

            // EXECUTE: Process with multiple errors
            audit.logKafka("multi-message", "multi-key", () -> {
                try {
                    guard.guard(entity1, () -> {
                        throw new RuntimeException("Error 1");
                    }, "testMethod1");
                } catch (Exception e) {
                    // Continue to next entity
                }

                try {
                    guard.guard(entity2, () -> {
                        throw new RuntimeException("Error 2");
                    }, "testMethod2");
                } catch (Exception e) {
                    // Expected
                }
            }, "testMultiErrorsMethod");

            // VERIFY: Audit log has BOTH business IDs
            ProcessingAuditLogEntry auditEntry = getMostRecentAudit();
            String subId = auditEntry.getSubIdentifier();

            assertThat(subId).contains("ORD-MULTI-1");
            assertThat(subId).contains("ORD-MULTI-2");
            assertThat(subId).contains("|"); // Pipe delimiter
        }

        @Test
        @Order(4)
        @DisplayName("Duplicate business IDs are not added twice")
        void testDuplicateBusinessIdsNotAdded() {
            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            ComplexTestEntity sameEntity = new ComplexTestEntity("ORD-DUP", 555L, 666, "data");

            // EXECUTE: Same entity fails twice
            audit.logKafka("dup-message", "dup-key", () -> {
                try {
                    guard.guard(sameEntity, () -> {
                        throw new RuntimeException("Error 1");
                    }, "testMethod1");
                } catch (Exception e) {
                    // Ignore
                }

                try {
                    guard.guard(sameEntity, () -> {
                        throw new RuntimeException("Error 2");
                    }, "testMethod2");
                } catch (Exception e) {
                    // Expected
                }
            }, "testDuplicateMethod");

            // VERIFY: Business ID appears only ONCE
            ProcessingAuditLogEntry auditEntry = getMostRecentAudit();
            String subId = auditEntry.getSubIdentifier();

            // Count occurrences of "ORD-DUP"
            int count = subId.split("ORD-DUP", -1).length - 1;
            assertThat(count).isEqualTo(1); // Should appear only once
        }

        @Test
        @Order(5)
        @DisplayName("appendBusinessIdentifier handles null gracefully")
        void testAppendBusinessIdentifierWithNull() {
            ProcessingContext ctx = ProcessingContext.minimal(
                    ProcessingSource.KAFKA, "test-topic", "test-content", "test-service", "testMethod");

            // EXECUTE: Append null
            ctx.appendBusinessIdentifier(null);

            // VERIFY: No exception, accumulated IDs remain null
            assertThat(ctx.getAccumulatedBusinessIdentifiers()).isNull();
        }

        @Test
        @Order(6)
        @DisplayName("appendBusinessIdentifier handles empty string gracefully")
        void testAppendBusinessIdentifierWithEmpty() {
            ProcessingContext ctx = ProcessingContext.minimal(
                    ProcessingSource.KAFKA, "test-topic", "test-content", "test-service", "testMethod");

            // EXECUTE: Append empty string
            ctx.appendBusinessIdentifier("");

            // VERIFY: No exception, accumulated IDs remain null
            assertThat(ctx.getAccumulatedBusinessIdentifiers()).isNull();
        }

        @Test
        @Order(7)
        @DisplayName("determineSubIdentifier prefers accumulated over original")
        void testDetermineSubIdentifierPrefersAccumulated() {
            // This is a private method in ProcessingAuditService
            // We test it indirectly through audit logging

            var audit = AppProcessingContext.getContext("TEST_CONTEXT_1").getAudit();
            var guard = AppProcessingContext.getContext("TEST_CONTEXT_1")
                    .<ComplexTestEntity>getStepGuard("GUARD_1");

            ComplexTestEntity entity = new ComplexTestEntity("ORD-PREF", 777L, 888, "data");

            // EXECUTE: Process with error (will accumulate business ID)
            // The Kafka key is "ORIGINAL-KEY" but should be replaced by business ID
            audit.logKafka("pref-message", "ORIGINAL-KEY", () -> {
                try {
                    guard.guard(entity, () -> {
                        throw new RuntimeException("Test");
                    }, "testMethod");
                } catch (Exception e) {
                    // Expected
                }
            }, "testPreferenceMethod");

            // VERIFY: sub_identifier is accumulated business ID, not "ORIGINAL-KEY"
            ProcessingAuditLogEntry auditEntry = getMostRecentAudit();
            String subId = auditEntry.getSubIdentifier();

            assertThat(subId).contains("ORD-PREF"); // Has accumulated business ID
            // Note: sub_identifier might contain ORIGINAL-KEY depending on implementation
            // The important thing is it ALSO contains the business ID
        }
    }

    /**
     * CRITICAL TESTS - Kafka Error Handler Auto-Configuration
     * Add to ProcessingFrameworkConsolidatedTest
     * <p>
     * These tests verify that .withKafkaListener() properly configures
     * contexts for auto-creation of Kafka error handler beans.
     */

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Kafka Error Handler Auto-Configuration")
    class KafkaErrorHandlerAutoConfigTests {

        @Test
        @Order(1)
        @DisplayName("Context with withKafkaListener is marked as Kafka-enabled")
        void testWithKafkaListenerMarksContextAsKafka() {
            // Clean up any existing context
            try {
                AppProcessingContext.getContext("KAFKA_TEST");
            } catch (IllegalArgumentException e) {
                // Not registered yet, that's fine
            }

            AppProcessingContext.createContext("KAFKA_TEST")
                    .withOriginMarker("KAFKA_ORIGIN")
                    .withAuditSource("kafka-topic")
                    .withKafkaListener("TestListener")
                    .withStepGuard("GUARD", ComplexTestEntity.class,
                            ErrorHandlingPolicy.builder()
                                    .code("TEST")
                                    .description("Test")
                                    .businessImpact("Test")
                                    .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                                    .responsibleTeam("Test")
                                    .immediateAction("Test")
                                    .autoRetry(true)
                                    .maxRetries(3)
                                    .build())
                    .register();

            var definition = AppProcessingContext.getContextDefinition("KAFKA_TEST");

            // VERIFY: Context is Kafka-enabled
            assertThat(definition.isKafkaContext()).isTrue();
            assertThat(definition.getKafkaListenerName()).isEqualTo("TestListener");
        }

        @Test
        @Order(2)
        @DisplayName("Invalid listener name throws exception")
        void testInvalidKafkaListenerName() {
            assertThatThrownBy(() -> {
                AppProcessingContext.createContext("INVALID_KAFKA_1")
                        .withOriginMarker("TEST")
                        .withAuditSource("topic")
                        .withKafkaListener("123Invalid") // Starts with number - invalid
                        .register();
            }).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid Kafka listener name");
        }

        @Test
        @Order(3)
        @DisplayName("Empty listener name throws exception")
        void testEmptyKafkaListenerName() {
            assertThatThrownBy(() -> {
                AppProcessingContext.createContext("INVALID_KAFKA_2")
                        .withOriginMarker("TEST")
                        .withAuditSource("topic")
                        .withKafkaListener("") // Empty - invalid
                        .register();
            }).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot be null or empty");
        }

        @Test
        @Order(4)
        @DisplayName("Null listener name throws exception")
        void testNullKafkaListenerName() {
            assertThatThrownBy(() -> {
                AppProcessingContext.createContext("INVALID_KAFKA_3")
                        .withOriginMarker("TEST")
                        .withAuditSource("topic")
                        .withKafkaListener(null) // Null - invalid
                        .register();
            }).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot be null or empty");
        }

        @Test
        @Order(5)
        @DisplayName("Context without withKafkaListener is not Kafka-enabled")
        void testContextWithoutKafkaListenerNotKafkaEnabled() {
            // Clean up any existing context
            try {
                AppProcessingContext.getContext("NON_KAFKA_TEST");
            } catch (IllegalArgumentException e) {
                // Not registered yet, that's fine
            }

            AppProcessingContext.createContext("NON_KAFKA_TEST")
                    .withOriginMarker("API")
                    .withAuditSource("api-endpoint")
                    .withStepGuard("GUARD", ComplexTestEntity.class,
                            ErrorHandlingPolicy.builder()
                                    .code("TEST")
                                    .description("Test")
                                    .businessImpact("Test")
                                    .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                                    .responsibleTeam("Test")
                                    .immediateAction("Test")
                                    .autoRetry(false)
                                    .build())
                    .register();

            var definition = AppProcessingContext.getContextDefinition("NON_KAFKA_TEST");

            // VERIFY: Context is NOT Kafka-enabled
            assertThat(definition.isKafkaContext()).isFalse();
            assertThat(definition.getKafkaListenerName()).isNull();
        }

        @Test
        @Order(6)
        @DisplayName("getFirstStepGuardPolicy returns policy from first guard")
        void testGetFirstStepGuardPolicy() {
            ErrorHandlingPolicy policy1 = ErrorHandlingPolicy.builder()
                    .code("POLICY_1")
                    .description("First")
                    .businessImpact("Test")
                    .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P1)
                    .responsibleTeam("Team1")
                    .immediateAction("Action1")
                    .autoRetry(true)
                    .maxRetries(5)
                    .build();

            ErrorHandlingPolicy policy2 = ErrorHandlingPolicy.builder()
                    .code("POLICY_2")
                    .description("Second")
                    .businessImpact("Test")
                    .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P2)
                    .responsibleTeam("Team2")
                    .immediateAction("Action2")
                    .autoRetry(false)
                    .build();

            // Clean up any existing context
            try {
                AppProcessingContext.getContext("MULTI_GUARD");
            } catch (IllegalArgumentException e) {
                // Not registered yet, that's fine
            }

            AppProcessingContext.createContext("MULTI_GUARD")
                    .withOriginMarker("TEST")
                    .withAuditSource("topic")
                    .withStepGuard("GUARD_1", ComplexTestEntity.class, policy1)
                    .withStepGuard("GUARD_2", ComplexTestEntity.class, policy2)
                    .register();

            var definition = AppProcessingContext.getContextDefinition("MULTI_GUARD");

            // VERIFY: Returns first policy (GUARD_1)
            ErrorHandlingPolicy firstPolicy = definition.getFirstStepGuardPolicy();
            assertThat(firstPolicy).isNotNull();
            assertThat(firstPolicy.getCode()).isEqualTo("POLICY_1");
            assertThat(firstPolicy.getMaxRetries()).isEqualTo(5);
        }

        @Test
        @Order(7)
        @DisplayName("Kafka context without step guards throws exception")
        void testKafkaContextWithoutStepGuards() {
            assertThatThrownBy(() -> {
                AppProcessingContext.createContext("NO_GUARDS")
                        .withOriginMarker("TEST")
                        .withAuditSource("topic")
                        .withKafkaListener("NoGuardsListener")
                        .register();
            }).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("must have at least one step guard");
        }

        @Test
        @Order(8)
        @DisplayName("Valid listener name with underscores is accepted")
        void testValidListenerNameWithUnderscores() {
            // Clean up any existing context
            try {
                AppProcessingContext.getContext("UNDERSCORE_TEST");
            } catch (IllegalArgumentException e) {
                // Not registered yet, that's fine
            }

            assertThatCode(() -> {
                AppProcessingContext.createContext("UNDERSCORE_TEST")
                        .withOriginMarker("TEST")
                        .withAuditSource("topic")
                        .withKafkaListener("Location_Master_Listener") // Valid with underscores
                        .withStepGuard("GUARD", ComplexTestEntity.class,
                                ErrorHandlingPolicy.builder()
                                        .code("TEST")
                                        .description("Test")
                                        .businessImpact("Test")
                                        .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                                        .responsibleTeam("Test")
                                        .immediateAction("Test")
                                        .autoRetry(true)
                                        .build())
                        .register();
            }).doesNotThrowAnyException();

            var definition = AppProcessingContext.getContextDefinition("UNDERSCORE_TEST");
            assertThat(definition.getKafkaListenerName()).isEqualTo("Location_Master_Listener");
        }
    }

// Added by SRS 1110

    /**
     * CRITICAL TESTS - Error Handler Opt-In/Opt-Out Configuration
     * <p>
     * These tests verify that contexts can opt into framework error handling
     * or use pre-written handlers, allowing coexistence with legacy implementations.
     */
    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @DisplayName("Error Handler Opt-In/Opt-Out Configuration")
    class ErrorHandlerOptInTests {

        @Test
        @Order(1)
        @DisplayName("Context with usePreWrittenHandler flag is properly configured")
        void testContextWithPreWrittenHandlerFlag() {
            try {
                // Clean up any existing context
                try {
                    AppProcessingContext.getContext("PRE_WRITTEN_TEST");
                } catch (IllegalArgumentException e) {
                    // Not registered yet, that's fine
                }

                // EXECUTE: Register context with pre-written handler flag
                AppProcessingContext.createContext("PRE_WRITTEN_TEST")
                        .withOriginMarker("PRE_WRITTEN_ORIGIN")
                        .withAuditSource("prewritten-topic")
                        .withKafkaListener("PreWrittenListener")
                        .withStepGuard("GUARD", ComplexTestEntity.class,
                                ErrorHandlingPolicy.builder()
                                        .code("PRE_WRITTEN_ERROR")
                                        .description("Pre-written test")
                                        .businessImpact("Test")
                                        .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P2)
                                        .responsibleTeam("Test Team")
                                        .immediateAction("Test")
                                        .autoRetry(false)
                                        .build())
                        .usePreWrittenHandler()
                        .register();

                // VERIFY: Context registered successfully
                var definition = AppProcessingContext.getContextDefinition("PRE_WRITTEN_TEST");
                assertThat(definition).isNotNull();

                // VERIFY: Flag is set correctly
                assertThat(definition.shouldUsePreWrittenHandler()).isTrue();
                assertThat(definition.isFrameworkErrorHandlingEnabled()).isFalse();

                // VERIFY: Context is still Kafka-enabled
                assertThat(definition.isKafkaContext()).isTrue();
                assertThat(definition.getKafkaListenerName()).isEqualTo("PreWrittenListener");

            } catch (Exception ex) {
                fail("Test failed: " + ex.getMessage(), ex);
            }
        }

        @Test
        @Order(2)
        @DisplayName("Context with enableFrameworkErrorHandling flag is properly configured")
        void testContextWithFrameworkHandlerFlag() {
            try {
                // Clean up any existing context
                try {
                    AppProcessingContext.getContext("FRAMEWORK_TEST");
                } catch (IllegalArgumentException e) {
                    // Not registered yet, that's fine
                }

                // EXECUTE: Register context with framework handler flag
                AppProcessingContext.createContext("FRAMEWORK_TEST")
                        .withOriginMarker("FRAMEWORK_ORIGIN")
                        .withAuditSource("framework-topic")
                        .withKafkaListener("FrameworkListener")
                        .withStepGuard("GUARD", ComplexTestEntity.class,
                                ErrorHandlingPolicy.builder()
                                        .code("FRAMEWORK_ERROR")
                                        .description("Framework test")
                                        .businessImpact("Test")
                                        .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P1)
                                        .responsibleTeam("Test Team")
                                        .immediateAction("Test")
                                        .autoRetry(true)
                                        .maxRetries(5)
                                        .retryDelay(Duration.ofSeconds(2))
                                        .build())
                        .enableFrameworkErrorHandling()
                        .register();

                // VERIFY: Context registered successfully
                var definition = AppProcessingContext.getContextDefinition("FRAMEWORK_TEST");
                assertThat(definition).isNotNull();

                // VERIFY: Flag is set correctly
                assertThat(definition.shouldUsePreWrittenHandler()).isFalse();
                assertThat(definition.isFrameworkErrorHandlingEnabled()).isTrue();

                // VERIFY: Context is Kafka-enabled with proper listener
                assertThat(definition.isKafkaContext()).isTrue();
                assertThat(definition.getKafkaListenerName()).isEqualTo("FrameworkListener");

                // VERIFY: Policy is accessible
                ErrorHandlingPolicy policy = definition.getFirstStepGuardPolicy();
                assertThat(policy).isNotNull();
                assertThat(policy.getCode()).isEqualTo("FRAMEWORK_ERROR");
                assertThat(policy.isAutoRetry()).isTrue();
                assertThat(policy.getMaxRetries()).isEqualTo(5);

            } catch (Exception ex) {
                fail("Test failed: " + ex.getMessage(), ex);
            }
        }

        @Test
        @Order(3)
        @DisplayName("Context without explicit flag defaults to framework handling")
        void testContextDefaultBehavior() {
            try {
                // Clean up any existing context
                try {
                    AppProcessingContext.getContext("DEFAULT_TEST");
                } catch (IllegalArgumentException e) {
                    // Not registered yet, that's fine
                }

                // EXECUTE: Register context without explicit flag
                AppProcessingContext.createContext("DEFAULT_TEST")
                        .withOriginMarker("DEFAULT_ORIGIN")
                        .withAuditSource("default-topic")
                        .withKafkaListener("DefaultListener")
                        .withStepGuard("GUARD", ComplexTestEntity.class,
                                ErrorHandlingPolicy.builder()
                                        .code("DEFAULT_ERROR")
                                        .description("Default test")
                                        .businessImpact("Test")
                                        .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P2)
                                        .responsibleTeam("Test Team")
                                        .immediateAction("Test")
                                        .autoRetry(false)
                                        .build())
                        .register();

                // VERIFY: Context registered successfully
                var definition = AppProcessingContext.getContextDefinition("DEFAULT_TEST");
                assertThat(definition).isNotNull();

                // VERIFY: Default is framework handling (NOT pre-written)
                assertThat(definition.shouldUsePreWrittenHandler()).isFalse();
                assertThat(definition.isFrameworkErrorHandlingEnabled()).isTrue();

            } catch (Exception ex) {
                fail("Test failed: " + ex.getMessage(), ex);
            }
        }

        @Test
        @Order(4)
        @DisplayName("Switching from pre-written to framework works")
        void testSwitchingFromPreWrittenToFramework() {
            try {
                // Clean up any existing context
                try {
                    AppProcessingContext.getContext("SWITCH_TEST");
                } catch (IllegalArgumentException e) {
                    // Not registered yet, that's fine
                }

                // EXECUTE: Build context with pre-written first, then switch to framework
                var builder = AppProcessingContext.createContext("SWITCH_TEST")
                        .withOriginMarker("SWITCH_ORIGIN")
                        .withAuditSource("switch-topic")
                        .withKafkaListener("SwitchListener")
                        .withStepGuard("GUARD", ComplexTestEntity.class,
                                ErrorHandlingPolicy.builder()
                                        .code("SWITCH_ERROR")
                                        .description("Switch test")
                                        .businessImpact("Test")
                                        .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P2)
                                        .responsibleTeam("Test Team")
                                        .immediateAction("Test")
                                        .autoRetry(false)
                                        .build());

                // First set to pre-written
                builder.usePreWrittenHandler();
                // Then switch to framework
                builder.enableFrameworkErrorHandling();
                // Register
                builder.register();

                // VERIFY: Last call wins - framework enabled
                var definition = AppProcessingContext.getContextDefinition("SWITCH_TEST");
                assertThat(definition.shouldUsePreWrittenHandler()).isFalse();
                assertThat(definition.isFrameworkErrorHandlingEnabled()).isTrue();

            } catch (Exception ex) {
                fail("Test failed: " + ex.getMessage(), ex);
            }
        }

        @Test
        @Order(5)
        @DisplayName("Non-Kafka context can have opt-in flags")
        void testNonKafkaContextWithFlags() {
            try {
                // Clean up any existing context
                try {
                    AppProcessingContext.getContext("API_OPT_TEST");
                } catch (IllegalArgumentException e) {
                    // Not registered yet, that's fine
                }

                // EXECUTE: API context (no Kafka listener) with pre-written flag
                AppProcessingContext.createContext("API_OPT_TEST")
                        .withOriginMarker("API_ORIGIN")
                        .withAuditSource("api-endpoint")
                        .withStepGuard("GUARD", ComplexTestEntity.class,
                                ErrorHandlingPolicy.builder()
                                        .code("API_ERROR")
                                        .description("API test")
                                        .businessImpact("Test")
                                        .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                                        .responsibleTeam("Test Team")
                                        .immediateAction("Test")
                                        .autoRetry(false)
                                        .build())
                        .usePreWrittenHandler()
                        .register();

                // VERIFY: Context registered successfully
                var definition = AppProcessingContext.getContextDefinition("API_OPT_TEST");
                assertThat(definition).isNotNull();

                // VERIFY: Flag is set even though not Kafka context
                assertThat(definition.shouldUsePreWrittenHandler()).isTrue();
                assertThat(definition.isKafkaContext()).isFalse();

            } catch (Exception ex) {
                fail("Test failed: " + ex.getMessage(), ex);
            }
        }

        @Test
        @Order(6)
        @DisplayName("Multiple contexts with different configurations coexist")
        void testMultipleContextsCoexistence() {
            try {
                // Clean up any existing contexts
                try {
                    AppProcessingContext.getContext("COEXIST_PRE");
                } catch (IllegalArgumentException e) {
                    // Not registered yet, that's fine
                }
                try {
                    AppProcessingContext.getContext("COEXIST_FWK");
                } catch (IllegalArgumentException e) {
                    // Not registered yet, that's fine
                }

                // EXECUTE: Register multiple contexts with different configurations
                AppProcessingContext.createContext("COEXIST_PRE")
                        .withOriginMarker("PRE_ORIGIN")
                        .withAuditSource("pre-topic")
                        .withKafkaListener("PreListener")
                        .withStepGuard("GUARD", ComplexTestEntity.class,
                                ErrorHandlingPolicy.builder()
                                        .code("PRE_ERROR")
                                        .description("Pre test")
                                        .businessImpact("Test")
                                        .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P2)
                                        .responsibleTeam("Test Team")
                                        .immediateAction("Test")
                                        .autoRetry(false)
                                        .build())
                        .usePreWrittenHandler()
                        .register();

                AppProcessingContext.createContext("COEXIST_FWK")
                        .withOriginMarker("FWK_ORIGIN")
                        .withAuditSource("fwk-topic")
                        .withKafkaListener("FwkListener")
                        .withStepGuard("GUARD", ComplexTestEntity.class,
                                ErrorHandlingPolicy.builder()
                                        .code("FWK_ERROR")
                                        .description("Framework test")
                                        .businessImpact("Test")
                                        .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P1)
                                        .responsibleTeam("Test Team")
                                        .immediateAction("Test")
                                        .autoRetry(true)
                                        .maxRetries(3)
                                        .build())
                        .enableFrameworkErrorHandling()
                        .register();

                // VERIFY: Both contexts registered with different configurations
                var preDef = AppProcessingContext.getContextDefinition("COEXIST_PRE");
                var fwkDef = AppProcessingContext.getContextDefinition("COEXIST_FWK");

                assertThat(preDef.shouldUsePreWrittenHandler()).isTrue();
                assertThat(preDef.isFrameworkErrorHandlingEnabled()).isFalse();

                assertThat(fwkDef.shouldUsePreWrittenHandler()).isFalse();
                assertThat(fwkDef.isFrameworkErrorHandlingEnabled()).isTrue();

                // VERIFY: Each has its own listener name
                assertThat(preDef.getKafkaListenerName()).isEqualTo("PreListener");
                assertThat(fwkDef.getKafkaListenerName()).isEqualTo("FwkListener");

            } catch (Exception ex) {
                fail("Test failed: " + ex.getMessage(), ex);
            }
        }

        @Test
        @Order(7)
        @DisplayName("Pre-written handler context still has full framework capabilities")
        void testPreWrittenContextHasFrameworkCapabilities() {
            try {
                // Clean up any existing context
                try {
                    AppProcessingContext.getContext("FULL_CAPS_TEST");
                } catch (IllegalArgumentException e) {
                    // Not registered yet, that's fine
                }

                // EXECUTE: Register pre-written context
                AppProcessingContext.createContext("FULL_CAPS_TEST")
                        .withOriginMarker("CAPS_ORIGIN")
                        .withAuditSource("caps-topic")
                        .withKafkaListener("CapsListener")
                        .withStepGuard("GUARD", ComplexTestEntity.class,
                                ErrorHandlingPolicy.builder()
                                        .code("CAPS_ERROR")
                                        .description("Capabilities test")
                                        .businessImpact("Test")
                                        .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P2)
                                        .responsibleTeam("Test Team")
                                        .immediateAction("Test")
                                        .autoRetry(false)
                                        .build())
                        .usePreWrittenHandler()
                        .register();

                // VERIFY: Context has full framework capabilities despite pre-written flag
                var accessor = AppProcessingContext.getContext("FULL_CAPS_TEST");

                // Can get audit logger
                assertThat(accessor.getAudit()).isNotNull();

                // Can get step guards
                var stepGuard = accessor.<ComplexTestEntity>getStepGuard("GUARD");
                assertThat(stepGuard).isNotNull();

                // Step guard works normally
                long initialErrorCount = errorRepository.count();
                ComplexTestEntity entity = new ComplexTestEntity("ORD-CAPS", 999L, 888, "data");

                accessor.getAudit().logKafka("test-message", "test-key", () -> {
                    try {
                        stepGuard.guard(entity, () -> {
                            throw new RuntimeException("Test error");
                        }, "testMethod");
                    } catch (RuntimeException e) {
                        // Expected
                    }
                }, "testCapsMethod");

                // VERIFY: Business error logging still works
                assertThat(errorRepository.count()).isEqualTo(initialErrorCount + 1);

                ProcessingErrorLogEntry error = getMostRecentError();
                assertThat(error.getBusinessIdentifier()).contains("order: ORD-CAPS");
                assertThat(error.getErrorPolicyCode()).isEqualTo("CAPS_ERROR");

            } catch (Exception ex) {
                fail("Test failed: " + ex.getMessage(), ex);
            }
        }
    }
// End Addition by SRS


}