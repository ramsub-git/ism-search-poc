package com.sephora.services.ismsearchpoc.processing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central registry for processing contexts with elegant builder pattern and direct access.
 * <p>
 * This class provides the main API for the Business Error Logging Framework, offering:
 * - Fluent builder pattern for context registration during application startup
 * - Direct access pattern for runtime usage without setup overhead
 * - Pre-initialization of all reflection-based components for zero-runtime overhead
 * - Thread-safe context registry supporting concurrent access during processing
 * <p>
 * The design follows a two-phase approach:
 * 1. **Configuration Phase** (startup): Rich builder pattern for elegant context setup
 * 2. **Usage Phase** (runtime): Direct access with automatic context inheritance
 * <p>
 * Example lifecycle:
 * <pre>
 * // 1. Configuration Phase (application startup)
 * AppProcessingContext.createContext("INVENTORY_UPDATE")
 *     .withOriginMarker("INVENTORY_UPDATE_FROM_SAP")
 *     .withAuditSource("inventory-update-topic")
 *     .withStepGuard("SKU_UPDATE", SkuMasterRecord.class, skuUpdatePolicy)
 *     .withStepGuard("RESERVE_CALC", SkulocReserveRecord.class, reserveCalcPolicy)
 *     .register();  // Triggers pre-initialization of all field extractors
 *
 * // 2. Usage Phase (runtime processing)
 * var context = AppProcessingContext.getContext("INVENTORY_UPDATE");
 * var audit = context.getAudit();           // Self-contained audit logger
 * var skuGuard = context.getStepGuard("SKU_UPDATE");  // Pre-initialized step guard
 *
 * // Pure usage - one line with automatic context management
 * audit.logKafka(message, key, businessLogic, "processInventoryUpdate");
 * </pre>
 * <p>
 * Thread safety: Uses ConcurrentHashMap for the context registry, supporting safe
 * concurrent access during processing while allowing updates during startup.
 *
 * @author ISM Processing Framework
 * @see ProcessingContextBuilder
 * @see ProcessingContextAccessor
 * @see ContextualAuditLogger
 * @see ContextualStepGuard
 * @since 1.0
 */
@Component
@Slf4j
public class AppProcessingContext {

    private static final ConcurrentHashMap<String, ProcessingContextDefinition> contexts = new ConcurrentHashMap<>();
    private static ProcessingAuditHelper auditHelper;
    private static ProcessingErrorService errorService;  // FIX: Use ProcessingErrorService

    @Autowired
    public void setFrameworkHelpers(ProcessingAuditHelper audit, ProcessingErrorService errorSvc) {
        auditHelper = audit;
        errorService = errorSvc;  // FIX: Correct field name
        log.info("AppProcessingContext initialized with existing framework helpers");
    }

    public static ProcessingContextBuilder createContext(String contextName) {
        if (contextName == null || contextName.trim().isEmpty()) {
            throw new IllegalArgumentException("Context name cannot be null or empty");
        }
        return new ProcessingContextBuilder(contextName.trim());
    }

    public static ProcessingContextAccessor getContext(String contextName) {
        ProcessingContextDefinition definition = contexts.get(contextName);
        if (definition == null) {
            throw new IllegalArgumentException(
                    String.format("Unknown processing context: '%s'. Available contexts: %s",
                            contextName, contexts.keySet()));
        }
        return new ProcessingContextAccessor(definition, auditHelper, errorService);
    }

    public static Collection<ProcessingContextDefinition> getAllContexts() {
        return contexts.values();
    }

    static void registerContext(String name, ProcessingContextDefinition definition) {
        // Check if context already exists
        ProcessingContextDefinition existing = contexts.get(name);

        if (existing != null) {
            // Warn the developer - this is likely a mistake
            log.warn("Processing context '{}' is already registered. " +
                            "Merging {} new step guards into existing context which has {} step guards. " +
                            "Existing guards: {}, New guards: {}",
                    name,
                    definition.getStepGuards().size(),
                    existing.getStepGuards().size(),
                    existing.getStepGuards().keySet(),
                    definition.getStepGuards().keySet());

            // Merge new step guards into existing context
            definition.getStepGuards().forEach((guardName, guardDef) -> {
                if (existing.getStepGuards().containsKey(guardName)) {
                    log.warn("Step guard '{}' already exists in context '{}', replacing with new definition",
                            guardName, name);
                }
                existing.addStepGuard(guardName, guardDef);
            });

            // Use the existing context (now with merged guards)
            definition = existing;
        }


        // Kafka contexts must have at least one step guard
        if (definition.isKafkaContext() && definition.getStepGuards().isEmpty()) {
            log.error("EXIT: registerContext - FAILED: Kafka context '{}' must have at least one step guard", name);
            throw new IllegalStateException(
                    String.format("Kafka processing context '%s' must have at least one step guard. " +
                                    "Kafka contexts require step guards to properly handle business errors and maintain operational intelligence.",
                            name));
        }


        // Register (first time) or just update reference (duplicate with merge)
        contexts.put(name, definition);

        // Pre-initialize all step guards for zero-reflection runtime performance
        definition.getStepGuards().forEach((guardName, guardDef) -> {
            try {
                guardDef.preInitialize();
            } catch (Exception ex) {
                log.error("Failed to pre-initialize step guard '{}' in context '{}': {}",
                        guardName, name, ex.getMessage(), ex);
                throw new IllegalStateException("Step guard pre-initialization failed", ex);
            }
        });

        log.info("Registered processing context '{}' with {} step guards: {}",
                name, definition.getStepGuards().size(), definition.getStepGuards().keySet());
    }

    // =====================================================================================
    // BUILDER CLASS
    // =====================================================================================

    public static class ProcessingContextBuilder {
        private final ProcessingContextDefinition definition;

        public ProcessingContextBuilder(String contextName) {
            this.definition = new ProcessingContextDefinition(contextName);
        }

        public ProcessingContextBuilder withOriginMarker(String marker) {
            definition.setOriginMarker(marker);
            return this;
        }

        public ProcessingContextBuilder withDescription(String description) {
            definition.setDescription(description);
            return this;
        }

        public ProcessingContextBuilder withAuditSource(String source) {
            definition.setAuditSource(source);
            return this;
        }

        public ProcessingContextBuilder withInitiator(String className, String methodName) {
            definition.setInitiatingClass(className);
            definition.setInitiatingMethod(methodName);
            return this;
        }

        public ProcessingContextBuilder withServiceName(String serviceName) {
            definition.setServiceName(serviceName);
            return this;
        }

        public <T> ProcessingContextBuilder withStepGuard(String guardName, Class<T> entityType,
                                                          ErrorHandlingPolicy policy) {
            StepGuardDefinition<T> guardDefinition = new StepGuardDefinition<>(guardName, entityType, policy);
            definition.addStepGuard(guardName, guardDefinition);
            return this;
        }

        public <T> ProcessingContextBuilder withStepGuard(String guardName, Class<T> entityType,
                                                          ErrorHandlingPolicy policy,
                                                          boolean rethrow, boolean relog) {
            StepGuardDefinition<T> guardDefinition = new StepGuardDefinition<>(guardName, entityType, policy)
                    .withRethrow(rethrow)
                    .withRelog(relog);
            definition.addStepGuard(guardName, guardDefinition);
            return this;
        }

        public ProcessingContextBuilder withKafkaListener(String listenerName) {
            log.trace("ENTRY: withKafkaListener - contextName={}, listenerName={}",
                    definition.getContextName(), listenerName);

            try {
                if (listenerName == null || listenerName.trim().isEmpty()) {
                    String errorMsg = String.format(
                            "Kafka listener name cannot be null or empty for context '%s'",
                            definition.getContextName());
                    log.error(errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }

                String trimmedName = listenerName.trim();

                if (!trimmedName.matches("[A-Za-z][A-Za-z0-9_]*")) {
                    String errorMsg = String.format(
                            "Invalid Kafka listener name '%s' for context '%s'",
                            trimmedName, definition.getContextName());
                    log.error(errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }

                definition.setKafkaListenerName(trimmedName);

                log.info("Configured Kafka listener '{}' for context '{}'",
                        trimmedName, definition.getContextName());
                log.debug("Framework will auto-create bean: errorHandler.{}", trimmedName);
                log.trace("EXIT: withKafkaListener - SUCCESS");

                return this;

            } catch (Exception ex) {
                log.error("EXIT: withKafkaListener - FAILED: {}", ex.getMessage(), ex);
                throw ex;
            } finally {
                // Nothing to clean up
            }
        }

        // Added by SRS 1110
        public ProcessingContextBuilder usePreWrittenHandler() {
            log.trace("ENTRY: usePreWrittenHandler - contextName={}", definition.getContextName());

            try {
                definition.setUsePreWrittenHandler(true);
                log.debug("Context '{}' configured to use pre-written error handler",
                        definition.getContextName());
                log.trace("EXIT: usePreWrittenHandler - SUCCESS");
                return this;

            } catch (Exception ex) {
                log.error("EXIT: usePreWrittenHandler - FAILED: {}", ex.getMessage(), ex);
                throw ex;
            }
        }

        public ProcessingContextBuilder enableFrameworkErrorHandling() {
            log.trace("ENTRY: enableFrameworkErrorHandling - contextName={}", definition.getContextName());

            try {
                definition.setUsePreWrittenHandler(false);
                log.debug("Context '{}' configured to use framework error handling",
                        definition.getContextName());
                log.trace("EXIT: enableFrameworkErrorHandling - SUCCESS");
                return this;

            } catch (Exception ex) {
                log.error("EXIT: enableFrameworkErrorHandling - FAILED: {}", ex.getMessage(), ex);
                throw ex;
            }
        }
        // End Addition

        public void register() {
            AppProcessingContext.registerContext(definition.getContextName(), definition);
        }
    }

    // =====================================================================================
    // ACCESSOR CLASS - FIX: Use ProcessingErrorService
    // =====================================================================================

    public static class ProcessingContextAccessor {
        private final ProcessingContextDefinition definition;
        private final ProcessingAuditHelper auditHelper;
        private final ProcessingErrorService errorService;  // FIX: Correct type

        public ProcessingContextAccessor(ProcessingContextDefinition definition,
                                         ProcessingAuditHelper auditHelper,
                                         ProcessingErrorService errorService) {  // FIX: Correct parameter
            this.definition = definition;
            this.auditHelper = auditHelper;
            this.errorService = errorService;
        }

        public ContextualAuditLogger getAudit() {
            String serviceName = getServiceName();
            return new ContextualAuditLogger(definition, auditHelper, serviceName);
        }

        public <T> ContextualStepGuard<T> getStepGuard(String guardName) {
            StepGuardDefinition<T> guardDefinition = definition.getStepGuard(guardName);
            if (guardDefinition == null) {
                throw new IllegalArgumentException(
                        String.format("Unknown step guard '%s' in context '%s'. Available guards: %s",
                                guardName, definition.getContextName(), definition.getStepGuards().keySet()));
            }

            return new ContextualStepGuard<>(definition, guardDefinition, errorService);  // FIX: Pass errorService
        }

        private String getServiceName() {
            if (definition.getServiceName() != null) {
                return definition.getServiceName();
            }
            return System.getProperty("spring.application.name", "ism-processing-service");
        }


        /**
         * Add a business entity to current thread's context.
         * Returns this accessor for fluent chaining.
         */
        public ProcessingContextAccessor addBusinessEntity(Object entity) {
            ProcessingContextManager.addBusinessEntity(entity);
            return this;
        }

        /**
         * Add multiple business entities at once.
         * Returns this accessor for fluent chaining.
         */
        public ProcessingContextAccessor addBusinessEntities(Object... entities) {
            if (entities != null) {
                for (Object entity : entities) {
                    ProcessingContextManager.addBusinessEntity(entity);
                }
            }
            return this;
        }
    }

    public static ProcessingContextDefinition getContextDefinition(String name) {
        ProcessingContextDefinition def = contexts.get(name);
        if (def == null) {
            throw new IllegalArgumentException("Context not found: " + name);
        }
        return def;
    }
}