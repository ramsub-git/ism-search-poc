package com.sephora.services.ismsearchpoc.processing;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;


/**
 * Base class for repo-specific processing context configuration.
 *
 * <h3>Purpose:</h3>
 * Separates framework code (portable) from configuration code (repo-specific).
 * Each repo extends this class to register their processing contexts.
 *
 * <h3>Framework Structure:</h3>
 * <pre>
 * ism-supply/
 *   ├─ processing/                    ← FRAMEWORK (copy to other repos)
 *   │    ├─ AppProcessingContext.java
 *   │    ├─ ProcessingAuditHelper.java
 *   │    ├─ ProcessingContextConfigBase.java  ← THIS FILE
 *   │    └─ ...
 *   │
 *   └─ config/                         ← CONFIGURATION (repo-specific)
 *        └─ SupplyProcessingConfig.java        ← Extends this class
 *
 * ism-foundation/
 *   ├─ processing/                    ← SAME FRAMEWORK (copied)
 *   └─ config/
 *        └─ FoundationProcessingConfig.java    ← Different config
 * </pre>
 *
 * <h3>Usage Example:</h3>
 * <pre>
 * @Configuration
 * public class SupplyProcessingConfig extends ProcessingContextConfigBase {
 *
 *     @Override
 *     protected void configureContexts() {
 *         // Register POSLOG processing context
 *         context("POSLOG")
 *             .withOriginMarker("POS_TRANSACTION_PROCESSING")
 *             .withAuditSource("pos-log-topic")
 *             .withDescription("Point of Sale transaction processing")
 *             .withStepGuard("POS_LINE", PosLine.class, posLinePolicy())
 *             .withStepGuard("POS_RETURN", PosReturn.class, posReturnPolicy())
 *             .register();
 *
 *         // Register INVENTORY processing context
 *         context("INVENTORY")
 *             .withOriginMarker("INVENTORY_UPDATE_FROM_SAP")
 *             .withAuditSource("inventory-update-topic")
 *             .withDescription("SAP inventory feed processing")
 *             .withStepGuard("SKU_UPDATE", SkuMaster.class, skuUpdatePolicy())
 *             .withStepGuard("RESERVE_CALC", SkulocReserve.class, reservePolicy())
 *             .register();
 *     }
 *
 *     private ErrorHandlingPolicy posLinePolicy() {
 *         return ErrorHandlingPolicy.builder()
 *                 .code("POS_LINE_ERROR")
 *                 .description("Error processing POS transaction line")
 *                 .businessImpact("HIGH - Revenue reporting impact")
 *                 .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P2)
 *                 .responsibleTeam("Retail Systems")
 *                 .immediateAction("Check POS connectivity")
 *                 .autoRetry(false)
 *                 .build();
 *     }
 * }
 * </pre>
 *
 * <h3>Benefits:</h3>
 * <ul>
 *   <li>Framework code is 100% portable (just copy `processing/` folder)</li>
 *   <li>Each repo has its own configuration class</li>
 *   <li>Clear separation: framework vs configuration</li>
 *   <li>Easy to replicate to other repos</li>
 * </ul>
 *
 * @see AppProcessingContext Registry where contexts are stored
 * @see ProcessingContextDefinition Metadata for each processing domain
 */
@Slf4j
public abstract class ProcessingContextConfigBase implements BeanDefinitionRegistryPostProcessor {

    /**
     * Configure processing contexts for this specific repo.
     *
     * <p>Subclasses MUST implement this method to register their contexts.
     *
     * <p>Example implementation:
     * <pre>
     * @Override
     * protected void configureContexts() {
     *     context("POSLOG")
     *         .withOriginMarker("POS_TRANSACTION")
     *         .withStepGuard("POS_LINE", PosLine.class, policy())
     *         .register();
     * }
     * </pre>
     */
    protected abstract void configureContexts();

    /**
     * Helper method to start building a processing context.
     *
     * <p>This provides a clean, fluent API for subclasses:
     * <pre>
     * context("POSLOG")
     *     .withOriginMarker("POS_TRANSACTION")
     *     .withStepGuard(...)
     *     .register();
     * </pre>
     *
     * @param contextName unique name for this processing context
     * @return builder for configuring the context
     */
    protected AppProcessingContext.ProcessingContextBuilder context(String contextName) {
        log.debug("Creating processing context: {}", contextName);
        return AppProcessingContext.createContext(contextName);
    }

//    @Override
//    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
//        log.info("========================================");
//        log.info("Kafka Error Handler Auto-Configuration");
//        log.info("========================================");
//        log.trace("ENTRY: postProcessBeanDefinitionRegistry");
//
//        int beansCreated = 0;
//
//        try {
//            // Let subclass register contexts first
//            log.debug("Invoking configureContexts() to register processing contexts");
//            configureContexts();
//
//            // Scan all contexts for Kafka listeners
//            Collection<ProcessingContextDefinition> allContexts = AppProcessingContext.getAllContexts();
//            log.info("Scanned {} context(s), looking for Kafka configurations", allContexts.size());
//
//            for (ProcessingContextDefinition contextDef : allContexts) {
//                try {
//                    if (!contextDef.isKafkaContext()) {
//                        log.trace("Context '{}' is not Kafka-enabled, skipping", contextDef.getContextName());
//                        continue;
//                    }
//
//                    String listenerName = contextDef.getKafkaListenerName();
//                    log.info("Processing Kafka context: '{}' with listener '{}'",
//                            contextDef.getContextName(), listenerName);
//
//                    registerKafkaErrorHandlerBean(registry, listenerName, contextDef);
//                    beansCreated++;
//
//                } catch (Exception ex) {
//                    String errorMsg = String.format(
//                            "Failed to create Kafka error handler for context '%s': %s",
//                            contextDef.getContextName(), ex.getMessage());
//                    log.error(errorMsg, ex);
//                    throw new IllegalStateException(errorMsg, ex);
//                }
//            }
//
//            log.info("========================================");
//            log.info("Scanned {} context(s), created {} Kafka error handler bean(s)",
//                    allContexts.size(), beansCreated);
//            log.info("Kafka Error Handler Auto-Configuration Complete");
//            log.info("========================================");
//            log.trace("EXIT: postProcessBeanDefinitionRegistry - SUCCESS - beansCreated={}", beansCreated);
//
//        } catch (Exception ex) {
//            log.error("EXIT: postProcessBeanDefinitionRegistry - FAILED: {}", ex.getMessage(), ex);
//            log.error("CRITICAL ERROR: Kafka error handler auto-configuration failed", ex);
//            throw new IllegalStateException("Kafka error handler initialization failed", ex);
//        } finally {
//            // Nothing to clean up
//        }
//    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        log.info("========================================");
        log.info("Kafka Error Handler Auto-Configuration");
        log.info("========================================");
        log.trace("ENTRY: postProcessBeanDefinitionRegistry");

        int beansCreated = 0;

        try {
            // CRITICAL FIX: Call configureContexts() FIRST to register all contexts
            log.info("========================================");
            log.info("Initializing Processing Contexts...");
            log.info("========================================");
            log.debug("Invoking configureContexts() to register processing contexts");
            configureContexts();

            // Log summary of registered contexts
            int contextCount = AppProcessingContext.getAllContexts().size();
            log.info("Successfully registered {} processing context(s):", contextCount);

            AppProcessingContext.getAllContexts().forEach(def -> {
                log.info("  • {} - {} step guard(s): {}",
                        def.getContextName(),
                        def.getStepGuards().size(),
                        def.getStepGuards().keySet());

                if (def.getDescription() != null) {
                    log.info("    Description: {}", def.getDescription());
                }
                if (def.getOriginMarker() != null) {
                    log.info("    Origin Marker: {}", def.getOriginMarker());
                }
            });

            log.info("========================================");
            log.info("Processing Contexts Initialization Complete");
            log.info("========================================");

            // NOW scan for Kafka contexts
            log.info("Scanning for Kafka contexts...");
            Collection<ProcessingContextDefinition> allContexts = AppProcessingContext.getAllContexts();

            for (ProcessingContextDefinition contextDef : allContexts) {
                try {
                    if (!contextDef.isKafkaContext()) {
                        log.trace("Context '{}' is not Kafka-enabled, skipping", contextDef.getContextName());
                        continue;
                    }

                    String listenerName = contextDef.getKafkaListenerName();
                    log.info("Processing Kafka context: '{}' with listener '{}'",
                            contextDef.getContextName(), listenerName);

                    registerKafkaErrorHandlerBean(registry, listenerName, contextDef);
                    beansCreated++;

                } catch (Exception ex) {
                    String errorMsg = String.format(
                            "Failed to create Kafka error handler for context '%s': %s",
                            contextDef.getContextName(), ex.getMessage());
                    log.error(errorMsg, ex);
                    throw new IllegalStateException(errorMsg, ex);
                }
            }

            log.info("========================================");
            log.info("Scanned {} context(s), created {} Kafka error handler bean(s)",
                    allContexts.size(), beansCreated);
            log.info("Kafka Error Handler Auto-Configuration Complete");
            log.info("========================================");
            log.trace("EXIT: postProcessBeanDefinitionRegistry - SUCCESS - beansCreated={}", beansCreated);

        } catch (Exception ex) {
            log.error("EXIT: postProcessBeanDefinitionRegistry - FAILED: {}", ex.getMessage(), ex);
            log.error("CRITICAL ERROR: Kafka error handler auto-configuration failed", ex);
            throw new IllegalStateException("Kafka error handler initialization failed", ex);
        } finally {
            // Nothing to clean up
        }
    }

    // Changed by SRS 1110
//    private void registerKafkaErrorHandlerBean(BeanDefinitionRegistry registry,
//                                               String listenerName,
//                                               ProcessingContextDefinition contextDef) {
//        log.trace("ENTRY: registerKafkaErrorHandlerBean - listenerName={}, context={}",
//                listenerName, contextDef.getContextName());
//
//        try {
//            String beanName = "errorHandler." + listenerName;
//
//            if (registry.containsBeanDefinition(beanName)) {
//                String errorMsg = String.format(
//                        "Bean '%s' already exists. Duplicate error handler for listener '%s'",
//                        beanName, listenerName);
//                log.error(errorMsg);
//                throw new IllegalStateException(errorMsg);
//            }
//
//            log.debug("Extracting ErrorHandlingPolicy from context '{}'", contextDef.getContextName());
//            ErrorHandlingPolicy policy = contextDef.getFirstStepGuardPolicy();
//
//            logPolicyConfiguration(listenerName, contextDef.getContextName(), policy);
//
//            log.debug("Building DefaultErrorHandler for listener '{}'", listenerName);
//            DefaultErrorHandler errorHandler = buildDefaultErrorHandler(policy, listenerName);
//
//            log.debug("Registering Spring bean: {}", beanName);
//            BeanDefinitionBuilder beanDefBuilder = BeanDefinitionBuilder
//                    .genericBeanDefinition(DefaultErrorHandler.class, () -> errorHandler);
//
//            registry.registerBeanDefinition(beanName, beanDefBuilder.getBeanDefinition());
//
//            log.info("Successfully registered bean: {}", beanName);
//            log.trace("EXIT: registerKafkaErrorHandlerBean - SUCCESS");
//
//        } catch (Exception ex) {
//            log.error("EXIT: registerKafkaErrorHandlerBean - FAILED: {}", ex.getMessage(), ex);
//            throw ex;
//        } finally {
//            // Nothing to clean up
//        }
//    }

    private void registerKafkaErrorHandlerBean(BeanDefinitionRegistry registry,
                                               String listenerName,
                                               ProcessingContextDefinition contextDef) {
        log.trace("ENTRY: registerKafkaErrorHandlerBean - listenerName={}, context={}",
                listenerName, contextDef.getContextName());

        try {
            // Check if context wants to use pre-written handler
            if (contextDef.shouldUsePreWrittenHandler()) {
                log.info("Context '{}' configured to use pre-written handler, skipping framework bean registration",
                        contextDef.getContextName());
                log.trace("EXIT: registerKafkaErrorHandlerBean - SKIPPED (pre-written handler)");
                return;
            }

            String beanName = "errorHandler." + listenerName;

            // Check for existing bean (conflict detection)
            if (registry.containsBeanDefinition(beanName)) {
                log.warn("Bean '{}' already exists. Context '{}' may have conflicting configuration. " +
                                "To use framework error handling, ensure no other bean creates this handler. Skipping registration.",
                        beanName, contextDef.getContextName());
                log.trace("EXIT: registerKafkaErrorHandlerBean - SKIPPED (bean exists)");
                return;
            }

            log.info("Context '{}' using FRAMEWORK error handling", contextDef.getContextName());
            log.debug("Extracting ErrorHandlingPolicy from context '{}'", contextDef.getContextName());
            ErrorHandlingPolicy policy = contextDef.getFirstStepGuardPolicy();

            logPolicyConfiguration(listenerName, contextDef.getContextName(), policy);

            log.debug("Building DefaultErrorHandler for listener '{}'", listenerName);
            DefaultErrorHandler errorHandler = buildDefaultErrorHandler(policy, listenerName);

            log.debug("Registering Spring bean: {}", beanName);
            BeanDefinitionBuilder beanDefBuilder = BeanDefinitionBuilder
                    .genericBeanDefinition(DefaultErrorHandler.class, () -> errorHandler);

            registry.registerBeanDefinition(beanName, beanDefBuilder.getBeanDefinition());

            log.info("Successfully registered framework error handler bean: {}", beanName);
            log.trace("EXIT: registerKafkaErrorHandlerBean - SUCCESS");

        } catch (Exception ex) {
            log.error("EXIT: registerKafkaErrorHandlerBean - FAILED: {}", ex.getMessage(), ex);
            throw ex;
        }
    }

    // End Change by SRS
    private DefaultErrorHandler buildDefaultErrorHandler(ErrorHandlingPolicy policy, String listenerName) {
        log.trace("ENTRY: buildDefaultErrorHandler - listenerName={}", listenerName);

        DefaultErrorHandler handler = null;

        try {
            boolean autoRetry = policy.isAutoRetry();
            int maxRetries = policy.getMaxRetries();  // primitive int, no null check needed
            Duration retryDelay = policy.getRetryDelay() != null ? policy.getRetryDelay() : Duration.ofSeconds(1);

            if (!autoRetry) {
                log.debug("Auto-retry disabled for listener '{}', setting maxRetries to 0", listenerName);
                maxRetries = 0;
            }

            // If maxRetries not set (is 0), default to 3
            if (maxRetries == 0 && autoRetry) {
                maxRetries = 3;
            }

            // Create exponential backoff: 1s, 2s, 4s, etc.
            log.debug("Creating ExponentialBackOffWithMaxRetries: maxRetries={}, initialInterval={}ms",
                    maxRetries, retryDelay.toMillis());
            ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
            backOff.setInitialInterval(retryDelay.toMillis());
            backOff.setMultiplier(2.0);

            // Create handler with custom recoverer for final failures
            handler = new DefaultErrorHandler(
                    (record, ex) -> logFinalKafkaFailure(record, ex, listenerName),
                    backOff
            );

            // Add non-retryable exceptions
            List<Class<? extends Throwable>> nonRetryableExceptions = policy.getNonRetryableExceptions();
            if (nonRetryableExceptions != null && !nonRetryableExceptions.isEmpty()) {
                log.debug("Adding {} non-retryable exception types for listener '{}'",
                        nonRetryableExceptions.size(), listenerName);

                // Convert List to array and pass to handler
                @SuppressWarnings("unchecked")
                Class<? extends Exception>[] exceptionArray = nonRetryableExceptions.stream()
                        .map(clazz -> (Class<? extends Exception>) clazz)
                        .toArray(Class[]::new);

                handler.addNotRetryableExceptions(exceptionArray);

                for (Class<? extends Throwable> exceptionClass : nonRetryableExceptions) {
                    log.debug("  → {} (skips retry immediately)", exceptionClass.getSimpleName());
                }
            }

            log.debug("Successfully built DefaultErrorHandler for listener '{}'", listenerName);
            log.trace("EXIT: buildDefaultErrorHandler - SUCCESS");
            return handler;

        } catch (Exception ex) {
            log.error("EXIT: buildDefaultErrorHandler - FAILED: {}", ex.getMessage(), ex);
            throw new IllegalStateException("Failed to build error handler for listener: " + listenerName, ex);
        } finally {
            // Nothing to clean up
        }
    }

    private void logFinalKafkaFailure(ConsumerRecord<?, ?> record, Exception ex, String listenerName) {
        log.trace("ENTRY: logFinalKafkaFailure - listenerName={}, topic={}, partition={}, offset={}",
                listenerName, record.topic(), record.partition(), record.offset());

        try {
            log.error("========================================");
            log.error("FINAL KAFKA FAILURE - Listener: {}", listenerName);
            log.error("Topic: {}, Partition: {}, Offset: {}",
                    record.topic(), record.partition(), record.offset());
            log.error("Key: {}", record.key());
            log.error("Timestamp: {}", record.timestamp());
            log.error("Exception: {}", ex.getMessage(), ex);
            log.error("========================================");
            log.error("Message processing failed after all retries. Offset will be committed and processing will continue.");
            log.error("========================================");

            log.trace("EXIT: logFinalKafkaFailure - SUCCESS");

        } catch (Exception loggingEx) {
            log.error("CRITICAL: Failed to log final Kafka failure: {}", loggingEx.getMessage(), loggingEx);
        } finally {
            // Nothing to clean up
        }
    }

    private void logPolicyConfiguration(String listenerName, String contextName, ErrorHandlingPolicy policy) {
        log.trace("ENTRY: logPolicyConfiguration - listenerName={}, context={}", listenerName, contextName);

        try {
            log.info("Creating error handler bean: errorHandler.{}", listenerName);
            log.info("  → Policy Code: {}", policy.getCode());
            log.info("  → Max Retries: {}", policy.getMaxRetries());
            log.info("  → Retry Delay: {}", policy.getRetryDelay());
            log.info("  → Auto Retry: {}", policy.isAutoRetry());

            List<Class<? extends Throwable>> nonRetryableExceptions = policy.getNonRetryableExceptions();
            if (nonRetryableExceptions != null && !nonRetryableExceptions.isEmpty()) {
                String exNames = nonRetryableExceptions.stream()
                        .map(Class::getSimpleName)
                        .collect(Collectors.joining(", "));
                log.info("  → Non-Retryable Exceptions: {}", exNames);
            } else {
                log.info("  → Non-Retryable Exceptions: (none)");
            }

            log.trace("EXIT: logPolicyConfiguration - SUCCESS");

        } catch (Exception ex) {
            log.warn("Failed to log policy configuration: {}", ex.getMessage());
            log.trace("EXIT: logPolicyConfiguration - FAILED (non-critical)");
        } finally {
            // Nothing to clean up
        }
    }

    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
        log.trace("ENTRY: postProcessBeanFactory");
        try {
            // No action needed in this phase
            log.trace("EXIT: postProcessBeanFactory - NO ACTION");
        } catch (Exception ex) {
            log.error("EXIT: postProcessBeanFactory - FAILED: {}", ex.getMessage(), ex);
            throw ex;
        } finally {
            // Nothing to clean up
        }
    }
}