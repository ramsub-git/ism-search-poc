// =====================================================================================
// 2. ProcessingContextDefinition.java - Context Metadata
// =====================================================================================

package com.sephora.services.ismsearchpoc.processing;

import lombok.Data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

/**
 * Defines a processing context with all metadata, step guards, and configuration.
 * Registered once at application startup and accessed throughout the application.
 */
@Data
@Slf4j
public class ProcessingContextDefinition {

    /**
     * Unique context name (e.g., "INVENTORY_UPDATE", "POSLOG").
     */
    private String contextName;

    /**
     * Searchable marker placed in source code for traceability.
     */
    private String originMarker;

    /**
     * Human-readable description of this processing context.
     */
    private String description;

    /**
     * Audit source identifier (e.g., Kafka topic, API endpoint).
     */
    private String auditSource;

    /**
     * Class that typically initiates processing for this context.
     */
    private String initiatingClass;

    /**
     * Method that typically initiates processing for this context.
     */
    private String initiatingMethod;

    /**
     * Service name for audit logging.
     */
    private String serviceName;

    private String kafkaListenerName;

    // Added by SRS 1110
    /**
     * Flag indicating whether to use pre-written error handler (team's legacy)
     * instead of framework's auto-configured handler.
     */
    private boolean usePreWrittenHandler = false;

    public boolean shouldUsePreWrittenHandler() {
        return usePreWrittenHandler;
    }

    public void setUsePreWrittenHandler(boolean usePreWrittenHandler) {
        log.trace("ENTRY: setUsePreWrittenHandler - contextName={}, value={}",
                contextName, usePreWrittenHandler);
        try {
            this.usePreWrittenHandler = usePreWrittenHandler;
            log.debug("Set usePreWrittenHandler to '{}' for context '{}'", usePreWrittenHandler, contextName);
            log.trace("EXIT: setUsePreWrittenHandler - SUCCESS");
        } catch (Exception ex) {
            log.error("EXIT: setUsePreWrittenHandler - FAILED: {}", ex.getMessage(), ex);
            throw ex;
        }
    }

    public boolean isFrameworkErrorHandlingEnabled() {
        return !usePreWrittenHandler;
    }
    // End Addition

    /**
     * Map of step guard definitions for this context.
     * Key: guard name, Value: step guard definition
     */
    private Map<String, StepGuardDefinition<?>> stepGuards = new ConcurrentHashMap<>();

    /**
     * Constructor requiring context name.
     */
    public ProcessingContextDefinition(String contextName) {
        this.contextName = contextName;
    }

    /**
     * Adds a step guard to this context.
     */
    public <T> void addStepGuard(String name, StepGuardDefinition<T> definition) {
        stepGuards.put(name, definition);
    }

    /**
     * Gets a step guard by name with proper type casting.
     */
    @SuppressWarnings("unchecked")
    public <T> StepGuardDefinition<T> getStepGuard(String name) {
        return (StepGuardDefinition<T>) stepGuards.get(name);
    }

    public String getKafkaListenerName() {
        return kafkaListenerName;
    }

    public void setKafkaListenerName(String kafkaListenerName) {
        log.trace("ENTRY: setKafkaListenerName - contextName={}, listenerName={}",
                contextName, kafkaListenerName);
        try {
            this.kafkaListenerName = kafkaListenerName;
            log.debug("Set Kafka listener name '{}' for context '{}'", kafkaListenerName, contextName);
            log.trace("EXIT: setKafkaListenerName - SUCCESS");
        } catch (Exception ex) {
            log.error("EXIT: setKafkaListenerName - FAILED: {}", ex.getMessage(), ex);
            throw ex;
        } finally {
            // Nothing to clean up
        }
    }

    public boolean isKafkaContext() {
        return kafkaListenerName != null && !kafkaListenerName.trim().isEmpty();
    }

    public ErrorHandlingPolicy getFirstStepGuardPolicy() {
        log.trace("ENTRY: getFirstStepGuardPolicy - contextName={}", contextName);

        try {
            if (stepGuards.isEmpty()) {
                String errorMsg = String.format(
                        "Context '%s' with Kafka listener '%s' has no step guards",
                        contextName, kafkaListenerName);
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }

            StepGuardDefinition<?> firstGuard = stepGuards.values().iterator().next();
            ErrorHandlingPolicy policy = firstGuard.getPolicy();

            if (policy == null) {
                String errorMsg = String.format(
                        "First step guard in context '%s' has null ErrorHandlingPolicy",
                        contextName);
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }

            log.debug("Retrieved ErrorHandlingPolicy from context '{}'", contextName);
            log.trace("EXIT: getFirstStepGuardPolicy - SUCCESS");
            return policy;

        } catch (Exception ex) {
            log.error("EXIT: getFirstStepGuardPolicy - FAILED: {}", ex.getMessage(), ex);
            throw ex;
        } finally {
            // Nothing to clean up
        }
    }


    /**
     * Gets all step guards for iteration.
     */
    public Map<String, StepGuardDefinition<?>> getStepGuards() {
        return stepGuards;
    }
}