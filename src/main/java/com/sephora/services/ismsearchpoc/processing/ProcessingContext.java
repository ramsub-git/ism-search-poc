package com.sephora.services.ismsearchpoc.processing;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * LAYER 1 (BASE): Atomic context for one logging operation.
 * <p>
 * This class contains all data needed for audit/error logging.
 * It can be used in two modes:
 *
 * <h3>1. SIMPLE MODE (Legacy API - for developers who want simplicity):</h3>
 * <pre>
 * // Created per operation, passed directly to services
 * processingAuditHelper.auditKafkaProcessing(topic, message, () -> {
 *     // business logic
 * }, "methodName");
 * </pre>
 *
 * <h3>2. CONTEXTUAL MODE (Contextual API - for advanced features):</h3>
 * <pre>
 * // Stored in ThreadLocal, propagated between layers
 * var audit = AppProcessingContext.getContext("POSLOG").getAudit();
 * audit.logKafka(message, key, () -> {
 *     // business logic - guards automatically inherit context
 * }, "methodName");
 * </pre>
 *
 * <h3>Architecture:</h3>
 * <ul>
 *   <li>Layer 1 (Base): ProcessingContext - atomic logging data</li>
 *   <li>Layer 2 (Process): ProcessingContextDefinition - domain metadata (POSLOG, INVENTORY)</li>
 *   <li>Layer 3 (App): AppProcessingContext - registry of all domains</li>
 * </ul>
 *
 * <p>Thread-safety: Immutable after creation (except guard stack for nested tracking).
 * Stored in ThreadLocal by ProcessingContextManager for contextual mode.
 *
 * @see ProcessingContextDefinition Layer 2 - Process-level metadata
 * @see AppProcessingContext Layer 3 - App-level registry
 * @see ProcessingContextManager ThreadLocal storage for contextual mode
 */
@Builder
@Data
@Slf4j
public class ProcessingContext {

    // =====================================================================================
    // BASE LAYER DATA - Required for audit/error logging
    // =====================================================================================

    /**
     * Source of processing (KAFKA, API, SCHEDULER, FILE, OUTBOUND).
     */
    private ProcessingSource source;

    /**
     * Primary identifier (topic name, endpoint, job name, file name, destination).
     */
    private String identifier;

    /**
     * Secondary identifier (partition key, request ID, etc).
     */
    private String subIdentifier;

    /**
     * Content being processed (message, request body, file data, etc).
     */
    private String content;

    /**
     * Service name for audit logging.
     */
    private String serviceName;

    /**
     * Method/function name being executed.
     */
    private String processingMethod;

    /**
     * Timestamp when processing started.
     */
    private LocalDateTime timestamp;

    /**
     * Accumulated business identifiers from all errors in this processing context.
     * Format: "SKU:12345|LOC:1001|ORDER:ABC123"
     * Updated as step guards encounter errors during processing.
     * <p>
     * NOTE: Only FAILED records are accumulated, not successful ones.
     * This provides a concise troubleshooting trail without overwhelming the log.
     */
    private String accumulatedBusinessIdentifiers;

    // =====================================================================================
    // CONTEXTUAL LAYER DATA - Used by contextual API for enhanced features
    // =====================================================================================

    /**
     * Processing event ID for linking audit to error logs.
     * Format: SOURCE:identifier:key:timestamp
     * Example: KAFKA:pos-log-topic:ORDER123:1727258400000
     * <p>
     * This enables traceability from audit entry to all related error logs.
     */
    @Builder.Default
    private String processingEventId = "LEGACY";
    ;

    /**
     * Origin marker for code traceability (searchable in source code).
     * Example: "POS_TRANSACTION_PROCESSING", "INVENTORY_UPDATE_FROM_SAP"
     * <p>
     * Enables finding the exact code that initiated this processing.
     */
    private String originMarker;

    /**
     * Context name (which processing domain: POSLOG, INVENTORY, SHIPMENT, etc).
     * Links this operation to its ProcessingContextDefinition.
     */
    private String contextName;

    // Additional Information to track the event.
    // This can be used for storing things like current app state, memory, or other information
    private Map<String, Object> metadata;


    // =====================================================================================
    // RUNTIME STATE - Guard stack management for nested guards
    // =====================================================================================

    /**
     * Current guard stack depth (how many nested guards are active).
     * Used to determine if we're in a nested guard scenario.
     */
    @Builder.Default
    private int guardStackDepth = 0;

    /**
     * Stack of active guard names for debugging and logging.
     * Example: ["processOrder", "validateLine", "checkInventory"]
     */
    @Builder.Default
    private List<String> guardStack = new ArrayList<>();

    // =====================================================================================
    // FACTORY METHODS - Easy creation for different use cases
    // =====================================================================================

    /**
     * Create a minimal context for base layer usage (legacy API).
     * <p>
     * Used by: ProcessingAuditHelper for simple audit logging
     *
     * @param source      processing source (KAFKA, API, etc)
     * @param identifier  primary identifier (topic, endpoint, etc)
     * @param content     content being processed
     * @param serviceName service name
     * @param method      method name
     * @return minimal ProcessingContext for audit logging
     */
    public static ProcessingContext minimal(ProcessingSource source, String identifier,
                                            String content, String serviceName, String method) {
        return ProcessingContext.builder()
                .source(source)
                .identifier(identifier)
                .content(content)
                .serviceName(serviceName)
                .processingMethod(method)
                .timestamp(LocalDateTime.now())
                .guardStackDepth(0)
                .guardStack(new ArrayList<>())
                .metadata(new HashMap<>())  // Empty map by default
                .build();
    }

    /**
     * Create a full context for contextual layer usage.
     * <p>
     * Used by: ContextualAuditLogger for context-aware processing
     *
     * @param source       processing source
     * @param identifier   primary identifier
     * @param content      content being processed
     * @param serviceName  service name
     * @param method       method name
     * @param eventId      processing event ID for traceability
     * @param originMarker searchable code marker
     * @param contextName  which processing domain
     * @return full ProcessingContext with all metadata
     */
    public static ProcessingContext contextual(ProcessingSource source, String identifier,
                                               String content, String serviceName, String method,
                                               String eventId, String originMarker, String contextName) {
        return ProcessingContext.builder()
                .source(source)
                .identifier(identifier)
                .content(content)
                .serviceName(serviceName)
                .processingMethod(method)
                .processingEventId(eventId)
                .originMarker(originMarker)
                .contextName(contextName)
                .timestamp(LocalDateTime.now())
                .guardStackDepth(0)
                .guardStack(new ArrayList<>())
                .metadata(new HashMap<>())  // Empty map by default
                .build();
    }

    // =====================================================================================
    // GUARD STACK MANAGEMENT - Methods for tracking nested guards
    // =====================================================================================

    /**
     * Enter a guard - increment depth and track guard name.
     * Called by ContextualStepGuard when guard() method is entered.
     *
     * @param guardName name of the guard being entered
     */
    public void enterGuard(String guardName) {
        guardStackDepth++;
        guardStack.add(guardName);
    }

    /**
     * Exit a guard - decrement depth and remove guard name.
     * Called by ContextualStepGuard when guard() method exits.
     */
    public void exitGuard() {
        if (guardStackDepth > 0) {
            guardStackDepth--;
            if (!guardStack.isEmpty()) {
                guardStack.remove(guardStack.size() - 1);
            }
        }
    }

    /**
     * Check if we're currently in a nested guard scenario.
     * Used by ContextualStepGuard to determine relog behavior.
     *
     * @return true if guard stack depth > 1 (nested guards exist)
     */
    public boolean isNestedGuard() {
        return guardStackDepth > 1;
    }

    /**
     * Get string representation of current guard stack for debugging.
     * Example: "processOrder -> validateLine -> checkInventory"
     *
     * @return guard stack as string
     */
    public String getGuardStackRepresentation() {
        return String.join(" -> ", guardStack);
    }

    // =====================================================================================
    // UTILITY METHODS
    // =====================================================================================

    /**
     * Create a copy of this context with updated event ID.
     * Used for child processing scenarios.
     *
     * @param newEventId new processing event ID
     * @return new ProcessingContext with updated event ID
     */
    public ProcessingContext withEventId(String newEventId) {
        return ProcessingContext.builder()
                .source(this.source)
                .identifier(this.identifier)
                .subIdentifier(this.subIdentifier)
                .content(this.content)
                .serviceName(this.serviceName)
                .processingMethod(this.processingMethod)
                .timestamp(this.timestamp)
                .processingEventId(newEventId)
                .originMarker(this.originMarker)
                .contextName(this.contextName)
                .guardStackDepth(this.guardStackDepth)
                .guardStack(new ArrayList<>(this.guardStack))
                .build();
    }

    /**
     * Check if this is a contextual mode context (has contextual metadata).
     *
     * @return true if this context has eventId, originMarker, or contextName
     */
    public boolean isContextual() {
        return processingEventId != null || originMarker != null || contextName != null;
    }

    public void addMetadata(String key, Object value) {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }
        this.metadata.put(key, value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ProcessingContext{");
        sb.append("source=").append(source);
        sb.append(", identifier=").append(identifier);
        sb.append(", method=").append(processingMethod);

        if (processingEventId != null) {
            sb.append(", eventId=").append(processingEventId);
        }
        if (contextName != null) {
            sb.append(", contextName=").append(contextName);
        }
        if (guardStackDepth > 0) {
            sb.append(", guardDepth=").append(guardStackDepth);
            sb.append(", guardStack=").append(getGuardStackRepresentation());
        }

        sb.append("}");
        return sb.toString();
    }


    // NEW: Store business entities for automatic extraction
    private List<Object> businessEntities;

    /**
     * Add a business entity to this context for automatic business ID extraction.
     * Guards and audit loggers will automatically find and extract business IDs from these entities.
     *
     * @param entity the business entity (Request, Skuloc, OrderLine, etc.)
     */
    public void addBusinessEntity(Object entity) {
        if (entity == null) {
            return;
        }
        if (this.businessEntities == null) {
            this.businessEntities = new ArrayList<>();
        }
        this.businessEntities.add(entity);
    }

    /**
     * Get business entity of specific type from context.
     * If multiple entities of same type, returns the LAST one added (most recent).
     *
     * @param entityType the class type to search for
     * @return the entity, or null if not found
     */
    @SuppressWarnings("unchecked")
    public <T> T getBusinessEntity(Class<T> entityType) {
        if (businessEntities == null || businessEntities.isEmpty()) {
            return null;
        }

        // Search backwards (most recent first)
        for (int i = businessEntities.size() - 1; i >= 0; i--) {
            Object entity = businessEntities.get(i);
            if (entityType.isInstance(entity)) {
                return (T) entity;
            }
        }
        return null;
    }

    /**
     * Get all business entities of specific type from context.
     * Useful when processing multiple entities of same type (e.g., order lines).
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getBusinessEntities(Class<T> entityType) {
        if (businessEntities == null || businessEntities.isEmpty()) {
            return Collections.emptyList();
        }

        return businessEntities.stream()
                .filter(entityType::isInstance)
                .map(entity -> (T) entity)
                .collect(Collectors.toList());
    }

    /**
     * Get all business entities (any type) from context.
     */
    public List<Object> getAllBusinessEntities() {
        return businessEntities != null ?
                Collections.unmodifiableList(businessEntities) :
                Collections.emptyList();
    }

    /**
     * Check if context has any business entities.
     */
    public boolean hasBusinessEntities() {
        return businessEntities != null && !businessEntities.isEmpty();
    }

    /**
     * Clear all business entities from context.
     * Useful when transitioning between processing phases.
     */
    public void clearBusinessEntities() {
        if (businessEntities != null) {
            businessEntities.clear();
        }
    }


    /**
     * Append a business identifier to the accumulated list.
     * Avoids duplicates and maintains pipe-delimited format.
     * <p>
     * Called by ContextualStepGuard when an error occurs during processing.
     *
     * @param businessId Business identifier string (e.g., "SKU:12345|LOC:1001")
     */
    public void appendBusinessIdentifier(String businessId) {
        log.trace("Appending business identifier to context: {}", businessId);

        if (businessId == null || businessId.trim().isEmpty()) {
            log.trace("Business identifier is null or empty, skipping append");
            return;
        }

        try {
            if (this.accumulatedBusinessIdentifiers == null) {
                this.accumulatedBusinessIdentifiers = businessId;
                log.debug("First business identifier accumulated: {}", businessId);
            } else {
                // Avoid duplicates
                if (!this.accumulatedBusinessIdentifiers.contains(businessId)) {
                    this.accumulatedBusinessIdentifiers += "|" + businessId;
                    log.debug("Business identifier appended. Total: {}", this.accumulatedBusinessIdentifiers);
                } else {
                    log.trace("Business identifier already present, skipping: {}", businessId);
                }
            }
        } catch (Exception ex) {
            // CRITICAL: Never let business ID accumulation break processing
            log.error("CRITICAL: Failed to append business identifier '{}': {}", businessId, ex.getMessage(), ex);
        }
    }

    /**
     * Get all accumulated business identifiers from this processing context.
     * Returns pipe-delimited string of all business IDs from failed records.
     *
     * @return Accumulated business identifiers, or null if none accumulated
     */
    public String getAccumulatedBusinessIdentifiers() {
        return accumulatedBusinessIdentifiers;
    }

}