// =====================================================================================
// This component enables automatic context inheritance between audit logging and
// business error guards using thread-local storage. It eliminates manual event ID
// passing and ensures complete traceability.
// =====================================================================================

// =====================================================================================
// ProcessingContextManager.java - Thread-Local Context Propagation
// =====================================================================================

package com.sephora.services.ismsearchpoc.processing;

import lombok.extern.slf4j.Slf4j;

/**
 * Manages thread-local processing context for automatic inheritance between
 * audit logging and business error guards.
 *
 * <p>This enables the elegant pattern where:
 * <ul>
 *   <li>Audit logger sets the processing event ID in thread-local storage</li>
 *   <li>Step guards automatically inherit the event ID for error logging</li>
 *   <li>Complete traceability without manual event ID passing</li>
 *   <li>Nested guard detection to control rethrow/relog behavior</li>
 * </ul>
 *
 * <p>Thread safety: Uses ThreadLocal for thread-safe context isolation.
 * Each thread maintains its own independent context stack.
 *
 * <p>Example flow:
 * <pre>
 * // 1. Audit logger sets context
 * ProcessingContextManager.setCurrentContext("KAFKA:topic:key:12345", "INVENTORY_UPDATE");
 *
 * try {
 *     // 2. Business logic executes with step guards
 *     skuGuard.guard(entity, () -> {
 *         // Step guard automatically gets event ID from thread-local
 *         String eventId = ProcessingContextManager.getCurrentEventId();
 *         // Uses inherited context for error logging
 *     });
 * } finally {
 *     // 3. Context cleanup
 *     ProcessingContextManager.clearContext();
 * }
 * </pre>
 */


/**
 * Thread-local storage for processing context.
 *
 * <h3>Purpose:</h3>
 * Enables automatic context propagation between layers:
 * <ul>
 *   <li>Layer 2 (Contextual) sets context via ContextualAuditLogger</li>
 *   <li>Layer 1 (Base) reads context via guards/error logging</li>
 *   <li>Complete traceability without manual parameter passing</li>
 * </ul>
 *
 * <h3>Example Flow:</h3>
 * <pre>
 * // 1. ContextualAuditLogger sets context
 * ProcessingContextManager.setCurrentContext(context);
 *
 * try {
 *     // 2. Business logic with step guards
 *     guard.guard(entity, () -> {
 *         // Guard automatically inherits context from ThreadLocal
 *         // No manual parameter passing needed!
 *     }, "methodName");
 * } finally {
 *     // 3. CRITICAL: Always clear context to prevent memory leaks
 *     ProcessingContextManager.clearContext();
 * }
 * </pre>
 *
 * <h3>Thread Safety:</h3>
 * Uses ThreadLocal for thread-safe context isolation.
 * Each thread maintains its own independent context.
 *
 * <p><strong>CRITICAL:</strong> Always call clearContext() in finally block
 * to prevent ThreadLocal memory leaks!
 *
 * @see ProcessingContext Layer 1 - Atomic context data
 * @see ProcessingContextDefinition Layer 2 - Process-level metadata
 * @see AppProcessingContext Layer 3 - App-level registry
 */
@Slf4j
public class ProcessingContextManager {

    // =====================================================================================
    // THREAD-LOCAL STORAGE
    // =====================================================================================

    /**
     * Thread-local storage for the complete processing context.
     * <p>
     * BEFORE (5 separate ThreadLocals):
     * - ThreadLocal<String> currentEventId
     * - ThreadLocal<String> currentOriginMarker
     * - ThreadLocal<String> currentContextName
     * - ThreadLocal<Integer> guardStackDepth
     * - ThreadLocal<StringBuilder> guardStackNames
     * <p>
     * AFTER (1 ThreadLocal with complete object):
     * - ThreadLocal<ProcessingContext> currentContext
     * <p>
     * Benefits:
     * - Simpler code (1 object vs 5 variables)
     * - Less error-prone (impossible to forget to clear one)
     * - Better performance (1 ThreadLocal lookup vs 5)
     * - Easier to reason about
     */
    private static final ThreadLocal<ProcessingContext> currentContext = new ThreadLocal<>();

    // =====================================================================================
    // CONTEXT LIFECYCLE
    // =====================================================================================

    /**
     * Set current processing context for this thread.
     *
     * <p>Used by: ContextualAuditLogger (Layer 2) when starting processing
     *
     * @param context complete processing context to store
     * @throws IllegalArgumentException if context is null
     */
    public static void setCurrentContext(ProcessingContext context) {
        if (context == null) {
            throw new IllegalArgumentException("ProcessingContext cannot be null");
        }

        currentContext.set(context);

        log.trace("Set processing context: eventId={}, contextName={}, method={}",
                context.getProcessingEventId(),
                context.getContextName(),
                context.getProcessingMethod());
    }

    /**
     * Get current processing context from thread-local storage.
     *
     * <p>Used by: ContextualStepGuard (Layer 2), error logging (Layer 1)
     *
     * @return current ProcessingContext, or null if not set
     */
    public static ProcessingContext getCurrentContext() {
        return currentContext.get();
    }

    /**
     * Clear processing context from thread-local storage.
     *
     * <p><strong>CRITICAL:</strong> MUST be called in finally block to prevent memory leaks!
     *
     * <p>ThreadLocal variables can cause memory leaks if not properly cleaned up,
     * especially in thread pool scenarios where threads are reused.
     *
     * <p>Used by: ContextualAuditLogger in finally block
     */
    public static void clearContext() {
        ProcessingContext ctx = currentContext.get();
        currentContext.remove();

        if (ctx != null) {
            log.trace("Cleared processing context: eventId={}, contextName={}, finalDepth={}",
                    ctx.getProcessingEventId(),
                    ctx.getContextName(),
                    ctx.getGuardStackDepth());
        }
    }

    // =====================================================================================
    // CONVENIENCE METHODS - Backward compatibility with existing code
    // =====================================================================================

    /**
     * Get processing event ID from current context.
     *
     * @return event ID, or null if no context set
     */
    public static String getCurrentEventId() {
        ProcessingContext ctx = currentContext.get();
        return ctx != null ? ctx.getProcessingEventId() : null;
    }

    /**
     * Get origin marker from current context.
     *
     * @return origin marker, or null if no context set
     */
    public static String getCurrentOriginMarker() {
        ProcessingContext ctx = currentContext.get();
        return ctx != null ? ctx.getOriginMarker() : null;
    }

    /**
     * Get context name from current context.
     *
     * @return context name, or null if no context set
     */
    public static String getCurrentContextName() {
        ProcessingContext ctx = currentContext.get();
        return ctx != null ? ctx.getContextName() : null;
    }

    /**
     * Check if we're currently in a nested guard scenario.
     *
     * @return true if guard stack depth > 1
     */
    public static boolean isNestedGuard() {
        ProcessingContext ctx = currentContext.get();
        return ctx != null && ctx.isNestedGuard();
    }

    /**
     * Get current guard stack depth.
     *
     * @return guard stack depth, or 0 if no context set
     */
    public static int getGuardStackDepth() {
        ProcessingContext ctx = currentContext.get();
        return ctx != null ? ctx.getGuardStackDepth() : 0;
    }

    /**
     * Get string representation of guard stack.
     *
     * @return guard stack string, or empty string if no context set
     */
    public static String getGuardStackRepresentation() {
        ProcessingContext ctx = currentContext.get();
        return ctx != null ? ctx.getGuardStackRepresentation() : "";
    }

    // =====================================================================================
    // GUARD STACK MANAGEMENT
    // =====================================================================================

    /**
     * Enter a guard - increment depth and track guard name.
     *
     * <p>Used by: ContextualStepGuard when guard() method is entered
     *
     * @param guardName name of the guard being entered
     */
    public static void enterGuard(String guardName) {
        ProcessingContext ctx = currentContext.get();
        if (ctx != null) {
            ctx.enterGuard(guardName);
            log.trace("Entered guard: {} (depth: {}, stack: {})",
                    guardName,
                    ctx.getGuardStackDepth(),
                    ctx.getGuardStackRepresentation());
        } else {
            log.trace("Entered guard: {} (no context set)", guardName);
        }
    }

    /**
     * Exit a guard - decrement depth and remove guard name.
     *
     * <p>Used by: ContextualStepGuard when guard() method exits (in finally block)
     *
     * @param guardName name of the guard being exited (for logging only)
     */
    public static void exitGuard(String guardName) {
        ProcessingContext ctx = currentContext.get();
        if (ctx != null) {
            ctx.exitGuard();
            log.trace("Exited guard: {} (depth: {}, stack: {})",
                    guardName,
                    ctx.getGuardStackDepth(),
                    ctx.getGuardStackRepresentation());
        } else {
            log.trace("Exited guard: {} (no context set)", guardName);
        }
    }

    // =====================================================================================
    // UTILITY METHODS
    // =====================================================================================

    /**
     * Check if a processing context is currently set.
     * Useful for validation and debugging.
     *
     * @return true if context exists, false otherwise
     */
    public static boolean hasContext() {
        return currentContext.get() != null;
    }

    /**
     * Get complete context information for debugging.
     *
     * @return formatted string with all context details
     */
    public static String getContextInfo() {
        ProcessingContext ctx = currentContext.get();
        if (ctx == null) {
            return "No processing context set";
        }

        return ctx.toString();
    }

    /**
     * Update the processing event ID in current context.
     * Rarely needed, but available for complex scenarios where event ID changes mid-processing.
     *
     * @param newEventId new processing event ID
     */
    public static void updateEventId(String newEventId) {
        ProcessingContext ctx = currentContext.get();
        if (ctx != null) {
            String oldEventId = ctx.getProcessingEventId();
            ProcessingContext updated = ctx.withEventId(newEventId);
            currentContext.set(updated);

            log.debug("Updated processing event ID: {} -> {}", oldEventId, newEventId);
        } else {
            log.warn("Cannot update event ID - no processing context set");
        }
    }

    /**
     * Create a child event ID for sub-processing scenarios.
     * Format: parent-event-id:child-identifier
     *
     * @param childIdentifier unique identifier for the child processing
     * @return child event ID, or just the childIdentifier if no parent context exists
     */
    public static String createChildEventId(String childIdentifier) {
        String parentEventId = getCurrentEventId();
        if (parentEventId == null) {
            log.warn("Cannot create child event ID - no parent context set, returning: {}", childIdentifier);
            return childIdentifier;
        }

        return parentEventId + ":" + childIdentifier;
    }

    /**
     * Add a business entity to the current context for automatic extraction.
     */
    public static void addBusinessEntity(Object entity) {
        ProcessingContext ctx = currentContext.get();
        if (ctx != null) {
            ctx.addBusinessEntity(entity);
            log.trace("Added business entity to context: {}",
                    entity != null ? entity.getClass().getSimpleName() : "null");
        } else {
            log.warn("Attempted to add business entity but no context is set");
        }
    }

    /**
     * Get business entity of specific type from current context.
     */
    public static <T> T getBusinessEntity(Class<T> entityType) {
        ProcessingContext ctx = currentContext.get();
        return ctx != null ? ctx.getBusinessEntity(entityType) : null;
    }

    /**
     * Check if current context has any business entities.
     */
    public static boolean hasBusinessEntities() {
        ProcessingContext ctx = currentContext.get();
        return ctx != null && ctx.hasBusinessEntities();
    }


}

// =====================================================================================
// USAGE EXAMPLES AND PATTERNS
// =====================================================================================
/*

Example 1: Basic Audit -> Guard Flow
=====================================

// Audit logger sets context
ProcessingContextManager.setCurrentContext(
    "KAFKA:pos-log-topic:ORDER123:1727258400000", 
    "POS_TRANSACTION_PROCESSING"
);

try {
    // Business logic with step guards
    posGuard.guard(transaction, () -> {
        // Guard automatically inherits event ID
        // No manual event ID passing needed!
        processTransaction(transaction);
    }, "processTransaction");
    
} finally {
    ProcessingContextManager.clearContext();
}


Example 2: Nested Guards with Stack Tracking
=============================================

ProcessingContextManager.setCurrentContext(eventId, originMarker);

try {
    inventoryGuard.guard(data, () -> {                      // Depth: 1
        
        skuGuard.guard(data.getSkuRecord(), () -> {         // Depth: 2 (nested!)
            updateSku(data.getSkuRecord());
            
            reserveGuard.guard(data.getReserve(), () -> {   // Depth: 3 (deeply nested!)
                calculateReserves(data.getReserve());
            }, "calculateReserves");
            
        }, "updateSku");
        
    }, "processInventory");
    
} finally {
    ProcessingContextManager.clearContext();
}

// Guard stack at depth 3: "processInventory -> updateSku -> calculateReserves"


Example 3: Child Processing Event
==================================

ProcessingContextManager.setCurrentContext(parentEventId, originMarker);

try {
    for (int i = 0; i < items.size(); i++) {
        // Create child event ID for each item
        String childEventId = ProcessingContextManager.createChildEventId("item-" + i);
        ProcessingContextManager.updateEventId(childEventId);
        
        // Process with child event ID
        itemGuard.guard(items.get(i), () -> {
            processItem(items.get(i));
        }, "processItem");
    }
    
} finally {
    ProcessingContextManager.clearContext();
}


Example 4: Context Validation and Debugging
============================================

// Check if context is set before using guards
if (!ProcessingContextManager.hasContext()) {
    log.warn("No processing context - guards may not link to audit entries");
}

// Log detailed context information
log.debug("Current context: {}", ProcessingContextManager.getContextInfo());

// Check if we're in nested guard scenario
if (ProcessingContextManager.isNestedGuard()) {
    log.trace("Processing in nested guard: {}", 
        ProcessingContextManager.getGuardStackRepresentation());
}

*/

// =====================================================================================
// VERIFICATION CHECKLIST
// =====================================================================================
/*
After creating ProcessingContextManager, verify:

1. Thread-Local Storage:
   ✓ currentEventId - processing event ID
   ✓ currentOriginMarker - code traceability marker
   ✓ guardStackDepth - nested guard detection
   ✓ guardStackNames - guard stack representation

2. Context Lifecycle:
   ✓ setCurrentContext() - initialization
   ✓ clearContext() - cleanup (MUST be in finally blocks)
   ✓ Proper cleanup prevents thread-local memory leaks

3. Context Retrieval:
   ✓ getCurrentEventId() - for error logging
   ✓ getCurrentOriginMarker() - for traceability
   ✓ getGuardStackDepth() - for nested detection
   ✓ getGuardStackRepresentation() - for debugging

4. Guard Stack Management:
   ✓ enterGuard() - increment depth, track names
   ✓ exitGuard() - decrement depth, pop names
   ✓ isNestedGuard() - check if depth > 1
   ✓ Proper stack management for nested scenarios

5. Advanced Features:
   ✓ hasContext() - validation
   ✓ getContextInfo() - debugging
   ✓ updateEventId() - mid-processing updates
   ✓ createChildEventId() - sub-processing scenarios

Next Phase: Complete AppProcessingContext, ContextualAuditLogger, ContextualStepGuard
*/