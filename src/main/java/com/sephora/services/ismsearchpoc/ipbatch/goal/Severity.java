package com.sephora.services.ismsearchpoc.ipbatch.goal;

/**
 * Severity level of a goal
 * Used for prioritization in conflict resolution
 */
public enum Severity {
    /**
     * Critical goal - system cannot function if violated
     * Examples: Time deadline, critical errors
     */
    CRITICAL,
    
    /**
     * High priority goal - should be respected
     * Examples: Resource limits, error thresholds
     */
    HIGH,
    
    /**
     * Medium priority goal - important but can be traded off
     * Examples: Performance targets
     */
    MEDIUM,
    
    /**
     * Low priority goal - nice to have
     * Examples: Optimization targets
     */
    LOW
}
