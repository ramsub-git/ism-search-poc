// =====================================================================================
// Phase 2: Core Framework Classes - NEW FILES
// =====================================================================================
// These are the core framework components that enable business-aware error logging,
// elegant usage patterns, and complete operational traceability.
// =====================================================================================

// =====================================================================================
// 1. ErrorHandlingPolicy.java - Policy Definition
// =====================================================================================

package com.sephora.services.ismsearchpoc.processing;

import lombok.Builder;
import lombok.Data;

import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Defines the error handling policy for a specific type of business error.
 * Policies control escalation levels, team routing, retry behavior, and operational procedures.
 */
@Data
@Builder
public class ErrorHandlingPolicy {

    /**
     * Unique policy code (e.g., "ERROR_SKU_UPDATE", "ERROR_RESERVE_CALC").
     */
    private String code;

    /**
     * Human-readable description of what this error means.
     */
    private String description;

    /**
     * Business impact statement for operational teams.
     */
    private String businessImpact;

    /**
     * Escalation level determining urgency and response time.
     */
    private EscalationLevel escalationLevel;

    /**
     * Team responsible for handling this error type.
     */
    private String responsibleTeam;

    /**
     * Immediate action to take when this error occurs.
     */
    private String immediateAction;

    /**
     * Step-by-step diagnostic procedures.
     */
    private List<String> diagnosticSteps;

    /**
     * Resolution steps to fix the issue.
     */
    private List<String> resolutionSteps;

    /**
     * Whether the system should automatically retry on this error.
     */
    private boolean autoRetry;

    /**
     * Maximum number of retry attempts.
     */
    private int maxRetries;

    /**
     * Delay between retry attempts.
     */
    private Duration retryDelay;

    /**
     * List of exception types that should NOT be retried.
     * These exceptions will skip retry logic and fail immediately.
     * Example: NullPointerException, JsonProcessingException (poison pills)
     */
    private List<Class<? extends Throwable>> nonRetryableExceptions;

    public List<Class<? extends Throwable>> getNonRetryableExceptions() {
        return nonRetryableExceptions != null ?
                Collections.unmodifiableList(nonRetryableExceptions) :
                Collections.emptyList();
    }

    /**
     * Escalation priority levels.
     */
    public enum EscalationLevel {
        P0,  // Critical - immediate response required
        P1,  // High -  response required
        P2,  // Medium - response within hours
        P3   // Low - response within business day
    }

    /**
     * Validates that the policy is properly configured.
     */
    public boolean isValid() {
        return code != null && !code.trim().isEmpty() &&
                escalationLevel != null &&
                responsibleTeam != null && !responsibleTeam.trim().isEmpty();
    }
}