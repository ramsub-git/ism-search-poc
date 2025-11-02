package com.sephora.services.ismsearchpoc.framework.runtime;

import com.sephora.services.ismsearchpoc.framework.engine.ParallelBatchEngine;
import com.sephora.services.ismsearchpoc.framework.goal.Goal;
import com.sephora.services.ismsearchpoc.framework.goal.GoalEvaluation;
import com.sephora.services.ismsearchpoc.framework.goal.GoalStatus;
import com.sephora.services.ismsearchpoc.framework.goal.Severity;
import com.sephora.services.ismsearchpoc.framework.metrics.MetricsSnapshot;
import com.sephora.services.ismsearchpoc.framework.strategy.DialAdjustment;
import com.sephora.services.ismsearchpoc.framework.strategy.GoalStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Runtime manager that evaluates goals, collects strategy recommendations,
 * resolves conflicts, and adjusts engine concurrency
 */
public class ISMRuntimeManager {
    
    private static final Logger log = LoggerFactory.getLogger(ISMRuntimeManager.class);
    
    private final List<Goal> goals;
    private final Map<Goal, GoalStrategy> strategies;
    private final ParallelBatchEngine<?, ?, ?> engine;
    
    // Safety bounds
    private final int minWorkItemConcurrency = 1;
    private final int maxWorkItemConcurrency = 30;
    private final int minProcessingConcurrency = 1;
    private final int maxProcessingConcurrency = 20;
    
    // Current dial positions
    private int currentWorkItemConcurrency = 10;
    private int currentProcessingConcurrency = 5;
    
    // Oscillation protection
    private static final Duration MIN_ADJUSTMENT_INTERVAL = Duration.ofMinutes(10);
    private Instant lastAdjustment = Instant.MIN;
    
    public ISMRuntimeManager(List<Goal> goals, 
                            Map<Goal, GoalStrategy> strategies,
                            ParallelBatchEngine<?, ?, ?> engine) {
        this.goals = goals;
        this.strategies = strategies;
        this.engine = engine;
    }
    
    /**
     * Main evaluation and adjustment cycle
     */
    public void evaluateAndAdjust(MetricsSnapshot metrics) {
        log.info("=== Runtime Manager Evaluation Cycle ===");
        
        // Check oscillation protection
        if (Duration.between(lastAdjustment, Instant.now()).compareTo(MIN_ADJUSTMENT_INTERVAL) < 0) {
            log.debug("Skipping adjustment - within cooldown period");
            return;
        }
        
        // Step 1: Evaluate all goals
        List<GoalEvaluation> evaluations = goals.stream()
            .map(goal -> goal.checkStatus(metrics))
            .collect(Collectors.toList());
        
        logGoalStatus(evaluations);
        
        // Step 2: Check for critical violations requiring abort
        if (shouldAbort(evaluations)) {
            String reason = "Critical goal violations detected";
            log.error("ABORTING: {}", reason);
            engine.abort(reason);
            return;
        }
        
        // Step 3: Collect strategy recommendations
        Map<Goal, DialAdjustment> recommendations = evaluations.stream()
            .collect(Collectors.toMap(
                GoalEvaluation::getGoal,
                eval -> strategies.get(eval.getGoal()).recommendAdjustment(eval)
            ));
        
        logRecommendations(recommendations);
        
        // Step 4: Resolve conflicts and make final decision
        DialAdjustment finalAdjustment = resolveConflicts(evaluations, recommendations);
        
        // Step 5: Apply adjustment if needed
        if (!finalAdjustment.isNoChange()) {
            applyAdjustment(finalAdjustment);
            lastAdjustment = Instant.now();
        } else {
            log.info("No adjustment needed - all goals satisfied");
        }
        
        log.info("=== End Evaluation Cycle ===\n");
    }
    
    /**
     * Determine if execution should abort
     */
    private boolean shouldAbort(List<GoalEvaluation> evaluations) {
        // Abort if any CRITICAL severity goal is VIOLATED
        return evaluations.stream()
            .anyMatch(eval -> eval.getSeverity() == Severity.CRITICAL && 
                            eval.getStatus() == GoalStatus.VIOLATED);
    }
    
    /**
     * Resolve conflicts between strategy recommendations
     * Uses severity-based prioritization with conservative bias
     */
    private DialAdjustment resolveConflicts(List<GoalEvaluation> evaluations,
                                           Map<Goal, DialAdjustment> recommendations) {
        
        log.info("Resolving conflicts...");
        
        // Separate recommendations by direction
        List<DialAdjustment> increases = new ArrayList<>();
        List<DialAdjustment> decreases = new ArrayList<>();
        
        for (Map.Entry<Goal, DialAdjustment> entry : recommendations.entrySet()) {
            DialAdjustment adj = entry.getValue();
            if (adj.getWorkItemConcurrencyDelta() > 0 || adj.getProcessingConcurrencyDelta() > 0) {
                increases.add(adj);
            } else if (adj.getWorkItemConcurrencyDelta() < 0 || adj.getProcessingConcurrencyDelta() < 0) {
                decreases.add(adj);
            }
        }
        
        // If we have both increases and decreases, resolve by severity
        if (!increases.isEmpty() && !decreases.isEmpty()) {
            return resolveIncreaseDecreaseConflict(evaluations, recommendations);
        }
        
        // If only increases, pick the most aggressive (we have headroom)
        if (!increases.isEmpty()) {
            DialAdjustment maxIncrease = increases.stream()
                .max(Comparator.comparingInt(a -> 
                    Math.abs(a.getWorkItemConcurrencyDelta()) + 
                    Math.abs(a.getProcessingConcurrencyDelta())))
                .orElse(DialAdjustment.noChange());
            
            log.info("Only increases recommended - selecting most aggressive");
            return maxIncrease;
        }
        
        // If only decreases, pick the most aggressive (we're constrained)
        if (!decreases.isEmpty()) {
            DialAdjustment maxDecrease = decreases.stream()
                .max(Comparator.comparingInt(a -> 
                    Math.abs(a.getWorkItemConcurrencyDelta()) + 
                    Math.abs(a.getProcessingConcurrencyDelta())))
                .orElse(DialAdjustment.noChange());
            
            log.info("Only decreases recommended - selecting most aggressive");
            return maxDecrease;
        }
        
        // No recommendations
        return DialAdjustment.noChange();
    }
    
    /**
     * Resolve conflict when we have both increase and decrease recommendations
     * High/Critical severity goals trump performance goals
     */
    private DialAdjustment resolveIncreaseDecreaseConflict(List<GoalEvaluation> evaluations,
                                                           Map<Goal, DialAdjustment> recommendations) {
        
        log.info("Conflicting recommendations detected - applying severity-based resolution");
        
        // Find highest severity violated or at-risk goal
        Optional<GoalEvaluation> highestSeverityIssue = evaluations.stream()
            .filter(eval -> eval.getStatus() == GoalStatus.VIOLATED || 
                           eval.getStatus() == GoalStatus.AT_RISK)
            .max(Comparator.comparing(GoalEvaluation::getSeverity));
        
        if (highestSeverityIssue.isPresent()) {
            Goal priorityGoal = highestSeverityIssue.get().getGoal();
            DialAdjustment priorityAdjustment = recommendations.get(priorityGoal);
            
            log.info("Prioritizing {} goal (severity: {})", 
                priorityGoal.getName(), highestSeverityIssue.get().getSeverity());
            
            return priorityAdjustment;
        }
        
        // If no issues, prefer to maintain current state
        return DialAdjustment.noChange();
    }
    
    /**
     * Apply the dial adjustment with safety bounds
     */
    private void applyAdjustment(DialAdjustment adjustment) {
        int newWorkItem = currentWorkItemConcurrency + adjustment.getWorkItemConcurrencyDelta();
        int newProcessing = currentProcessingConcurrency + adjustment.getProcessingConcurrencyDelta();
        
        // Apply safety bounds
        newWorkItem = Math.max(minWorkItemConcurrency, Math.min(maxWorkItemConcurrency, newWorkItem));
        newProcessing = Math.max(minProcessingConcurrency, Math.min(maxProcessingConcurrency, newProcessing));
        
        // Only adjust if values actually changed
        if (newWorkItem != currentWorkItemConcurrency || newProcessing != currentProcessingConcurrency) {
            log.info("Applying adjustment: workItem {} -> {}, processing {} -> {} [Reason: {}]",
                currentWorkItemConcurrency, newWorkItem,
                currentProcessingConcurrency, newProcessing,
                adjustment.getReason());
            
            engine.adjustConcurrency(newWorkItem, newProcessing);
            
            currentWorkItemConcurrency = newWorkItem;
            currentProcessingConcurrency = newProcessing;
        } else {
            log.info("Adjustment capped by safety bounds - no change applied");
        }
    }
    
    /**
     * Log current goal status
     */
    private void logGoalStatus(List<GoalEvaluation> evaluations) {
        log.info("Goal Status:");
        for (GoalEvaluation eval : evaluations) {
            log.info("  [{}] {} (severity: {})", 
                eval.getGoal().getName(), 
                eval.getStatus(),
                eval.getSeverity());
        }
    }
    
    /**
     * Log strategy recommendations
     */
    private void logRecommendations(Map<Goal, DialAdjustment> recommendations) {
        log.info("Strategy Recommendations:");
        for (Map.Entry<Goal, DialAdjustment> entry : recommendations.entrySet()) {
            DialAdjustment adj = entry.getValue();
            if (adj.isNoChange()) {
                log.info("  [{}] No change", entry.getKey().getName());
            } else {
                log.info("  [{}] {}", entry.getKey().getName(), adj);
            }
        }
    }
    
    /**
     * Get current dial settings
     */
    public Map<String, Integer> getCurrentSettings() {
        Map<String, Integer> settings = new HashMap<>();
        settings.put("workItemConcurrency", currentWorkItemConcurrency);
        settings.put("processingConcurrency", currentProcessingConcurrency);
        return settings;
    }
}
