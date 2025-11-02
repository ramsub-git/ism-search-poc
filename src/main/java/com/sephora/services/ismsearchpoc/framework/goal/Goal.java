package com.sephora.services.ismsearchpoc.framework.goal;

import com.sephora.services.ismsearchpoc.framework.metrics.MetricsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Base class for goals
 * Goals know how to evaluate themselves and calculate domain-specific metrics
 */
public abstract class Goal {
    
    private static final Logger log = LoggerFactory.getLogger(Goal.class);
    
    protected final String name;
    protected GoalStatus currentStatus;
    
    protected Goal(String name) {
        this.name = name;
        this.currentStatus = GoalStatus.NOT_STARTED;
    }
    
    /**
     * Check status and return evaluation with metrics
     */
    public GoalEvaluation checkStatus(MetricsSnapshot metrics) {
        GoalStatus newStatus = evaluate(metrics);
        
        if (newStatus != currentStatus) {
            log.info("Goal [{}] status changed: {} -> {}", name, currentStatus, newStatus);
            currentStatus = newStatus;
        }
        
        return GoalEvaluation.builder()
            .goal(this)
            .status(newStatus)
            .metrics(calculateGoalMetrics(metrics))
            .severity(getSeverity())
            .build();
    }
    
    /**
     * Evaluate current status based on metrics
     */
    protected abstract GoalStatus evaluate(MetricsSnapshot metrics);
    
    /**
     * Calculate goal-specific metrics for strategy to use
     */
    protected abstract Map<String, Object> calculateGoalMetrics(MetricsSnapshot metrics);
    
    /**
     * Severity level of this goal
     */
    public abstract Severity getSeverity();
    
    // Status checks
    public boolean isAtRisk() { 
        return currentStatus == GoalStatus.AT_RISK; 
    }
    
    public boolean isViolated() { 
        return currentStatus == GoalStatus.VIOLATED; 
    }
    
    public boolean isMet() { 
        return currentStatus == GoalStatus.MET; 
    }
    
    public String getName() { 
        return name; 
    }
    
    public GoalStatus getCurrentStatus() { 
        return currentStatus; 
    }
}
