package com.sephora.services.ismsearchpoc.ipbatch.goal;

import java.util.Map;

/**
 * Result of evaluating a goal
 * Contains status and goal-specific metrics
 */
public class GoalEvaluation {
    
    private final Goal goal;
    private final GoalStatus status;
    private final Map<String, Object> metrics;
    private final Severity severity;
    
    private GoalEvaluation(Builder builder) {
        this.goal = builder.goal;
        this.status = builder.status;
        this.metrics = builder.metrics;
        this.severity = builder.severity;
    }
    
    public Goal getGoal() {
        return goal;
    }
    
    public GoalStatus getStatus() {
        return status;
    }
    
    public Map<String, Object> getMetrics() {
        return metrics;
    }
    
    public Severity getSeverity() {
        return severity;
    }
    
    public boolean isAtRisk() {
        return status == GoalStatus.AT_RISK;
    }
    
    public boolean isViolated() {
        return status == GoalStatus.VIOLATED;
    }
    
    public boolean isMet() {
        return status == GoalStatus.MET;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private Goal goal;
        private GoalStatus status;
        private Map<String, Object> metrics;
        private Severity severity;
        
        public Builder goal(Goal goal) {
            this.goal = goal;
            return this;
        }
        
        public Builder status(GoalStatus status) {
            this.status = status;
            return this;
        }
        
        public Builder metrics(Map<String, Object> metrics) {
            this.metrics = metrics;
            return this;
        }
        
        public Builder severity(Severity severity) {
            this.severity = severity;
            return this;
        }
        
        public GoalEvaluation build() {
            return new GoalEvaluation(this);
        }
    }
}
