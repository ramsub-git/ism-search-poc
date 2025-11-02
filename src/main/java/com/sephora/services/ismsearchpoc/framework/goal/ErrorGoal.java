package com.sephora.services.ismsearchpoc.framework.goal;

import com.sephora.services.ismsearchpoc.framework.metrics.MetricsSnapshot;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Goal for keeping errors within acceptable limits
 */
public class ErrorGoal extends Goal {
    
    private final double maxErrorRatePerFile;
    private final int maxTotalErrorCount;
    private final Set<String> criticalErrorTypes;
    
    public ErrorGoal(double maxErrorRatePerFile, 
                    int maxTotalErrorCount, 
                    Set<String> criticalErrorTypes) {
        super("Error");
        this.maxErrorRatePerFile = maxErrorRatePerFile;
        this.maxTotalErrorCount = maxTotalErrorCount;
        this.criticalErrorTypes = criticalErrorTypes;
    }
    
    @Override
    protected GoalStatus evaluate(MetricsSnapshot metrics) {
        if (metrics.hasCriticalError(criticalErrorTypes)) {
            return GoalStatus.VIOLATED;
        }
        
        if (metrics.getTotalErrors() > maxTotalErrorCount) {
            return GoalStatus.VIOLATED;
        }
        
        if (metrics.getTotalErrors() > maxTotalErrorCount * 0.7) {
            return GoalStatus.AT_RISK;
        }
        
        return GoalStatus.MET;
    }
    
    @Override
    protected Map<String, Object> calculateGoalMetrics(MetricsSnapshot metrics) {
        double errorRate = (double) metrics.getTotalErrors() / Math.max(1, metrics.getRecordsProcessed());
        int errorBudgetRemaining = maxTotalErrorCount - metrics.getTotalErrors();
        
        Map<String, Object> goalMetrics = new HashMap<>();
        goalMetrics.put("totalErrors", metrics.getTotalErrors());
        goalMetrics.put("errorRate", errorRate);
        goalMetrics.put("errorBudgetRemaining", errorBudgetRemaining);
        goalMetrics.put("failedFiles", metrics.getFailedFiles());
        goalMetrics.put("hasCriticalError", metrics.hasCriticalError(criticalErrorTypes));
        
        return goalMetrics;
    }
    
    @Override
    public Severity getSeverity() {
        return Severity.HIGH;
    }
}
