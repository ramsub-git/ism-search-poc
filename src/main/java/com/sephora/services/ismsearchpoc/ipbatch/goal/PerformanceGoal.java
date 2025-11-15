package com.sephora.services.ismsearchpoc.ipbatch.goal;

import com.sephora.services.ismsearchpoc.ipbatch.metrics.MetricsSnapshot;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Goal for meeting performance/time requirements
 */
public class PerformanceGoal extends Goal {
    
    private final Duration maxTotalTime;
    private final double minFilesPerMinute;
    private final double paceToleranceFactor;
    private final Instant startTime;
    
    public PerformanceGoal(Duration maxTotalTime, 
                          double minFilesPerMinute, 
                          double paceToleranceFactor) {
        super("Performance");
        this.maxTotalTime = maxTotalTime;
        this.minFilesPerMinute = minFilesPerMinute;
        this.paceToleranceFactor = paceToleranceFactor;
        this.startTime = Instant.now();
    }
    
    @Override
    protected GoalStatus evaluate(MetricsSnapshot metrics) {
        Duration elapsed = Duration.between(startTime, Instant.now());
        Duration remaining = maxTotalTime.minus(elapsed);
        
        if (remaining.isNegative()) {
            return GoalStatus.VIOLATED; // Time's up
        }
        
        int filesRemaining = metrics.getTotalFiles() - metrics.getFilesProcessed();
        double requiredRate = filesRemaining > 0 ? (double) filesRemaining / remaining.toMinutes() : 0;
        double currentRate = metrics.getFilesPerMinute();
        
        if (currentRate >= requiredRate * paceToleranceFactor) {
            return GoalStatus.MET;
        } else if (currentRate >= requiredRate * 0.5) {
            return GoalStatus.AT_RISK;
        } else {
            return GoalStatus.VIOLATED;
        }
    }
    
    @Override
    protected Map<String, Object> calculateGoalMetrics(MetricsSnapshot metrics) {
        Duration elapsed = Duration.between(startTime, Instant.now());
        Duration remaining = maxTotalTime.minus(elapsed);
        
        int filesRemaining = metrics.getTotalFiles() - metrics.getFilesProcessed();
        double requiredRate = filesRemaining > 0 ? (double) filesRemaining / remaining.toMinutes() : 0;
        double currentRate = metrics.getFilesPerMinute();
        double gap = requiredRate - currentRate;
        
        Map<String, Object> goalMetrics = new HashMap<>();
        goalMetrics.put("requiredFilesPerMinute", requiredRate);
        goalMetrics.put("currentFilesPerMinute", currentRate);
        goalMetrics.put("rateGap", gap);
        goalMetrics.put("filesRemaining", filesRemaining);
        goalMetrics.put("timeRemainingMinutes", remaining.toMinutes());
        goalMetrics.put("percentComplete", metrics.getPercentComplete());
        
        return goalMetrics;
    }
    
    @Override
    public Severity getSeverity() {
        return Severity.CRITICAL; // Missing cutover deadline is critical
    }
}
