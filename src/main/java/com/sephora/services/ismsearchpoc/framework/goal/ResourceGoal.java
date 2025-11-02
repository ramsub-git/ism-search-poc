package com.sephora.services.ismsearchpoc.framework.goal;

import com.sephora.services.ismsearchpoc.framework.metrics.MetricsSnapshot;

import java.util.HashMap;
import java.util.Map;

/**
 * Goal for staying within resource limits
 */
public class ResourceGoal extends Goal {
    
    private final int maxDbConnections;
    private final double maxDbUtilization;
    private final double maxHeapUtilization;
    
    public ResourceGoal(int maxDbConnections, 
                       double maxDbUtilization, 
                       double maxHeapUtilization) {
        super("Resource");
        this.maxDbConnections = maxDbConnections;
        this.maxDbUtilization = maxDbUtilization;
        this.maxHeapUtilization = maxHeapUtilization;
    }
    
    @Override
    protected GoalStatus evaluate(MetricsSnapshot metrics) {
        double dbUtilization = (double) metrics.getActiveDbConnections() / maxDbConnections;
        
        if (dbUtilization > maxDbUtilization || metrics.getHeapUtilization() > maxHeapUtilization) {
            return GoalStatus.VIOLATED;
        }
        
        if (dbUtilization > maxDbUtilization * 0.85 || metrics.getHeapUtilization() > maxHeapUtilization * 0.85) {
            return GoalStatus.AT_RISK;
        }
        
        return GoalStatus.MET;
    }
    
    @Override
    protected Map<String, Object> calculateGoalMetrics(MetricsSnapshot metrics) {
        double dbUtilization = (double) metrics.getActiveDbConnections() / maxDbConnections;
        int availableConnections = maxDbConnections - metrics.getActiveDbConnections();
        int safeMaxConnections = (int)(maxDbConnections * maxDbUtilization);
        
        Map<String, Object> goalMetrics = new HashMap<>();
        goalMetrics.put("dbUtilizationPercent", dbUtilization * 100);
        goalMetrics.put("activeConnections", metrics.getActiveDbConnections());
        goalMetrics.put("availableConnections", availableConnections);
        goalMetrics.put("safeMaxConnections", safeMaxConnections);
        goalMetrics.put("heapUtilizationPercent", metrics.getHeapUtilization() * 100);
        goalMetrics.put("connectionPressure", dbUtilization > maxDbUtilization * 0.85);
        
        return goalMetrics;
    }
    
    @Override
    public Severity getSeverity() {
        return Severity.HIGH;
    }
}
