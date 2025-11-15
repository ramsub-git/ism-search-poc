package com.sephora.services.ismsearchpoc.ipbatch.strategy;

import com.sephora.services.ismsearchpoc.ipbatch.goal.GoalEvaluation;
import com.sephora.services.ismsearchpoc.ipbatch.goal.GoalStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Strategy for resource goal
 * Decreases concurrency when resources are constrained
 */
public class ResourceStrategy implements GoalStrategy {
    
    private static final Logger log = LoggerFactory.getLogger(ResourceStrategy.class);
    
    @Override
    public DialAdjustment recommendAdjustment(GoalEvaluation evaluation) {
        GoalStatus status = evaluation.getStatus();
        Map<String, Object> metrics = evaluation.getMetrics();
        
        double dbUtilization = (Double) metrics.get("dbUtilizationPercent");
        double heapUtilization = (Double) metrics.get("heapUtilizationPercent");
        boolean connectionPressure = (Boolean) metrics.get("connectionPressure");
        
        if (status == GoalStatus.VIOLATED) {
            // Resource limits exceeded - aggressive decrease
            int decrease = calculateAggressiveDecrease(dbUtilization, heapUtilization);
            log.warn("Resource VIOLATED - recommending aggressive decrease: -{}", decrease);
            return DialAdjustment.decrease(decrease, decrease,
                String.format("Resource limits exceeded (DB: %.1f%%, Heap: %.1f%%)", 
                    dbUtilization, heapUtilization));
        }
        
        if (status == GoalStatus.AT_RISK || connectionPressure) {
            // Resources getting tight - moderate decrease
            int decrease = calculateModerateDecrease(dbUtilization, heapUtilization);
            log.info("Resource AT_RISK - recommending moderate decrease: -{}", decrease);
            return DialAdjustment.decrease(decrease, decrease,
                String.format("Resource pressure (DB: %.1f%%, Heap: %.1f%%)", 
                    dbUtilization, heapUtilization));
        }
        
        // Resources are healthy - no change
        return DialAdjustment.noChange();
    }
    
    private int calculateAggressiveDecrease(double dbUtilization, double heapUtilization) {
        // More aggressive decrease if either resource is severely constrained
        double maxUtilization = Math.max(dbUtilization, heapUtilization);
        
        if (maxUtilization > 95) {
            return 8; // Critical - drop fast
        } else if (maxUtilization > 90) {
            return 5;
        } else {
            return 3;
        }
    }
    
    private int calculateModerateDecrease(double dbUtilization, double heapUtilization) {
        double maxUtilization = Math.max(dbUtilization, heapUtilization);
        
        if (maxUtilization > 88) {
            return 3;
        } else {
            return 2;
        }
    }
}
