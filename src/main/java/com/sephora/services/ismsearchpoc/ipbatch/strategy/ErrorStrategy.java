package com.sephora.services.ismsearchpoc.ipbatch.strategy;

import com.sephora.services.ismsearchpoc.ipbatch.goal.GoalEvaluation;
import com.sephora.services.ismsearchpoc.ipbatch.goal.GoalStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Strategy for error goal
 * Decreases concurrency when error rates are high
 */
public class ErrorStrategy implements GoalStrategy {
    
    private static final Logger log = LoggerFactory.getLogger(ErrorStrategy.class);
    
    @Override
    public DialAdjustment recommendAdjustment(GoalEvaluation evaluation) {
        GoalStatus status = evaluation.getStatus();
        Map<String, Object> metrics = evaluation.getMetrics();
        
        int totalErrors = (Integer) metrics.get("totalErrors");
        double errorRate = (Double) metrics.get("errorRate");
        boolean hasCriticalError = (Boolean) metrics.get("hasCriticalError");
        
        if (status == GoalStatus.VIOLATED || hasCriticalError) {
            // Error threshold exceeded or critical error detected
            if (hasCriticalError) {
                log.error("Critical error detected - recommending shutdown");
                // Return large decrease to signal near-abort
                return DialAdjustment.decrease(20, 20, "Critical error detected");
            }
            
            // Too many errors - aggressive decrease
            int decrease = calculateAggressiveDecrease(errorRate);
            log.warn("Error VIOLATED - recommending aggressive decrease: -{}", decrease);
            return DialAdjustment.decrease(decrease, decrease,
                String.format("Error threshold exceeded (rate: %.4f, total: %d)", 
                    errorRate, totalErrors));
        }
        
        if (status == GoalStatus.AT_RISK) {
            // Error rate climbing - moderate decrease
            int decrease = calculateModerateDecrease(errorRate);
            log.info("Error AT_RISK - recommending moderate decrease: -{}", decrease);
            return DialAdjustment.decrease(decrease, decrease,
                String.format("Error rate increasing (rate: %.4f, total: %d)", 
                    errorRate, totalErrors));
        }
        
        // Error rate is acceptable - no change
        return DialAdjustment.noChange();
    }
    
    private int calculateAggressiveDecrease(double errorRate) {
        if (errorRate > 0.10) { // More than 10% error rate
            return 8;
        } else if (errorRate > 0.07) {
            return 5;
        } else {
            return 3;
        }
    }
    
    private int calculateModerateDecrease(double errorRate) {
        if (errorRate > 0.05) {
            return 3;
        } else {
            return 2;
        }
    }
}
