package com.sephora.services.ismsearchpoc.framework.strategy;

import com.sephora.services.ismsearchpoc.framework.goal.GoalEvaluation;
import com.sephora.services.ismsearchpoc.framework.goal.GoalStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Strategy for performance goal
 * Increases concurrency when falling behind, decreases when ahead
 */
public class PerformanceStrategy implements GoalStrategy {
    
    private static final Logger log = LoggerFactory.getLogger(PerformanceStrategy.class);
    
    @Override
    public DialAdjustment recommendAdjustment(GoalEvaluation evaluation) {
        GoalStatus status = evaluation.getStatus();
        Map<String, Object> metrics = evaluation.getMetrics();
        
        double rateGap = (Double) metrics.get("rateGap");
        double percentComplete = (Double) metrics.get("percentComplete");
        
        if (status == GoalStatus.VIOLATED) {
            // Falling significantly behind - aggressive increase
            int increase = calculateAggressiveIncrease(rateGap, percentComplete);
            log.info("Performance VIOLATED - recommending aggressive increase: +{}", increase);
            return DialAdjustment.increase(increase, increase, 
                String.format("Falling behind pace (gap: %.2f files/min)", rateGap));
        }
        
        if (status == GoalStatus.AT_RISK) {
            // Trending behind - moderate increase
            int increase = calculateModerateIncrease(rateGap);
            log.info("Performance AT_RISK - recommending moderate increase: +{}", increase);
            return DialAdjustment.increase(increase, increase,
                String.format("At risk of falling behind (gap: %.2f files/min)", rateGap));
        }
        
        if (status == GoalStatus.MET && percentComplete < 80) {
            // Ahead of pace and still early - could increase slightly for buffer
            if (rateGap < -5) { // Significantly ahead
                log.debug("Performance MET and ahead of pace - could increase for buffer");
                return DialAdjustment.increase(2, 1, "Building buffer while ahead");
            }
        }
        
        // Performance is good - no change
        return DialAdjustment.noChange();
    }
    
    private int calculateAggressiveIncrease(double rateGap, double percentComplete) {
        // More aggressive early in the run when we have time to catch up
        if (percentComplete < 25) {
            return Math.min(10, (int) Math.ceil(rateGap / 2));
        } else if (percentComplete < 50) {
            return Math.min(8, (int) Math.ceil(rateGap / 2));
        } else {
            return Math.min(5, (int) Math.ceil(rateGap / 3));
        }
    }
    
    private int calculateModerateIncrease(double rateGap) {
        return Math.min(5, (int) Math.ceil(rateGap / 3));
    }
}
