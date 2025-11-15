package com.sephora.services.ismsearchpoc.ipbatch.strategy;

import com.sephora.services.ismsearchpoc.ipbatch.goal.GoalEvaluation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No-op strategy that never recommends changes
 * Use this for observable but non-adaptive mode:
 * - Goals still evaluate and track status
 * - Metrics are still collected
 * - But concurrency dials never change
 * 
 * Perfect for static data migration where you just want monitoring
 */
public class NoOpStrategy implements GoalStrategy {
    
    private static final Logger log = LoggerFactory.getLogger(NoOpStrategy.class);
    
    @Override
    public DialAdjustment recommendAdjustment(GoalEvaluation evaluation) {
        // Always return no change, but log the goal status for observability
        log.debug("NoOp strategy for goal [{}]: status={}, but no adjustment recommended",
            evaluation.getGoal().getName(), evaluation.getStatus());
        
        return DialAdjustment.noChange();
    }
}
