package com.sephora.services.ismsearchpoc.framework.strategy;

import com.sephora.services.ismsearchpoc.framework.goal.GoalEvaluation;

/**
 * Strategy for calculating dial adjustments based on goal evaluation
 * Each goal has an associated strategy
 */
public interface GoalStrategy {
    
    /**
     * Recommend a dial adjustment based on goal evaluation
     * @param evaluation The goal evaluation result
     * @return Recommended dial adjustment (can be no change)
     */
    DialAdjustment recommendAdjustment(GoalEvaluation evaluation);
}
