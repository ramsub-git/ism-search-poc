package com.sephora.services.ismsearchpoc.ipbatch.goal;

/**
 * Status of a goal evaluation
 */
public enum GoalStatus {
    /**
     * Goal has not been evaluated yet
     */
    NOT_STARTED,
    
    /**
     * Goal is being met successfully
     */
    MET,
    
    /**
     * Goal is trending toward violation
     */
    AT_RISK,
    
    /**
     * Goal has been violated
     */
    VIOLATED
}
