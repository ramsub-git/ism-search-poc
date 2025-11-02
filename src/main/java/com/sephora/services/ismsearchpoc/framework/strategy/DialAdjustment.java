package com.sephora.services.ismsearchpoc.framework.strategy;

/**
 * Recommended dial adjustment from a strategy
 */
public class DialAdjustment {
    
    private final int workItemConcurrencyDelta;
    private final int processingConcurrencyDelta;
    private final String reason;
    
    private DialAdjustment(int workItemConcurrencyDelta, 
                          int processingConcurrencyDelta, 
                          String reason) {
        this.workItemConcurrencyDelta = workItemConcurrencyDelta;
        this.processingConcurrencyDelta = processingConcurrencyDelta;
        this.reason = reason;
    }
    
    public static DialAdjustment noChange() {
        return new DialAdjustment(0, 0, "No adjustment needed");
    }
    
    public static DialAdjustment increase(int workItemDelta, int processingDelta, String reason) {
        return new DialAdjustment(workItemDelta, processingDelta, reason);
    }
    
    public static DialAdjustment decrease(int workItemDelta, int processingDelta, String reason) {
        return new DialAdjustment(-Math.abs(workItemDelta), -Math.abs(processingDelta), reason);
    }
    
    public static DialAdjustment custom(int workItemDelta, int processingDelta, String reason) {
        return new DialAdjustment(workItemDelta, processingDelta, reason);
    }
    
    public int getWorkItemConcurrencyDelta() {
        return workItemConcurrencyDelta;
    }
    
    public int getProcessingConcurrencyDelta() {
        return processingConcurrencyDelta;
    }
    
    public String getReason() {
        return reason;
    }
    
    public boolean isNoChange() {
        return workItemConcurrencyDelta == 0 && processingConcurrencyDelta == 0;
    }
    
    @Override
    public String toString() {
        return String.format("DialAdjustment{workItem=%+d, processing=%+d, reason='%s'}",
            workItemConcurrencyDelta, processingConcurrencyDelta, reason);
    }
}
