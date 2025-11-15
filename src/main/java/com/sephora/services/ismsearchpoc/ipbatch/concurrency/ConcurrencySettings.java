package com.sephora.services.ismsearchpoc.ipbatch.concurrency;

/**
 * Current concurrency settings
 */
public class ConcurrencySettings {
    
    private final int workItemConcurrency;
    private final int processingConcurrency;
    private final String implementationType;
    
    private ConcurrencySettings(Builder builder) {
        this.workItemConcurrency = builder.workItemConcurrency;
        this.processingConcurrency = builder.processingConcurrency;
        this.implementationType = builder.implementationType;
    }
    
    public int getWorkItemConcurrency() {
        return workItemConcurrency;
    }
    
    public int getProcessingConcurrency() {
        return processingConcurrency;
    }
    
    public String getImplementationType() {
        return implementationType;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private int workItemConcurrency;
        private int processingConcurrency;
        private String implementationType;
        
        public Builder workItemConcurrency(int workItemConcurrency) {
            this.workItemConcurrency = workItemConcurrency;
            return this;
        }
        
        public Builder processingConcurrency(int processingConcurrency) {
            this.processingConcurrency = processingConcurrency;
            return this;
        }
        
        public Builder implementationType(String implementationType) {
            this.implementationType = implementationType;
            return this;
        }
        
        public ConcurrencySettings build() {
            return new ConcurrencySettings(this);
        }
    }
}
