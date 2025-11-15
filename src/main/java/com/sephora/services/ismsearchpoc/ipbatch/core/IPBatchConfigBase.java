package com.sephora.services.ismsearchpoc.ipbatch.core;

import jakarta.annotation.PostConstruct;

public abstract class IPBatchConfigBase {
    
    @PostConstruct
    public void initialize() {
        configureBatches();
    }
    
    protected abstract void configureBatches();
}
