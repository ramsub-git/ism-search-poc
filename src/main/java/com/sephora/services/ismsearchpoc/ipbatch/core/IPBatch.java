package com.sephora.services.ismsearchpoc.ipbatch.core;

public class IPBatch {
    
    public static BatchBuilder createBatch(String batchName) {
        return new BatchBuilder(batchName);
    }
}
