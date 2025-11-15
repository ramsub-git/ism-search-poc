package com.sephora.services.ismsearchpoc.ipbatch.core;

import com.sephora.services.ismsearchpoc.ipbatch.sizing.RecordCounter;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.worker.BatchProcessor;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemFetcher;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemReader;
import lombok.Getter;

@Getter
public class BatchStepDefinition {
    
    private final String stepName;
    private final WorkItemFetcher<?> workItemFetcher;
    private final WorkItemReader<?, ?> workItemReader;
    private final BatchProcessor<?, ?> batchProcessor;
    private final String stepGuardName;
    
    private final SizingStrategy stepSizingStrategy;
    private final RecordCounter stepRecordCounter;
    private final Long estimatedRecordsPerItem;
    
    BatchStepDefinition(BatchStepBuilder builder) {
        this.stepName = builder.stepName;
        this.workItemFetcher = builder.workItemFetcher;
        this.workItemReader = builder.workItemReader;
        this.batchProcessor = builder.batchProcessor;
        this.stepGuardName = builder.stepGuardName;
        this.stepSizingStrategy = builder.stepSizingStrategy;
        this.stepRecordCounter = builder.stepRecordCounter;
        this.estimatedRecordsPerItem = builder.estimatedRecordsPerItem;
    }
    
    public boolean hasStepLevelSizing() {
        return stepSizingStrategy != null;
    }
}
