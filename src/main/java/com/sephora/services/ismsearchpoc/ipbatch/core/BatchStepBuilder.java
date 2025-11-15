package com.sephora.services.ismsearchpoc.ipbatch.core;

import com.sephora.services.ismsearchpoc.ipbatch.goal.Goal;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.RecordCounter;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.GoalStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.worker.BatchProcessor;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemFetcher;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemReader;

import java.util.function.Consumer;

public class BatchStepBuilder {
    
    private final BatchBuilder parent;
    final String stepName;
    
    WorkItemFetcher<?> workItemFetcher;
    WorkItemReader<?, ?> workItemReader;
    BatchProcessor<?, ?> batchProcessor;
    
    String stepGuardName;
    
    SizingStrategy stepSizingStrategy;
    RecordCounter stepRecordCounter;
    Long estimatedRecordsPerItem;
    
    BatchStepBuilder(BatchBuilder parent, String stepName) {
        this.parent = parent;
        this.stepName = stepName;
    }
    
    public <T> BatchStepBuilder workItemFetcher(WorkItemFetcher<T> fetcher) {
        this.workItemFetcher = fetcher;
        return this;
    }
    
    public <T, R> BatchStepBuilder workItemReader(WorkItemReader<T, R> reader) {
        this.workItemReader = reader;
        return this;
    }
    
    public <R, V> BatchStepBuilder batchProcessor(BatchProcessor<R, V> processor) {
        this.batchProcessor = processor;
        return this;
    }
    
    public BatchStepBuilder withStepGuard(String stepGuardName) {
        this.stepGuardName = stepGuardName;
        return this;
    }
    
    public BatchStepBuilder withStepSizingStrategy(SizingStrategy strategy) {
        this.stepSizingStrategy = strategy;
        return this;
    }
    
    public BatchStepBuilder estimatedRecordsPerItem(long estimate) {
        this.estimatedRecordsPerItem = estimate;
        return this;
    }
    
    public BatchStepBuilder withRecordCounter(RecordCounter counter) {
        this.stepRecordCounter = counter;
        return this;
    }
    
    public BatchStepBuilder addStep(String nextStepName) {
        return parent.addStep(nextStepName);
    }
    
    public BatchBuilder withGoal(Goal goal, GoalStrategy strategy) {
        return parent.withGoal(goal, strategy);
    }
    
    public BatchBuilder beforeBatch(Runnable hook) {
        return parent.beforeBatch(hook);
    }
    
    public BatchBuilder afterBatch(Consumer<BatchResult> hook) {
        return parent.afterBatch(hook);
    }
    
    public void register() {
        parent.register();
    }



    public BatchStepDefinition build() {
        validate();
        return new BatchStepDefinition(this);
    }

    private void validateBatch() {
        parent.validate();  // Make parent.validate() protected instead of private
    }

    public BatchBuilder done() {
        return parent;
    }

    // OR just add a done().build() wrapper:
    public BatchDefinition buildBatch() {
        return done().build();
    }
    
    private void validate() {
        if (workItemFetcher == null) {
            throw new IllegalStateException("Step must have a WorkItemFetcher");
        }
        if (workItemReader == null) {
            throw new IllegalStateException("Step must have a WorkItemReader");
        }
        if (batchProcessor == null) {
            throw new IllegalStateException("Step must have a BatchProcessor");
        }
    }
}
