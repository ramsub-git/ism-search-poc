package com.sephora.services.ismsearchpoc.ipbatch.core;

import com.sephora.services.ismsearchpoc.ipbatch.strategy.ConcurrencyStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.WorkloadAwareConcurrencyStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.goal.Goal;
import com.sephora.services.ismsearchpoc.ipbatch.goal.ErrorGoal;
import com.sephora.services.ismsearchpoc.ipbatch.goal.PerformanceGoal;
import com.sephora.services.ismsearchpoc.ipbatch.goal.ResourceGoal;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.RecordCounter;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingScope;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.GoalStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.ErrorStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.PerformanceStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.ResourceStrategy;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

@Slf4j
public class BatchBuilder {
    
    final String batchName;
    String description;
    String processingContextName;
    
    SizingStrategy sizingStrategy = SizingStrategy.STATIC;
    SizingScope sizingScope = SizingScope.BATCH_LEVEL;
    RecordCounter batchLevelRecordCounter;
    
    ConcurrencyStrategy concurrencyStrategy = WorkloadAwareConcurrencyStrategy.builder().build();
    int minWorkItemConcurrency = 5;
    int maxWorkItemConcurrency = 30;
    int minProcessingConcurrency = 3;
    int maxProcessingConcurrency = 20;
    
    final List<BatchStepBuilder> steps = new ArrayList<>();
    
    final List<Goal> goals = new ArrayList<>();
    final Map<Goal, GoalStrategy> strategies = new HashMap<>();
    
    Runnable beforeBatchHook;
    Consumer<BatchResult> afterBatchHook;
    Consumer<BatchStepResult> beforeStepHook;
    Consumer<BatchStepResult> afterStepHook;
    
    BatchBuilder(String batchName) {
        this.batchName = batchName;
    }
    
    public BatchBuilder withDescription(String description) {
        this.description = description;
        return this;
    }
    
    public BatchBuilder withProcessingContext(String contextName) {
        this.processingContextName = contextName;
        return this;
    }
    
    public BatchBuilder withSizingStrategy(SizingStrategy strategy) {
        this.sizingStrategy = strategy;
        return this;
    }
    
    public BatchBuilder withSizingScope(SizingScope scope) {
        this.sizingScope = scope;
        return this;
    }
    
    public BatchBuilder withRecordCounter(RecordCounter counter) {
        this.batchLevelRecordCounter = counter;
        return this;
    }
    
    public BatchBuilder withConcurrencyStrategy(ConcurrencyStrategy strategy) {
        this.concurrencyStrategy = strategy;
        return this;
    }
    
    public BatchBuilder withConcurrencyLimits(
            int minWorkItem, int maxWorkItem,
            int minProcessing, int maxProcessing) {
        this.minWorkItemConcurrency = minWorkItem;
        this.maxWorkItemConcurrency = maxWorkItem;
        this.minProcessingConcurrency = minProcessing;
        this.maxProcessingConcurrency = maxProcessing;
        return this;
    }
    
    public BatchStepBuilder addStep(String stepName) {
        BatchStepBuilder stepBuilder = new BatchStepBuilder(this, stepName);
        steps.add(stepBuilder);
        return stepBuilder;
    }
    
    public BatchBuilder withGoal(Goal goal, GoalStrategy strategy) {
        goals.add(goal);
        strategies.put(goal, strategy);
        return this;
    }
    
    public BatchBuilder withPerformanceGoal(Duration maxTime, double minThroughput) {
        Goal goal = new PerformanceGoal(maxTime, minThroughput, 0.8);
        return withGoal(goal, new PerformanceStrategy());
    }
    
    public BatchBuilder withResourceGoal(int maxDbConn, double maxDbUtil, double maxHeap) {
        Goal goal = new ResourceGoal(maxDbConn, maxDbUtil, maxHeap);
        return withGoal(goal, new ResourceStrategy());
    }
    
    public BatchBuilder withErrorGoal(double maxErrorRate, int maxTotalErrors) {
        Goal goal = new ErrorGoal(maxErrorRate, maxTotalErrors, Set.of());
        return withGoal(goal, new ErrorStrategy());
    }
    
    public BatchBuilder beforeBatch(Runnable hook) {
        this.beforeBatchHook = hook;
        return this;
    }
    
    public BatchBuilder afterBatch(Consumer<BatchResult> hook) {
        this.afterBatchHook = hook;
        return this;
    }
    
    public BatchBuilder beforeStep(Consumer<BatchStepResult> hook) {
        this.beforeStepHook = hook;
        return this;
    }
    
    public BatchBuilder afterStep(Consumer<BatchStepResult> hook) {
        this.afterStepHook = hook;
        return this;
    }
    
    public BatchDefinition build() {
        validate();
        return new BatchDefinition(this);
    }
    
    public void register() {
        BatchDefinition definition = build();
        BatchRegistry.register(definition);
        log.info("Registered batch: {}", batchName);
    }
    
    private void validate() {
        if (steps.isEmpty()) {
            throw new IllegalStateException("Batch must have at least one step");
        }
        if (sizingStrategy == SizingStrategy.DYNAMIC 
                && sizingScope == SizingScope.BATCH_LEVEL 
                && batchLevelRecordCounter == null) {
            throw new IllegalStateException(
                "DYNAMIC sizing at BATCH_LEVEL requires a RecordCounter");
        }
    }
}
