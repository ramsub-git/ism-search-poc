package com.sephora.services.ismsearchpoc.ipbatch.core;

import com.sephora.services.ismsearchpoc.ipbatch.strategy.ConcurrencyLimits;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.ConcurrencyStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.goal.Goal;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.RecordCounter;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingScope;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.GoalStrategy;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Getter
public class BatchDefinition {
    
    private final String batchName;
    private final String description;
    private final String processingContextName;
    
    private final SizingStrategy sizingStrategy;
    private final SizingScope sizingScope;
    private final RecordCounter batchLevelRecordCounter;
    
    private final ConcurrencyStrategy concurrencyStrategy;
    private final ConcurrencyLimits concurrencyLimits;
    
    private final List<BatchStepDefinition> steps;
    
    private final List<Goal> goals;
    private final Map<Goal, GoalStrategy> strategies;
    
    private final Runnable beforeBatchHook;
    private final Consumer<BatchResult> afterBatchHook;
    private final Consumer<BatchStepResult> beforeStepHook;
    private final Consumer<BatchStepResult> afterStepHook;
    
    BatchDefinition(BatchBuilder builder) {
        this.batchName = builder.batchName;
        this.description = builder.description;
        this.processingContextName = builder.processingContextName;
        this.sizingStrategy = builder.sizingStrategy;
        this.sizingScope = builder.sizingScope;
        this.batchLevelRecordCounter = builder.batchLevelRecordCounter;
        this.concurrencyStrategy = builder.concurrencyStrategy;
        this.concurrencyLimits = ConcurrencyLimits.builder()
            .minWorkItemConcurrency(builder.minWorkItemConcurrency)
            .maxWorkItemConcurrency(builder.maxWorkItemConcurrency)
            .minProcessingConcurrency(builder.minProcessingConcurrency)
            .maxProcessingConcurrency(builder.maxProcessingConcurrency)
            .build();
        this.steps = builder.steps.stream()
            .map(BatchStepBuilder::build)
            .toList();
        this.goals = List.copyOf(builder.goals);
        this.strategies = Map.copyOf(builder.strategies);
        this.beforeBatchHook = builder.beforeBatchHook;
        this.afterBatchHook = builder.afterBatchHook;
        this.beforeStepHook = builder.beforeStepHook;
        this.afterStepHook = builder.afterStepHook;
    }
}
