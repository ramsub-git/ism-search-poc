package com.sephora.services.ismsearchpoc.ipbatch.runtime;

import com.sephora.services.ismsearchpoc.ipbatch.strategy.InitialConcurrency;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.ResourceSnapshot;
import com.sephora.services.ismsearchpoc.ipbatch.core.*;
import com.sephora.services.ismsearchpoc.ipbatch.engine.ParallelBatchEngine;
import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionResult;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Slf4j
@Service
public class BatchPipelineExecutor {

    private final WorkloadAnalyzer workloadAnalyzer;
    private final ResourceMonitor resourceMonitor;
    private final BatchEngineFactory engineFactory;

    public BatchPipelineExecutor(
            WorkloadAnalyzer workloadAnalyzer,
            ResourceMonitor resourceMonitor,
            BatchEngineFactory engineFactory) {
        this.workloadAnalyzer = workloadAnalyzer;
        this.resourceMonitor = resourceMonitor;
        this.engineFactory = engineFactory;
    }

    public BatchResult execute(BatchDefinition batch, ExecutionContext context) {

        log.info("Starting batch: {}", batch.getBatchName());

        if (batch.getBeforeBatchHook() != null) {
            batch.getBeforeBatchHook().run();
        }

        // FIX 1: Use var or BatchResult.BatchResultBuilder
        var resultBuilder = BatchResult.builder()
                .batchName(batch.getBatchName())
                .startTime(Instant.now());

        try {
            InitialConcurrency batchConcurrency = null;
            if (batch.getSizingScope() == SizingScope.BATCH_LEVEL) {
                batchConcurrency = analyzeBatchWorkload(batch, context);
                log.info("Batch-level sizing: {}", batchConcurrency.getRationale());
            }

            for (BatchStepDefinition step : batch.getSteps()) {

                log.info("Executing step: {}", step.getStepName());

                InitialConcurrency stepConcurrency =
                        (batch.getSizingScope() == SizingScope.STEP_LEVEL
                                || step.hasStepLevelSizing())
                                ? analyzeStepWorkload(batch, step, context)
                                : batchConcurrency;

                if (stepConcurrency != null) {
                    log.info("Step sizing: {}", stepConcurrency.getRationale());
                }

                BatchStepResult stepResult = executeStep(
                        batch, step, stepConcurrency, context);

                // FIX 2: Use stepResult (singular) because of @Singular annotation
                resultBuilder.stepResult(stepResult);

                if (!stepResult.isSuccess() && stepResult.shouldAbort()) {
                    log.error("Step failed, aborting batch: {}", step.getStepName());
                    resultBuilder.aborted(true)
                            .abortReason("Step failed: " + step.getStepName());
                    break;
                }
            }

            resultBuilder.success(true);

        } catch (Exception e) {
            log.error("Batch execution failed", e);
            resultBuilder.success(false)
                    .aborted(true)
                    .abortReason("Exception: " + e.getMessage());
        } finally {
            resultBuilder.endTime(Instant.now());
        }

        BatchResult result = resultBuilder.build();

        if (batch.getAfterBatchHook() != null) {
            batch.getAfterBatchHook().accept(result);
        }

        return result;
    }

    private InitialConcurrency analyzeBatchWorkload(
            BatchDefinition batch, ExecutionContext context) {

        if (batch.getSizingStrategy() == SizingStrategy.STATIC) {
            return InitialConcurrency.builder()
                    .workItemConcurrency(
                            batch.getConcurrencyLimits().getMinWorkItemConcurrency())
                    .processingConcurrency(
                            batch.getConcurrencyLimits().getMinProcessingConcurrency())
                    .rationale("STATIC sizing - using configured minimums")
                    .build();
        }

        try {
            BatchStepDefinition firstStep = batch.getSteps().get(0);
            List<?> workItems = firstStep.getWorkItemFetcher().fetchWorkItems(context);

            WorkloadAnalysis analysis = workloadAnalyzer.analyze(
                    batch.getSizingStrategy(),
                    workItems.size(),
                    firstStep.getEstimatedRecordsPerItem(),
                    batch.getBatchLevelRecordCounter(),
                    context
            );

            ResourceSnapshot resources = resourceMonitor.snapshot();

            return batch.getConcurrencyStrategy().calculate(
                    analysis,
                    batch.getConcurrencyLimits(),
                    resources
            );

        } catch (Exception e) {
            log.error("Failed to analyze batch workload, using conservative defaults", e);
            // Fall back to minimum concurrency on error
            return InitialConcurrency.builder()
                    .workItemConcurrency(
                            batch.getConcurrencyLimits().getMinWorkItemConcurrency())
                    .processingConcurrency(
                            batch.getConcurrencyLimits().getMinProcessingConcurrency())
                    .rationale("Error during workload analysis - using minimum concurrency")
                    .build();
        }
    }

    private InitialConcurrency analyzeStepWorkload(
            BatchDefinition batch,
            BatchStepDefinition step,
            ExecutionContext context) {

        SizingStrategy strategy = step.hasStepLevelSizing()
                ? step.getStepSizingStrategy()
                : batch.getSizingStrategy();

        if (strategy == SizingStrategy.STATIC) {
            return InitialConcurrency.builder()
                    .workItemConcurrency(
                            batch.getConcurrencyLimits().getMinWorkItemConcurrency())
                    .processingConcurrency(
                            batch.getConcurrencyLimits().getMinProcessingConcurrency())
                    .rationale("STATIC sizing - using configured minimums")
                    .build();
        }

        try {
            List<?> workItems = step.getWorkItemFetcher().fetchWorkItems(context);

            RecordCounter counter = step.hasStepLevelSizing()
                    ? step.getStepRecordCounter()
                    : batch.getBatchLevelRecordCounter();

            WorkloadAnalysis analysis = workloadAnalyzer.analyze(
                    strategy,
                    workItems.size(),
                    step.getEstimatedRecordsPerItem(),
                    counter,
                    context
            );

            ResourceSnapshot resources = resourceMonitor.snapshot();

            return batch.getConcurrencyStrategy().calculate(
                    analysis,
                    batch.getConcurrencyLimits(),
                    resources
            );

        } catch (Exception e) {
            log.error("Failed to analyze step workload for {}, using conservative defaults",
                    step.getStepName(), e);
            // Fall back to minimum concurrency on error
            return InitialConcurrency.builder()
                    .workItemConcurrency(
                            batch.getConcurrencyLimits().getMinWorkItemConcurrency())
                    .processingConcurrency(
                            batch.getConcurrencyLimits().getMinProcessingConcurrency())
                    .rationale("Error during workload analysis - using minimum concurrency")
                    .build();
        }
    }

    private BatchStepResult executeStep(
            BatchDefinition batch,
            BatchStepDefinition step,
            InitialConcurrency concurrency,
            ExecutionContext context) {

        ParallelBatchEngine<?, ?, ?> engine = engineFactory.createEngine(
                batch, step, concurrency);

        if (batch.getBeforeStepHook() != null) {
            BatchStepResult prelimResult = BatchStepResult.builder()
                    .stepName(step.getStepName())
                    .startTime(Instant.now())
                    .build();
            batch.getBeforeStepHook().accept(prelimResult);
        }

        Instant stepStart = Instant.now();

        ExecutionResult execResult = engine.execute(context);

        BatchStepResult stepResult = BatchStepResult.builder()
                .stepName(step.getStepName())
                .success(execResult.isSuccess())
                .startTime(stepStart)
                .endTime(Instant.now())
                .itemsProcessed(execResult.getWorkItemsProcessed())
                .recordsProcessed(execResult.getRecordsProcessed())
                .errors(execResult.getTotalErrors())
                .abortReason(execResult.getAbortReason())
                .initialConcurrency(concurrency)
                .build();

        if (batch.getAfterStepHook() != null) {
            batch.getAfterStepHook().accept(stepResult);
        }

        return stepResult;
    }
}