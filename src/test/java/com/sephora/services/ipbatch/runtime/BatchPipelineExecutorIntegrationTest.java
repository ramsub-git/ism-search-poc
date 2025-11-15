package com.sephora.services.ipbatch.runtime;

import com.sephora.services.ismsearchpoc.ipbatch.runtime.BatchEngineFactory;
import com.sephora.services.ismsearchpoc.ipbatch.runtime.BatchPipelineExecutor;
import com.sephora.services.ismsearchpoc.ipbatch.runtime.ResourceMonitor;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.WorkloadAwareConcurrencyStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.core.*;
import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.ipbatch.model.ProcessingResult;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingScope;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadAnalyzer;
import com.sephora.services.ismsearchpoc.ipbatch.worker.BatchProcessor;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemFetcher;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemReader;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

class BatchPipelineExecutorIntegrationTest {

    private BatchPipelineExecutor executor;
    private HikariDataSource dataSource;

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(config);

        WorkloadAnalyzer workloadAnalyzer = new WorkloadAnalyzer();
        ResourceMonitor resourceMonitor = new ResourceMonitor(dataSource);
        BatchEngineFactory engineFactory = new BatchEngineFactory();

        executor = new BatchPipelineExecutor(workloadAnalyzer, resourceMonitor, engineFactory);

        BatchRegistry.clear();
    }

    @Test
    void shouldExecuteSingleStepBatch() {
        List<String> processedItems = new ArrayList<>();

        WorkItemFetcher<String> fetcher = ctx -> List.of("item1", "item2", "item3");
        WorkItemReader<String, String> reader = (item, ctx) -> List.of(item + "_record");
        BatchProcessor<String, Void> processor = (records, ctx) -> {
            processedItems.addAll(records);
            return List.of(ProcessingResult.success(null));        };

        BatchDefinition batch = IPBatch.createBatch("SINGLE_STEP")
                .withDescription("Single step test")
                .addStep("PROCESS")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(processor)
                .done()
                .build();

        ExecutionContext context = new ExecutionContext();
        BatchResult result = executor.execute(batch, context);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.isAborted()).isFalse();
        assertThat(result.getStepResults()).hasSize(1);
        assertThat(processedItems).containsExactly("item1_record", "item2_record", "item3_record");
    }

    @Test
    void shouldExecuteMultiStepBatch() {
        List<String> step1Processed = new ArrayList<>();
        List<String> step2Processed = new ArrayList<>();

        WorkItemFetcher<String> fetcher = ctx -> List.of("A", "B");
        WorkItemReader<String, String> reader = (item, ctx) -> List.of(item + "1", item + "2");

        BatchProcessor<String, Void> processor1 = (records, ctx) -> {
            step1Processed.addAll(records);
            return List.of(ProcessingResult.success(null));        };

        BatchProcessor<String, Void> processor2 = (records, ctx) -> {
            step2Processed.addAll(records);
            return List.of(ProcessingResult.success(null));        };

        BatchDefinition batch = IPBatch.createBatch("MULTI_STEP")
                .addStep("STEP1")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(processor1)
                .addStep("STEP2")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(processor2)
                .done()
                .build();

        ExecutionContext context = new ExecutionContext();
        BatchResult result = executor.execute(batch, context);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getStepResults()).hasSize(2);
        assertThat(step1Processed).hasSize(4);
        assertThat(step2Processed).hasSize(4);
    }

    @Test
    void shouldExecuteHooksInOrder() {
        List<String> hookOrder = new ArrayList<>();

        WorkItemFetcher<String> fetcher = ctx -> List.of("item1");
        WorkItemReader<String, String> reader = (item, ctx) -> List.of("record1");
        BatchProcessor<String, Void> processor = (records, ctx) ->
                List.of(ProcessingResult.success(null));

        BatchDefinition batch = IPBatch.createBatch("HOOKS_BATCH")
                .beforeBatch(() -> hookOrder.add("beforeBatch"))
                .beforeStep(step -> hookOrder.add("beforeStep:" + step.getStepName()))
                .afterStep(step -> hookOrder.add("afterStep:" + step.getStepName()))
                .afterBatch(result -> hookOrder.add("afterBatch"))
                .addStep("STEP1")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(processor)
                .done()
                .build();

        ExecutionContext context = new ExecutionContext();
        executor.execute(batch, context);

        assertThat(hookOrder).containsExactly(
                "beforeBatch",
                "beforeStep:STEP1",
                "afterStep:STEP1",
                "afterBatch"
        );
    }

    @Test
    void shouldUseDynamicSizingAtBatchLevel() {
        AtomicInteger counterCalls = new AtomicInteger(0);

        WorkItemFetcher<String> fetcher = ctx -> List.of("A", "B", "C");
        WorkItemReader<String, String> reader = (item, ctx) -> List.of(item);
        BatchProcessor<String, Void> processor = (records, ctx) ->
                List.of(ProcessingResult.success(null));

        BatchDefinition batch = IPBatch.createBatch("DYNAMIC_SIZING")
                .withSizingStrategy(SizingStrategy.DYNAMIC)
                .withSizingScope(SizingScope.BATCH_LEVEL)
                .withRecordCounter(ctx -> {
                    counterCalls.incrementAndGet();
                    return 1000L;
                })
                .withConcurrencyStrategy(WorkloadAwareConcurrencyStrategy.builder().build())
                .addStep("STEP1")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(processor)
                .estimatedRecordsPerItem(100L)
                .addStep("STEP2")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(processor)
                .estimatedRecordsPerItem(100L)
                .done()
                .build();

        ExecutionContext context = new ExecutionContext();
        executor.execute(batch, context);

        assertThat(counterCalls.get()).isEqualTo(1);
    }

    @Test
    void shouldUseDynamicSizingAtStepLevel() {
        AtomicInteger counterCalls = new AtomicInteger(0);

        WorkItemFetcher<String> fetcher = ctx -> List.of("A", "B");
        WorkItemReader<String, String> reader = (item, ctx) -> List.of(item);
        BatchProcessor<String, Void> processor = (records, ctx) ->
                List.of(ProcessingResult.success(null));

        BatchDefinition batch = IPBatch.createBatch("STEP_LEVEL_SIZING")
                .withSizingStrategy(SizingStrategy.DYNAMIC)
                .withSizingScope(SizingScope.STEP_LEVEL)
                .withRecordCounter(ctx -> {
                    counterCalls.incrementAndGet();
                    return 500L;
                })
                .addStep("STEP1")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(processor)
                .addStep("STEP2")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(processor)
                .done()
                .build();

        ExecutionContext context = new ExecutionContext();
        executor.execute(batch, context);

        assertThat(counterCalls.get()).isEqualTo(2);
    }

    @Test
    void shouldAbortOnStepFailure() {
        List<String> executedSteps = new ArrayList<>();

        WorkItemFetcher<String> fetcher = ctx -> List.of("item1");
        WorkItemReader<String, String> reader = (item, ctx) -> List.of("record1");

        BatchProcessor<String, Void> failingProcessor = (records, ctx) -> {
            executedSteps.add("STEP1");
            throw new RuntimeException("Processing failed");
        };

        BatchProcessor<String, Void> normalProcessor = (records, ctx) -> {
            executedSteps.add("STEP2");
            return List.of(ProcessingResult.success(null));
        };

        BatchDefinition batch = IPBatch.createBatch("ABORT_BATCH")
                .addStep("STEP1")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(failingProcessor)
                .addStep("STEP2")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(normalProcessor)
                .done()
                .build();

        ExecutionContext context = new ExecutionContext();
        BatchResult result = executor.execute(batch, context);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.isAborted()).isTrue();
        assertThat(executedSteps).containsExactly("STEP1");
    }

    @Test
    void shouldCaptureMetricsInResult() {
        WorkItemFetcher<String> fetcher = ctx -> List.of("A", "B", "C");
        WorkItemReader<String, String> reader = (item, ctx) -> List.of(item + "1", item + "2");
        BatchProcessor<String, Void> processor = (records, ctx) ->
                List.of(ProcessingResult.success(null));

        BatchDefinition batch = IPBatch.createBatch("METRICS_BATCH")
                .addStep("STEP1")
                .workItemFetcher(fetcher)
                .workItemReader(reader)
                .batchProcessor(processor)
                .done()
                .build();

        ExecutionContext context = new ExecutionContext();
        BatchResult result = executor.execute(batch, context);

        assertThat(result.getDuration()).isNotNull();
        assertThat(result.getStartTime()).isNotNull();
        assertThat(result.getEndTime()).isNotNull();

        BatchStepResult stepResult = result.getStepResults().get(0);
        assertThat(stepResult.getStepName()).isEqualTo("STEP1");
        assertThat(stepResult.isSuccess()).isTrue();
        assertThat(stepResult.getInitialConcurrency()).isNotNull();
    }
}