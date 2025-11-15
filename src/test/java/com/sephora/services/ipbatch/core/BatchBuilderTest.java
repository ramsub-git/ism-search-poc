package com.sephora.services.ipbatch.core;

import com.sephora.services.ismsearchpoc.ipbatch.core.BatchDefinition;
import com.sephora.services.ismsearchpoc.ipbatch.core.BatchRegistry;
import com.sephora.services.ismsearchpoc.ipbatch.core.BatchStepDefinition;
import com.sephora.services.ismsearchpoc.ipbatch.core.IPBatch;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.WorkloadAwareConcurrencyStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingScope;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.worker.BatchProcessor;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemFetcher;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class BatchBuilderTest {

    private WorkItemFetcher<String> mockFetcher;
    private WorkItemReader<String, String> mockReader;
    private BatchProcessor<String, Void> mockProcessor;

    @BeforeEach
    void setUp() {
        mockFetcher = ctx -> List.of("item1", "item2");
        mockReader = (item, ctx) -> List.of("record1", "record2");
        mockProcessor = (records, ctx) -> null;

        BatchRegistry.clear();
    }

    @Test
    void shouldBuildBatchWithMinimalConfiguration() {
        // When
        BatchDefinition batch = IPBatch.createBatch("TEST_BATCH")
                .addStep("STEP1")
                .workItemFetcher(mockFetcher)
                .workItemReader(mockReader)
                .batchProcessor(mockProcessor)
                .done()
                .build();

        // Then
        assertThat(batch.getBatchName()).isEqualTo("TEST_BATCH");
        assertThat(batch.getSteps()).hasSize(1);
        assertThat(batch.getSteps().get(0).getStepName()).isEqualTo("STEP1");
    }

    @Test
    void shouldBuildBatchWithFullConfiguration() {
        // When
        BatchDefinition batch = IPBatch.createBatch("FULL_BATCH")
                .withDescription("Test batch")
                .withProcessingContext("TEST_CONTEXT")
                .withSizingStrategy(SizingStrategy.DYNAMIC)
                .withSizingScope(SizingScope.STEP_LEVEL)
                .withRecordCounter(ctx -> 1000L)
                .withConcurrencyStrategy(WorkloadAwareConcurrencyStrategy.builder().build())
                .withConcurrencyLimits(5, 30, 3, 20)
                .withPerformanceGoal(Duration.ofMinutes(30), 100.0)
                .withResourceGoal(100, 0.8, 0.8)
                .withErrorGoal(0.01, 1000)
                .addStep("STEP1")
                .workItemFetcher(mockFetcher)
                .workItemReader(mockReader)
                .batchProcessor(mockProcessor)
                .withStepGuard("GUARD1")
                .estimatedRecordsPerItem(5000)
                .done()
                .build();

        // Then
        assertThat(batch.getBatchName()).isEqualTo("FULL_BATCH");
        assertThat(batch.getDescription()).isEqualTo("Test batch");
        assertThat(batch.getProcessingContextName()).isEqualTo("TEST_CONTEXT");
        assertThat(batch.getSizingStrategy()).isEqualTo(SizingStrategy.DYNAMIC);
        assertThat(batch.getSizingScope()).isEqualTo(SizingScope.STEP_LEVEL);
        assertThat(batch.getConcurrencyLimits().getMinWorkItemConcurrency()).isEqualTo(5);
        assertThat(batch.getGoals()).hasSize(3);
        assertThat(batch.getSteps()).hasSize(1);
    }

    @Test
    void shouldBuildBatchWithMultipleSteps() {
        // When
        BatchDefinition batch = IPBatch.createBatch("MULTI_STEP")
                .addStep("STEP1")
                .workItemFetcher(mockFetcher)
                .workItemReader(mockReader)
                .batchProcessor(mockProcessor)
                .addStep("STEP2")
                .workItemFetcher(mockFetcher)
                .workItemReader(mockReader)
                .batchProcessor(mockProcessor)
                .addStep("STEP3")
                .workItemFetcher(mockFetcher)
                .workItemReader(mockReader)
                .batchProcessor(mockProcessor)
                .done()
                .build();

        // Then
        assertThat(batch.getSteps()).hasSize(3);
        assertThat(batch.getSteps())
                .extracting(BatchStepDefinition::getStepName)
                .containsExactly("STEP1", "STEP2", "STEP3");
    }

    @Test
    void shouldRegisterBatch() {
        // When
        IPBatch.createBatch("REGISTERED_BATCH")
                .addStep("STEP1")
                .workItemFetcher(mockFetcher)
                .workItemReader(mockReader)
                .batchProcessor(mockProcessor)
                .register();

        // Then
        assertThat(BatchRegistry.exists("REGISTERED_BATCH")).isTrue();
        BatchDefinition registered = BatchRegistry.get("REGISTERED_BATCH");
        assertThat(registered.getBatchName()).isEqualTo("REGISTERED_BATCH");
    }

    @Test
    void shouldThrowExceptionWhenNoSteps() {
        // When/Then
        assertThatThrownBy(() ->
                IPBatch.createBatch("NO_STEPS").build()
        )
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Batch must have at least one step");
    }

    @Test
    void shouldThrowExceptionWhenDynamicSizingWithoutRecordCounter() {
        // When/Then
        assertThatThrownBy(() ->
                IPBatch.createBatch("INVALID_DYNAMIC")
                        .withSizingStrategy(SizingStrategy.DYNAMIC)
                        .withSizingScope(SizingScope.BATCH_LEVEL)
                        .addStep("STEP1")
                        .workItemFetcher(mockFetcher)
                        .workItemReader(mockReader)
                        .batchProcessor(mockProcessor)
                        .build()
        )
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("DYNAMIC sizing at BATCH_LEVEL requires a RecordCounter");
    }

    @Test
    void shouldBuildStepWithStepLevelSizing() {
        // When
        BatchDefinition batch = IPBatch.createBatch("STEP_LEVEL_SIZING")
                .withSizingStrategy(SizingStrategy.STATIC)
                .addStep("STEP1")
                .workItemFetcher(mockFetcher)
                .workItemReader(mockReader)
                .batchProcessor(mockProcessor)
                .withStepSizingStrategy(SizingStrategy.DYNAMIC)
                .withRecordCounter(ctx -> 5000L)
                .done()
                .build();

        // Then
        BatchStepDefinition step = batch.getSteps().get(0);
        assertThat(step.hasStepLevelSizing()).isTrue();
        assertThat(step.getStepSizingStrategy()).isEqualTo(SizingStrategy.DYNAMIC);
    }

    @Test
    void shouldExecuteHooks() {
        // Given
        boolean[] hookExecuted = {false, false, false, false};

        // When
        IPBatch.createBatch("HOOKS_BATCH")
                .beforeBatch(() -> hookExecuted[0] = true)
                .afterBatch(result -> hookExecuted[1] = true)
                .beforeStep(result -> hookExecuted[2] = true)
                .afterStep(result -> hookExecuted[3] = true)
                .addStep("STEP1")
                .workItemFetcher(mockFetcher)
                .workItemReader(mockReader)
                .batchProcessor(mockProcessor)
                .build();

        // Then - hooks are stored, not executed during build
        BatchDefinition batch = BatchRegistry.get("HOOKS_BATCH");
        assertThat(batch.getBeforeBatchHook()).isNotNull();
        assertThat(batch.getAfterBatchHook()).isNotNull();
        assertThat(batch.getBeforeStepHook()).isNotNull();
        assertThat(batch.getAfterStepHook()).isNotNull();
    }
}