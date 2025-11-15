package com.sephora.services.ipbatch.sizing;

import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadAnalysis;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadAnalyzer;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadSize;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.RecordCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class WorkloadAnalyzerTest {

    private WorkloadAnalyzer analyzer;

    @BeforeEach
    void setUp() {
        analyzer = new WorkloadAnalyzer();
    }

    @Test
    void shouldAnalyzeStaticStrategy() {
        // Given
        ExecutionContext context = new ExecutionContext();

        // When
        WorkloadAnalysis result = analyzer.analyze(
                SizingStrategy.STATIC,
                100,
                null,
                null,
                context
        );

        // Then
        assertThat(result.getWorkItemCount()).isEqualTo(100);
        assertThat(result.getTotalRecords()).isEqualTo(-1);
        assertThat(result.categorize()).isEqualTo(WorkloadSize.MEDIUM);
    }

    @Test
    void shouldAnalyzeEstimatedStrategy() {
        // Given
        ExecutionContext context = new ExecutionContext();

        // When
        WorkloadAnalysis result = analyzer.analyze(
                SizingStrategy.ESTIMATED,
                100,
                5000L,
                null,
                context
        );

        // Then
        assertThat(result.getWorkItemCount()).isEqualTo(100);
        assertThat(result.getTotalRecords()).isEqualTo(500000);
        assertThat(result.getAverageRecordsPerItem()).isEqualTo(5000);
    }

    @Test
    void shouldAnalyzeDynamicStrategy() {
        // Given
        ExecutionContext context = new ExecutionContext();
        RecordCounter counter = ctx -> 1_000_000L;

        // When
        WorkloadAnalysis result = analyzer.analyze(
                SizingStrategy.DYNAMIC,
                100,
                null,
                counter,
                context
        );

        // Then
        assertThat(result.getWorkItemCount()).isEqualTo(100);
        assertThat(result.getTotalRecords()).isEqualTo(1_000_000);
        assertThat(result.getAverageRecordsPerItem()).isEqualTo(10_000);
    }

    @Test
    void shouldCategorizeSmallWorkload() {
        WorkloadAnalysis analysis = WorkloadAnalysis.builder()
                .workItemCount(30)
                .build();

        assertThat(analysis.categorize()).isEqualTo(WorkloadSize.SMALL);
    }

    @Test
    void shouldCategorizeMediumWorkload() {
        WorkloadAnalysis analysis = WorkloadAnalysis.builder()
                .workItemCount(200)
                .build();

        assertThat(analysis.categorize()).isEqualTo(WorkloadSize.MEDIUM);
    }

    @Test
    void shouldCategorizeLargeWorkload() {
        WorkloadAnalysis analysis = WorkloadAnalysis.builder()
                .workItemCount(800)
                .build();

        assertThat(analysis.categorize()).isEqualTo(WorkloadSize.LARGE);
    }

    @Test
    void shouldThrowExceptionWhenEstimatedRecordsPerItemMissing() {
        ExecutionContext context = new ExecutionContext();

        assertThatThrownBy(() -> analyzer.analyze(
                SizingStrategy.ESTIMATED,
                100,
                null,
                null,
                context
        ))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ESTIMATED sizing requires estimatedRecordsPerItem");
    }

    @Test
    void shouldThrowExceptionWhenRecordCounterMissing() {
        ExecutionContext context = new ExecutionContext();

        assertThatThrownBy(() -> analyzer.analyze(
                SizingStrategy.DYNAMIC,
                100,
                null,
                null,
                context
        ))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("DYNAMIC sizing requires RecordCounter");
    }
}