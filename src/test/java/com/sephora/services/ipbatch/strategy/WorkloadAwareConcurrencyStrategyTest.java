package com.sephora.services.ipbatch.strategy;

import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadAnalysis;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadSize;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.ConcurrencyLimits;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.InitialConcurrency;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.ResourceSnapshot;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.WorkloadAwareConcurrencyStrategy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class WorkloadAwareConcurrencyStrategyTest {

    @Test
    void shouldCalculateMinimumConcurrencyForSmallWorkload() {
        // Given
        WorkloadAwareConcurrencyStrategy strategy = WorkloadAwareConcurrencyStrategy.builder().build();

        WorkloadAnalysis workload = WorkloadAnalysis.builder()
                .workItemCount(30)
                .totalRecords(150_000)
                .averageRecordsPerItem(5000)
                .build();

        ConcurrencyLimits limits = ConcurrencyLimits.builder()
                .minWorkItemConcurrency(5)
                .maxWorkItemConcurrency(30)
                .minProcessingConcurrency(3)
                .maxProcessingConcurrency(20)
                .build();

        ResourceSnapshot resources = ResourceSnapshot.builder()
                .availableDbConnections(50)
                .availableHeapPercent(0.6)
                .availableCpuCores(8)
                .build();

        // When
        InitialConcurrency result = strategy.calculate(workload, limits, resources);

        // Then
        assertThat(result.getWorkItemConcurrency()).isEqualTo(5);
        assertThat(result.getProcessingConcurrency()).isEqualTo(3);
        assertThat(result.getRationale()).contains("SMALL");
    }

    @Test
    void shouldCalculateMediumConcurrencyForMediumWorkload() {
        // Given
        WorkloadAwareConcurrencyStrategy strategy = WorkloadAwareConcurrencyStrategy.builder().build();

        WorkloadAnalysis workload = WorkloadAnalysis.builder()
                .workItemCount(200)
                .totalRecords(1_000_000)
                .averageRecordsPerItem(5000)
                .build();

        ConcurrencyLimits limits = ConcurrencyLimits.builder()
                .minWorkItemConcurrency(5)
                .maxWorkItemConcurrency(30)
                .minProcessingConcurrency(3)
                .maxProcessingConcurrency(20)
                .build();

        ResourceSnapshot resources = ResourceSnapshot.builder()
                .availableDbConnections(50)
                .availableHeapPercent(0.6)
                .availableCpuCores(8)
                .build();

        // When
        InitialConcurrency result = strategy.calculate(workload, limits, resources);

        // Then
        assertThat(result.getWorkItemConcurrency()).isEqualTo(17); // (5 + 30) / 2
        assertThat(result.getProcessingConcurrency()).isEqualTo(11); // (3 + 20) / 2
        assertThat(result.getRationale()).contains("MEDIUM");
    }

    @Test
    void shouldCalculateHighConcurrencyForLargeWorkload() {
        // Given
        WorkloadAwareConcurrencyStrategy strategy = WorkloadAwareConcurrencyStrategy.builder().build();

        WorkloadAnalysis workload = WorkloadAnalysis.builder()
                .workItemCount(800)
                .totalRecords(4_000_000)
                .averageRecordsPerItem(5000)
                .build();

        ConcurrencyLimits limits = ConcurrencyLimits.builder()
                .minWorkItemConcurrency(5)
                .maxWorkItemConcurrency(30)
                .minProcessingConcurrency(3)
                .maxProcessingConcurrency(20)
                .build();

        ResourceSnapshot resources = ResourceSnapshot.builder()
                .availableDbConnections(50)
                .availableHeapPercent(0.6)
                .availableCpuCores(8)
                .build();

        // When
        InitialConcurrency result = strategy.calculate(workload, limits, resources);

        // Then
        assertThat(result.getWorkItemConcurrency()).isEqualTo(24); // 30 * 0.8
        assertThat(result.getProcessingConcurrency()).isEqualTo(16); // 20 * 0.8
        assertThat(result.getRationale()).contains("LARGE");
    }

    @Test
    void shouldRespectDatabaseConnectionLimits() {
        // Given
        WorkloadAwareConcurrencyStrategy strategy = WorkloadAwareConcurrencyStrategy.builder().build();

        WorkloadAnalysis workload = WorkloadAnalysis.builder()
                .workItemCount(800)
                .totalRecords(4_000_000)
                .averageRecordsPerItem(5000)
                .build();

        ConcurrencyLimits limits = ConcurrencyLimits.builder()
                .minWorkItemConcurrency(5)
                .maxWorkItemConcurrency(30)
                .minProcessingConcurrency(3)
                .maxProcessingConcurrency(20)
                .build();

        ResourceSnapshot resources = ResourceSnapshot.builder()
                .availableDbConnections(10) // Only 10 available
                .availableHeapPercent(0.6)
                .availableCpuCores(8)
                .build();

        // When
        InitialConcurrency result = strategy.calculate(workload, limits, resources);

        // Then
        assertThat(result.getWorkItemConcurrency()).isEqualTo(7); // min(24, 10 * 0.7)
    }
}