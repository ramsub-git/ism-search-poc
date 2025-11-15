package com.sephora.services.ipbatch;

import com.sephora.services.ismsearchpoc.ipbatch.strategy.WorkloadAwareConcurrencyStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.core.*;
import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.ipbatch.model.ProcessingResult;
import com.sephora.services.ismsearchpoc.ipbatch.runtime.BatchEngineFactory;
import com.sephora.services.ismsearchpoc.ipbatch.runtime.BatchPipelineExecutor;
import com.sephora.services.ismsearchpoc.ipbatch.runtime.ResourceMonitor;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingScope;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.SizingStrategy;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadAnalyzer;
import com.sephora.services.ismsearchpoc.ipbatch.worker.BatchProcessor;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemFetcher;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemReader;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end test simulating the skuloc cleanup batch
 */
class IPBatchEndToEndTest {

    private BatchPipelineExecutor executor;
    private List<SkulocRecord> database;

    @Data
    static class SkulocRecord {
        private int locationId;
        private String sku;
        private Integer quantity;
        private Double cost;
        private Boolean isComingle;
    }

    @BeforeEach
    void setUp() {
        // Setup in-memory database
        database = new ArrayList<>();

        // Setup executor
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setMaximumPoolSize(10);
        HikariDataSource dataSource = new HikariDataSource(config);

        WorkloadAnalyzer workloadAnalyzer = new WorkloadAnalyzer();
        ResourceMonitor resourceMonitor = new ResourceMonitor(dataSource);
        BatchEngineFactory engineFactory = new BatchEngineFactory();

        executor = new BatchPipelineExecutor(workloadAnalyzer, resourceMonitor, engineFactory);

        BatchRegistry.clear();
    }

    @Test
    void shouldExecuteSkulocCleanupBatch() {
        // Given - Setup test data with problems
        for (int locId = 1; locId <= 100; locId++) {
            SkulocRecord record = new SkulocRecord();
            record.setLocationId(locId);
            record.setSku("SKU-" + locId);
            record.setQuantity(null); // Problem 1: null quantity
            record.setCost(null); // Problem 2: null cost
            record.setIsComingle(null); // Problem 3: null is_comingle
            database.add(record);
        }

        // Setup workers
        WorkItemFetcher<LocationRange> locationRangeFetcher = ctx ->
                List.of(
                        new LocationRange(1, 50),
                        new LocationRange(51, 100)
                );

        WorkItemReader<LocationRange, SkulocRecord> dbReader = (range, ctx) ->
                database.stream()
                        .filter(r -> r.getLocationId() >= range.start && r.getLocationId() <= range.end)
                        .collect(Collectors.toList());

        BatchProcessor<SkulocRecord, Void> nullReplacementProcessor = (records, ctx) -> {
            records.forEach(r -> {
                if (r.getQuantity() == null) r.setQuantity(0);
                if (r.getCost() == null) r.setCost(0.0);
            });
            return List.of(ProcessingResult.success(null));        };

        BatchProcessor<SkulocRecord, Void> comingleProcessor = (records, ctx) -> {
            records.forEach(r -> {
                r.setIsComingle(r.getLocationId() <= 50);
            });
            return List.of(ProcessingResult.success(null));        };

        // Configure batch
        BatchDefinition batch = IPBatch.createBatch("SKULOC_CLEANUP")
                .withDescription("End-to-end skuloc cleanup test")
                .withSizingStrategy(SizingStrategy.DYNAMIC)
                .withSizingScope(SizingScope.BATCH_LEVEL)
                .withRecordCounter(ctx -> (long) database.size())
                .withConcurrencyStrategy(
                        WorkloadAwareConcurrencyStrategy.builder()
                                .smallBatchThreshold(1)
                                .mediumBatchThreshold(5)
                                .build())
                .withConcurrencyLimits(2, 10, 2, 8)
                .withPerformanceGoal(Duration.ofMinutes(5), 10.0)
                .withResourceGoal(10, 0.8, 0.8)

                .addStep("NULL_REPLACEMENT")
                .workItemFetcher(locationRangeFetcher)
                .workItemReader(dbReader)
                .batchProcessor(nullReplacementProcessor)
                .estimatedRecordsPerItem(50)

                .addStep("COMINGLE_CALCULATION")
                .workItemFetcher(locationRangeFetcher)
                .workItemReader(dbReader)
                .batchProcessor(comingleProcessor)
                .estimatedRecordsPerItem(50)

                .beforeBatch(() -> System.out.println("Starting cleanup"))
                .afterBatch(result -> System.out.println("Cleanup complete: " +
                        result.getTotalRecordsProcessed() + " records"))

                .build();

        // When
        ExecutionContext context = new ExecutionContext();
        BatchResult result = executor.execute(batch, context);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.isAborted()).isFalse();
        assertThat(result.getStepResults()).hasSize(2);
//        assertThat(result.getTotalItemsProcessed()).isEqualTo(4); // 2 ranges * 2 steps
//        assertThat(result.getTotalRecordsProcessed()).isEqualTo(200); // 100 records * 2 steps

        // Verify data was cleaned
        assertThat(database)
                .allSatisfy(record -> {
                    assertThat(record.getQuantity()).isNotNull().isEqualTo(0);
                    assertThat(record.getCost()).isNotNull().isEqualTo(0.0);
                    assertThat(record.getIsComingle()).isNotNull();
                });

        // Verify comingle logic
        long comingleCount = database.stream()
                .filter(SkulocRecord::getIsComingle)
                .count();
        assertThat(comingleCount).isEqualTo(50); // Locations 1-50
    }

    @Data
    static class LocationRange {
        private final int start;
        private final int end;
    }
}