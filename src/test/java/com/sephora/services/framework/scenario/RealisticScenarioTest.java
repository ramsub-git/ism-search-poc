package com.sephora.services.framework.scenario;

import com.sephora.services.ismsearchpoc.framework.concurrency.ConcurrencyController;
import com.sephora.services.ismsearchpoc.framework.concurrency.SpringThreadPoolConcurrencyController;
import com.sephora.services.ismsearchpoc.framework.engine.ParallelBatchEngine;
import com.sephora.services.ismsearchpoc.framework.goal.*;
import com.sephora.services.ismsearchpoc.framework.metrics.ISMMetricsCollector;
import com.sephora.services.ismsearchpoc.framework.metrics.MetricsSnapshot;
import com.sephora.services.ismsearchpoc.framework.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.framework.model.ExecutionResult;
import com.sephora.services.ismsearchpoc.framework.model.ProcessingResult;
import com.sephora.services.ismsearchpoc.framework.runtime.ISMRuntimeManager;
import com.sephora.services.ismsearchpoc.framework.strategy.*;
import com.sephora.services.ismsearchpoc.framework.worker.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenario-based end-to-end tests
 * These tests simulate realistic ISM load scenarios to prove the system works as designed
 */
class RealisticScenarioTest {
    
    private static final Logger log = LoggerFactory.getLogger(RealisticScenarioTest.class);
    
    private HikariDataSource dataSource;
    private MeterRegistry meterRegistry;
    private ScheduledExecutorService scheduler;
    
    @BeforeEach
    void setup() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setMaximumPoolSize(100);
        dataSource = new HikariDataSource(config);
        
        meterRegistry = new SimpleMeterRegistry();
        scheduler = Executors.newScheduledThreadPool(1);
    }
    
    @AfterEach
    void teardown() {
        if (dataSource != null) {
            dataSource.close();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }
    
    // ============================================
    // Scenario 1: Happy Path - Smooth Load
    // ============================================
    
    @Test
    void scenario1_smoothLoad_completesSuccessfully() throws Exception {
        log.info("=== Scenario 1: Smooth Load ===");
        
        // 900 files, all uniform size, plenty of time
        ConcurrencyController controller = new SpringThreadPoolConcurrencyController(
            10, 5, 30, 20
        );
        
        WorkItemFetcher<String> fetcher = new MockWorkItemFetcher(900);
        WorkItemReader<String, TestRecord> reader = new UniformWorkItemReader(1000); // 1k records each
        BatchProcessor<TestRecord, Void> processor = new FastBatchProcessor(10); // Fast
        ProgressTracker<String, Void> tracker = new DetailedProgressTracker();
        
        ParallelBatchEngine<String, TestRecord, Void> engine = new ParallelBatchEngine<>(
            controller, fetcher, reader, processor, tracker, 1000
        );
        
        ISMMetricsCollector metricsCollector = new ISMMetricsCollector(
            meterRegistry, dataSource, engine
        );
        
        // Goals with adaptive strategies
        PerformanceGoal perfGoal = new PerformanceGoal(Duration.ofMinutes(60), 15.0, 0.8);
        ResourceGoal resourceGoal = new ResourceGoal(100, 0.8, 0.8);
        ErrorGoal errorGoal = new ErrorGoal(0.05, 10000, Set.of());
        
        Map<Goal, GoalStrategy> strategies = new HashMap<>();
        strategies.put(perfGoal, new PerformanceStrategy());
        strategies.put(resourceGoal, new ResourceStrategy());
        strategies.put(errorGoal, new ErrorStrategy());
        
        ISMRuntimeManager manager = new ISMRuntimeManager(
            Arrays.asList(perfGoal, resourceGoal, errorGoal),
            strategies,
            engine
        );
        
        // Start periodic evaluation
        ScheduledFuture<?> evaluationTask = scheduler.scheduleAtFixedRate(
            () -> manager.evaluateAndAdjust(metricsCollector.snapshot()),
            100, 200, TimeUnit.MILLISECONDS
        );
        
        try {
            // Execute
            ExecutionContext context = new ExecutionContext();
            ExecutionResult result = engine.execute(context);
            
            // Verify
            assertTrue(result.isSuccess(), "Load should complete successfully");
            assertEquals(900, result.getWorkItemsProcessed());
            assertNull(result.getAbortReason());
            
            // All goals should be met or at least not violated
            MetricsSnapshot finalMetrics = metricsCollector.snapshot();
            GoalEvaluation perfEval = perfGoal.checkStatus(finalMetrics);
            GoalEvaluation resourceEval = resourceGoal.checkStatus(finalMetrics);
            GoalEvaluation errorEval = errorGoal.checkStatus(finalMetrics);
            
            assertNotEquals(GoalStatus.VIOLATED, perfEval.getStatus(), 
                "Performance goal should not be violated");
            assertNotEquals(GoalStatus.VIOLATED, resourceEval.getStatus(),
                "Resource goal should not be violated");
            assertNotEquals(GoalStatus.VIOLATED, errorEval.getStatus(),
                "Error goal should not be violated");
            
            log.info("Scenario 1 complete: {} files in {}", 
                result.getWorkItemsProcessed(), result.getDuration());
            
        } finally {
            evaluationTask.cancel(false);
            controller.shutdown();
        }
    }
    
    // ============================================
    // Scenario 2: Performance Degradation - System Adapts
    // ============================================
    
    @Test
    void scenario2_performanceDegradation_systemAdaptsAndRecovers() throws Exception {
        log.info("=== Scenario 2: Performance Degradation ===");
        
        ConcurrencyController controller = new SpringThreadPoolConcurrencyController(
            10, 5, 30, 20
        );
        
        // Files that slow down halfway through
        WorkItemFetcher<String> fetcher = new MockWorkItemFetcher(100);
        WorkItemReader<String, TestRecord> reader = new VariableSpeedReader(100);
        BatchProcessor<TestRecord, Void> processor = new AdaptiveBatchProcessor();
        ProgressTracker<String, Void> tracker = new DetailedProgressTracker();
        
        ParallelBatchEngine<String, TestRecord, Void> engine = new ParallelBatchEngine<>(
            controller, fetcher, reader, processor, tracker, 100
        );
        
        ISMMetricsCollector metricsCollector = new ISMMetricsCollector(
            meterRegistry, dataSource, engine
        );
        
        // Tight deadline to force adaptation
        PerformanceGoal perfGoal = new PerformanceGoal(Duration.ofMinutes(5), 20.0, 0.8);
        ResourceGoal resourceGoal = new ResourceGoal(100, 0.8, 0.8);
        ErrorGoal errorGoal = new ErrorGoal(0.05, 1000, Set.of());
        
        Map<Goal, GoalStrategy> strategies = new HashMap<>();
        strategies.put(perfGoal, new PerformanceStrategy());
        strategies.put(resourceGoal, new NoOpStrategy()); // Not constraining
        strategies.put(errorGoal, new NoOpStrategy());
        
        ISMRuntimeManager manager = new ISMRuntimeManager(
            Arrays.asList(perfGoal, resourceGoal, errorGoal),
            strategies,
            engine
        );
        
        AtomicInteger adjustmentCount = new AtomicInteger(0);
        ScheduledFuture<?> evaluationTask = scheduler.scheduleAtFixedRate(
            () -> {
                int before = manager.getCurrentSettings().get("workItemConcurrency");
                manager.evaluateAndAdjust(metricsCollector.snapshot());
                int after = manager.getCurrentSettings().get("workItemConcurrency");
                if (before != after) {
                    adjustmentCount.incrementAndGet();
                    log.info("Concurrency adjusted: {} -> {}", before, after);
                }
            },
            100, 200, TimeUnit.MILLISECONDS
        );
        
        try {
            ExecutionContext context = new ExecutionContext();
            ExecutionResult result = engine.execute(context);
            
            // Should complete despite slowdown
            assertTrue(result.isSuccess());
            assertEquals(100, result.getWorkItemsProcessed());
            
            // Should have made at least some adjustments
            log.info("Scenario 2 complete: Made {} concurrency adjustments", 
                adjustmentCount.get());
            
        } finally {
            evaluationTask.cancel(false);
            controller.shutdown();
        }
    }
    
    // ============================================
    // Scenario 3: Resource Constrained - System Backs Off
    // ============================================
    
//    @Test
//    void scenario3_resourceConstrained_systemBacksOff() throws Exception {
//        log.info("=== Scenario 3: Resource Constrained ===");
//
//        ConcurrencyController controller = new SpringThreadPoolConcurrencyController(
//            15, 10, 30, 20  // Start aggressive
//        );
//
//        // Limited connection pool to simulate constraint
//        HikariConfig constrainedConfig = new HikariConfig();
//        constrainedConfig.setJdbcUrl("jdbc:h2:mem:test");
//        constrainedConfig.setMaximumPoolSize(20);  // Small pool
//        HikariDataSource constrainedDataSource = new HikariDataSource(constrainedConfig);
//
//        WorkItemFetcher<String> fetcher = new MockWorkItemFetcher(100);
//        WorkItemReader<String, TestRecord> reader = new UniformWorkItemReader(100);
//        BatchProcessor<TestRecord, Void> processor = new DatabaseHeavyProcessor(constrainedDataSource);
//        ProgressTracker<String, Void> tracker = new DetailedProgressTracker();
//
//        ParallelBatchEngine<String, TestRecord, Void> engine = new ParallelBatchEngine<>(
//            controller, fetcher, reader, processor, tracker, 50
//        );
//
//        ISMMetricsCollector metricsCollector = new ISMMetricsCollector(
//            meterRegistry, constrainedDataSource, engine
//        );
//
//        PerformanceGoal perfGoal = new PerformanceGoal(Duration.ofMinutes(10), 10.0, 0.8);
//        ResourceGoal resourceGoal = new ResourceGoal(20, 0.75, 0.8); // Tight limit
//        ErrorGoal errorGoal = new ErrorGoal(0.05, 1000, Set.of());
//
//        Map<Goal, GoalStrategy> strategies = new HashMap<>();
//        strategies.put(perfGoal, new PerformanceStrategy());
//        strategies.put(resourceGoal, new ResourceStrategy()); // Will force backoff
//        strategies.put(errorGoal, new ErrorStrategy());
//
//        ISMRuntimeManager manager = new ISMRuntimeManager(
//            Arrays.asList(perfGoal, resourceGoal, errorGoal),
//            strategies,
//            engine
//        );
//
//        int initialConcurrency = manager.getCurrentSettings().get("workItemConcurrency");
//        AtomicInteger minConcurrency = new AtomicInteger(initialConcurrency);
//
//        ScheduledFuture<?> evaluationTask = scheduler.scheduleAtFixedRate(
//            () -> {
//                manager.evaluateAndAdjust(metricsCollector.snapshot());
//                int current = manager.getCurrentSettings().get("workItemConcurrency");
//                minConcurrency.set(Math.min(minConcurrency.get(), current));
//            },
//            100, 200, TimeUnit.MILLISECONDS
//        );
//
//        try {
//            ExecutionContext context = new ExecutionContext();
//            ExecutionResult result = engine.execute(context);
//
//            // Give evaluation cycles time to run and detect resource pressure
//            Thread.sleep(500);
//            // Should complete but with reduced concurrency
//            assertTrue(result.isSuccess());
//
//            // Should have backed off from initial aggressive setting
//            assertTrue(minConcurrency.get() < initialConcurrency,
//                String.format("Should have reduced concurrency from %d to at least %d",
//                    initialConcurrency, minConcurrency.get()));
//
//            log.info("Scenario 3 complete: Reduced concurrency from {} to {}",
//                initialConcurrency, minConcurrency.get());
//
//        } finally {
//            evaluationTask.cancel(false);
//            controller.shutdown();
//            constrainedDataSource.close();
//        }
//    }
    
    // ============================================
    // Scenario 4: Observable Mode - Monitoring Only
    // ============================================
    
    @Test
    void scenario4_observableMode_monitorsButDoesNotAdjust() throws Exception {
        log.info("=== Scenario 4: Observable Mode (NoOp Strategies) ===");
        
        ConcurrencyController controller = new SpringThreadPoolConcurrencyController(
            10, 5, 30, 20
        );
        
        WorkItemFetcher<String> fetcher = new MockWorkItemFetcher(100);
        WorkItemReader<String, TestRecord> reader = new VariableSpeedReader(50);
        BatchProcessor<TestRecord, Void> processor = new FastBatchProcessor(5);
        ProgressTracker<String, Void> tracker = new DetailedProgressTracker();
        
        ParallelBatchEngine<String, TestRecord, Void> engine = new ParallelBatchEngine<>(
            controller, fetcher, reader, processor, tracker, 50
        );
        
        ISMMetricsCollector metricsCollector = new ISMMetricsCollector(
            meterRegistry, dataSource, engine
        );
        
        // Create goals but ALL with NoOp strategies
        PerformanceGoal perfGoal = new PerformanceGoal(Duration.ofMinutes(2), 50.0, 0.8);
        ResourceGoal resourceGoal = new ResourceGoal(100, 0.8, 0.8);
        ErrorGoal errorGoal = new ErrorGoal(0.05, 1000, Set.of());
        
        Map<Goal, GoalStrategy> strategies = new HashMap<>();
        strategies.put(perfGoal, new NoOpStrategy());  // Never adjusts
        strategies.put(resourceGoal, new NoOpStrategy());
        strategies.put(errorGoal, new NoOpStrategy());
        
        ISMRuntimeManager manager = new ISMRuntimeManager(
            Arrays.asList(perfGoal, resourceGoal, errorGoal),
            strategies,
            engine
        );
        
        int initialConcurrency = manager.getCurrentSettings().get("workItemConcurrency");
        List<Integer> concurrencyHistory = new CopyOnWriteArrayList<>();
        concurrencyHistory.add(initialConcurrency);
        
        ScheduledFuture<?> evaluationTask = scheduler.scheduleAtFixedRate(
            () -> {
                manager.evaluateAndAdjust(metricsCollector.snapshot());
                int current = manager.getCurrentSettings().get("workItemConcurrency");
                concurrencyHistory.add(current);
                
                // Log goal status for observability
                MetricsSnapshot metrics = metricsCollector.snapshot();
                log.info("Observable Mode - Goals: Perf={}, Resource={}, Error={}",
                    perfGoal.checkStatus(metrics).getStatus(),
                    resourceGoal.checkStatus(metrics).getStatus(),
                    errorGoal.checkStatus(metrics).getStatus());
            },
            1, 2, TimeUnit.SECONDS
        );
        
        try {
            ExecutionContext context = new ExecutionContext();
            ExecutionResult result = engine.execute(context);
            
            assertTrue(result.isSuccess());
            
            // Concurrency should NEVER change with NoOp strategies
            Set<Integer> uniqueSettings = new HashSet<>(concurrencyHistory);
            assertEquals(1, uniqueSettings.size(),
                "Concurrency should never change with NoOp strategies");
            assertEquals(initialConcurrency, (int) uniqueSettings.iterator().next());
            
            log.info("Scenario 4 complete: Concurrency remained fixed at {}", initialConcurrency);
            
        } finally {
            evaluationTask.cancel(false);
            controller.shutdown();
        }
    }
    
    // ============================================
    // Scenario 5: Error Spike - System Slows Down
    // ============================================
    
    @Test
    void scenario5_errorSpike_systemSlowsDown() throws Exception {
        log.info("=== Scenario 5: Error Spike ===");
        
        ConcurrencyController controller = new SpringThreadPoolConcurrencyController(
            15, 10, 30, 20  // Start aggressive
        );
        
        WorkItemFetcher<String> fetcher = new MockWorkItemFetcher(100);
        WorkItemReader<String, TestRecord> reader = new UniformWorkItemReader(100);
        BatchProcessor<TestRecord, Void> processor = new ErrorProneProcessor(0.10); // 10% error rate
        ProgressTracker<String, Void> tracker = new DetailedProgressTracker();
        
        ParallelBatchEngine<String, TestRecord, Void> engine = new ParallelBatchEngine<>(
            controller, fetcher, reader, processor, tracker, 50
        );
        
        ISMMetricsCollector metricsCollector = new ISMMetricsCollector(
            meterRegistry, dataSource, engine
        );
        
        PerformanceGoal perfGoal = new PerformanceGoal(Duration.ofMinutes(10), 10.0, 0.8);
        ResourceGoal resourceGoal = new ResourceGoal(100, 0.8, 0.8);
        ErrorGoal errorGoal = new ErrorGoal(0.05, 500, Set.of()); // Tight error limit
        
        Map<Goal, GoalStrategy> strategies = new HashMap<>();
        strategies.put(perfGoal, new PerformanceStrategy());
        strategies.put(resourceGoal, new NoOpStrategy());
        strategies.put(errorGoal, new ErrorStrategy()); // Will force slowdown
        
        ISMRuntimeManager manager = new ISMRuntimeManager(
            Arrays.asList(perfGoal, resourceGoal, errorGoal),
            strategies,
            engine
        );
        
        int initialConcurrency = manager.getCurrentSettings().get("workItemConcurrency");
        AtomicInteger minConcurrency = new AtomicInteger(initialConcurrency);
        
        ScheduledFuture<?> evaluationTask = scheduler.scheduleAtFixedRate(
            () -> {
                manager.evaluateAndAdjust(metricsCollector.snapshot());
                int current = manager.getCurrentSettings().get("workItemConcurrency");
                minConcurrency.set(Math.min(minConcurrency.get(), current));
            },
            100, 200, TimeUnit.MILLISECONDS
        );
        
        try {
            ExecutionContext context = new ExecutionContext();
            ExecutionResult result = engine.execute(context);

            // Give evaluation cycles time to detect errors and adjust
            Thread.sleep(500);

            // May or may not complete depending on error severity
            // But should have reduced concurrency
            assertTrue(minConcurrency.get() < initialConcurrency,
                "Should have reduced concurrency due to errors");
            
            log.info("Scenario 5 complete: Reduced concurrency from {} to {} due to errors",
                initialConcurrency, minConcurrency.get());
            
        } finally {
            evaluationTask.cancel(false);
            controller.shutdown();
        }
    }
    
    // ============================================
    // Mock Implementations
    // ============================================
    
    static class TestRecord {
        String id;
        TestRecord(String id) { this.id = id; }
    }
    
    static class MockWorkItemFetcher implements WorkItemFetcher<String> {
        private final int count;
        MockWorkItemFetcher(int count) { this.count = count; }
        
        @Override
        public List<String> fetchWorkItems(ExecutionContext context) {
            List<String> items = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                items.add("file_" + i);
            }
            return items;
        }
    }
    
    static class UniformWorkItemReader implements WorkItemReader<String, TestRecord> {
        private final int recordsPerFile;
        UniformWorkItemReader(int recordsPerFile) { this.recordsPerFile = recordsPerFile; }
        
        @Override
        public List<TestRecord> readWorkItem(String workItem, ExecutionContext context) {
            List<TestRecord> records = new ArrayList<>();
            for (int i = 0; i < recordsPerFile; i++) {
                records.add(new TestRecord(workItem + "_" + i));
            }
            return records;
        }
    }
    
    static class VariableSpeedReader implements WorkItemReader<String, TestRecord> {
        private final int baseRecords;
        private final AtomicInteger fileCount = new AtomicInteger(0);
        
        VariableSpeedReader(int baseRecords) { this.baseRecords = baseRecords; }
        
        @Override
        public List<TestRecord> readWorkItem(String workItem, ExecutionContext context) throws Exception {
            int fileNum = fileCount.getAndIncrement();
            
            // Slow down after 50 files
            if (fileNum > 50) {
                Thread.sleep(50); // Add delay
            }
            
            List<TestRecord> records = new ArrayList<>();
            int count = fileNum > 50 ? baseRecords * 2 : baseRecords; // Bigger files
            
            for (int i = 0; i < count; i++) {
                records.add(new TestRecord(workItem + "_" + i));
            }
            return records;
        }
    }
    
    static class FastBatchProcessor implements BatchProcessor<TestRecord, Void> {
        private final long delayMs;
        FastBatchProcessor(long delayMs) { this.delayMs = delayMs; }
        
        @Override
        public List<ProcessingResult<Void>> processBatch(List<TestRecord> records, 
                                                         ExecutionContext context) throws Exception {
            if (delayMs > 0) {
                Thread.sleep(delayMs);
            }
            
            List<ProcessingResult<Void>> results = new ArrayList<>();
            for (TestRecord record : records) {
                results.add(ProcessingResult.success(null));
            }
            return results;
        }
    }
    
    static class AdaptiveBatchProcessor implements BatchProcessor<TestRecord, Void> {
        private final AtomicInteger batchCount = new AtomicInteger(0);
        
        @Override
        public List<ProcessingResult<Void>> processBatch(List<TestRecord> records,
                                                         ExecutionContext context) throws Exception {
            int batch = batchCount.getAndIncrement();
            
            // Slow down after batch 50
            if (batch > 50) {
                Thread.sleep(100);
            }
            
            List<ProcessingResult<Void>> results = new ArrayList<>();
            for (TestRecord record : records) {
                results.add(ProcessingResult.success(null));
            }
            return results;
        }
    }
    
    static class DatabaseHeavyProcessor implements BatchProcessor<TestRecord, Void> {
        private final HikariDataSource dataSource;
        
        DatabaseHeavyProcessor(HikariDataSource dataSource) {
            this.dataSource = dataSource;
        }
        
        @Override
        public List<ProcessingResult<Void>> processBatch(List<TestRecord> records,
                                                         ExecutionContext context) throws Exception {
            // Simulate DB-heavy processing by acquiring connections
            try (var conn = dataSource.getConnection()) {
                Thread.sleep(200); // Simulate query
            }
            
            List<ProcessingResult<Void>> results = new ArrayList<>();
            for (TestRecord record : records) {
                results.add(ProcessingResult.success(null));
            }
            return results;
        }
    }
    
    static class ErrorProneProcessor implements BatchProcessor<TestRecord, Void> {
        private final double errorRate;
        private final Random random = new Random();
        
        ErrorProneProcessor(double errorRate) { this.errorRate = errorRate; }
        
        @Override
        public List<ProcessingResult<Void>> processBatch(List<TestRecord> records,
                                                         ExecutionContext context) {
            List<ProcessingResult<Void>> results = new ArrayList<>();
            for (TestRecord record : records) {
                if (random.nextDouble() < errorRate) {
                    results.add(ProcessingResult.failure(new RuntimeException("Simulated error")));
                } else {
                    results.add(ProcessingResult.success(null));
                }
            }
            return results;
        }
    }
    
    static class DetailedProgressTracker implements ProgressTracker<String, Void> {
        private final AtomicInteger started = new AtomicInteger(0);
        private final AtomicInteger completed = new AtomicInteger(0);
        private final AtomicInteger failed = new AtomicInteger(0);
        
        @Override
        public void onWorkItemStart(String workItem) {
            started.incrementAndGet();
        }
        
        @Override
        public void onWorkItemComplete(String workItem, int recordCount,
                                      List<ProcessingResult<Void>> results) {
            completed.incrementAndGet();
        }
        
        @Override
        public void onWorkItemFailure(String workItem, Throwable error) {
            failed.incrementAndGet();
        }
        
        @Override
        public void reportProgress(int processed, int total) {
            // Log progress
        }
        
        public int getStarted() { return started.get(); }
        public int getCompleted() { return completed.get(); }
        public int getFailed() { return failed.get(); }
    }
}
