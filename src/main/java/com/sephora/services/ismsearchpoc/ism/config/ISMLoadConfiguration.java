package com.sephora.services.ismsearchpoc.ism.config;

import com.azure.storage.blob.BlobContainerClient;
import com.sephora.services.ismsearchpoc.framework.concurrency.ConcurrencyController;
import com.sephora.services.ismsearchpoc.framework.concurrency.SpringThreadPoolConcurrencyController;
import com.sephora.services.ismsearchpoc.framework.engine.ParallelBatchEngine;
import com.sephora.services.ismsearchpoc.framework.goal.*;
import com.sephora.services.ismsearchpoc.framework.metrics.ISMMetricsCollector;
import com.sephora.services.ismsearchpoc.framework.runtime.ISMRuntimeManager;
import com.sephora.services.ismsearchpoc.framework.strategy.*;
import com.sephora.services.ismsearchpoc.framework.worker.BatchProcessor;
import com.sephora.services.ismsearchpoc.framework.worker.BlobFileFetcher;
import com.sephora.services.ismsearchpoc.framework.worker.CsvFileReader;
import com.sephora.services.ismsearchpoc.framework.worker.ProgressTracker;
import com.sephora.services.ismsearchpoc.framework.worker.WorkItemFetcher;
import com.sephora.services.ismsearchpoc.framework.worker.WorkItemReader;
import com.sephora.services.ismsearchpoc.ism.mapper.SkulocRecordMapper;
import com.sephora.services.ismsearchpoc.ism.model.SkulocRecord;
import com.sephora.services.ismsearchpoc.ism.worker.ISMProgressTracker;
import com.sephora.services.ismsearchpoc.ism.worker.SkulocDatabaseProcessor;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.*;

/**
 * Spring configuration for ISM data load
 * Wires together framework components with ISM-specific implementations
 */
@Configuration
public class ISMLoadConfiguration {
    
    // ============================================
    // Workers
    // ============================================
    
    @Bean
    public WorkItemFetcher<String> skulocFileFetcher(BlobContainerClient blobClient) {
        return new BlobFileFetcher(blobClient);
    }
    
    @Bean
    public WorkItemReader<String, SkulocRecord> skulocFileReader(
            BlobContainerClient blobClient,
            SkulocRecordMapper recordMapper) {
        
        // Configure CSV reader with header skip and comma delimiter
        return new CsvFileReader<>(blobClient, recordMapper, true, ",");
    }
    
    @Bean
    public BatchProcessor<SkulocRecord, Void> skulocProcessor() {
        return new SkulocDatabaseProcessor();
    }
    
    @Bean
    public ProgressTracker<String, Void> skulocProgressTracker() {
        return new ISMProgressTracker();
    }
    
    // ============================================
    // Concurrency Controller
    // ============================================
    
    @Bean
    public ConcurrencyController ismConcurrencyController() {
        return new SpringThreadPoolConcurrencyController(
            10,  // initial work item concurrency
            5,   // initial processing concurrency
            30,  // max work item concurrency
            20   // max processing concurrency
        );
    }
    
    // ============================================
    // Engine
    // ============================================
    
    @Bean
    public ParallelBatchEngine<String, SkulocRecord, Void> ismLoadEngine(
            ConcurrencyController concurrencyController,
            WorkItemFetcher<String> fetcher,
            WorkItemReader<String, SkulocRecord> reader,
            BatchProcessor<SkulocRecord, Void> processor,
            ProgressTracker<String, Void> tracker) {
        
        int batchSize = 10000; // Process 10k records per batch
        
        return new ParallelBatchEngine<>(
            concurrencyController,
            fetcher,
            reader,
            processor,
            tracker,
            batchSize
        );
    }
    
    // ============================================
    // Goals & Strategies
    // ============================================
    
    @Bean
    public PerformanceGoal ismPerformanceGoal() {
        return new PerformanceGoal(
            Duration.ofMinutes(60),  // 60 minute deadline
            15.0,                     // Min 15 files/min
            0.8                       // 80% tolerance
        );
    }
    
    @Bean
    public ResourceGoal ismResourceGoal() {
        return new ResourceGoal(
            100,  // Max 100 DB connections
            0.8,  // 80% DB utilization threshold
            0.8   // 80% heap utilization threshold
        );
    }
    
    @Bean
    public ErrorGoal ismErrorGoal() {
        return new ErrorGoal(
            0.05,    // 5% error rate threshold
            10000,   // Max 10,000 total errors
            Set.of() // Critical error types (none for now)
        );
    }
    
    // ============================================
    // Runtime Manager
    // ============================================
    
    @Bean
    public ISMRuntimeManager ismRuntimeManager(
            PerformanceGoal perfGoal,
            ResourceGoal resourceGoal,
            ErrorGoal errorGoal,
            ParallelBatchEngine<String, SkulocRecord, Void> engine) {
        
        // Wire goals with strategies
        Map<Goal, GoalStrategy> strategies = new HashMap<>();
        
        // Adaptive strategies
        strategies.put(perfGoal, new PerformanceStrategy());
        strategies.put(resourceGoal, new ResourceStrategy());
        strategies.put(errorGoal, new ErrorStrategy());
        
        // For static/observable mode, use NoOpStrategy instead:
        // strategies.put(perfGoal, new NoOpStrategy());
        // strategies.put(resourceGoal, new NoOpStrategy());
        // strategies.put(errorGoal, new NoOpStrategy());
        
        List<Goal> goals = Arrays.asList(perfGoal, resourceGoal, errorGoal);
        
        return new ISMRuntimeManager(goals, strategies, engine);
    }
    
    // ============================================
    // Metrics Collector
    // ============================================
    
    @Bean
    public ISMMetricsCollector ismMetricsCollector(
            MeterRegistry meterRegistry,
            @Qualifier("dataSource") HikariDataSource dataSource,
            ParallelBatchEngine<String, SkulocRecord, Void> engine) {
        
        return new ISMMetricsCollector(meterRegistry, dataSource, engine);
    }
}
