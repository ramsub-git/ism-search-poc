package com.sephora.services.ismsearchpoc.ipbatch.runtime;

import com.sephora.services.ismsearchpoc.ipbatch.concurrency.ConcurrencyController;
import com.sephora.services.ismsearchpoc.ipbatch.concurrency.SpringThreadPoolConcurrencyController;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.InitialConcurrency;
import com.sephora.services.ismsearchpoc.ipbatch.core.BatchDefinition;
import com.sephora.services.ismsearchpoc.ipbatch.core.BatchStepDefinition;
import com.sephora.services.ismsearchpoc.ipbatch.engine.ParallelBatchEngine;
import com.sephora.services.ismsearchpoc.ipbatch.model.ProcessingResult;
import com.sephora.services.ismsearchpoc.ipbatch.worker.BatchProcessor;
import com.sephora.services.ismsearchpoc.ipbatch.worker.ProgressTracker;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemFetcher;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemReader;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BatchEngineFactory {

    private static final int DEFAULT_BATCH_SIZE = 1000;  // Default batch size

    @SuppressWarnings("unchecked")
    public <T, R, V> ParallelBatchEngine<T, R, V> createEngine(
            BatchDefinition batch,
            BatchStepDefinition step,
            InitialConcurrency concurrency) {

        ConcurrencyController controller = new SpringThreadPoolConcurrencyController(
                concurrency.getWorkItemConcurrency(),
                concurrency.getProcessingConcurrency(),
                batch.getConcurrencyLimits().getMaxWorkItemConcurrency(),
                batch.getConcurrencyLimits().getMaxProcessingConcurrency()
        );

        WorkItemFetcher<T> fetcher = (WorkItemFetcher<T>) step.getWorkItemFetcher();
        WorkItemReader<T, R> reader = (WorkItemReader<T, R>) step.getWorkItemReader();
        BatchProcessor<R, V> processor = (BatchProcessor<R, V>) step.getBatchProcessor();

        ProgressTracker<T, V> tracker = new SimpleProgressTracker<>();

        return new ParallelBatchEngine<>(
                controller,
                fetcher,
                reader,
                processor,
                tracker,
                DEFAULT_BATCH_SIZE  // <-- 6th parameter: batch size
        );
    }

    private static class SimpleProgressTracker<T, V> implements ProgressTracker<T, V> {

        @Override
        public void onStart(int totalWorkItems) {
        }

        @Override
        public void onWorkItemStart(T workItem) {
        }

        @Override
        public void onWorkItemComplete(T workItem, int recordsProcessed, List<ProcessingResult<V>> results) {
        }

        @Override
        public void onWorkItemFailure(T workItem, Throwable error) {
        }

        @Override
        public void reportProgress(int itemsProcessed, int totalItems) {
        }

        @Override
        public void onComplete(int itemsProcessed, long recordsProcessed, int totalErrors) {
        }
    }
}