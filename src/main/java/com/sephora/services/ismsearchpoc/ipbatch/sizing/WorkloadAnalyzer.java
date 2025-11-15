package com.sephora.services.ismsearchpoc.ipbatch.sizing;

import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import org.springframework.stereotype.Component;

@Component
public class WorkloadAnalyzer {
    
    public WorkloadAnalysis analyze(
            SizingStrategy strategy,
            int workItemCount,
            Long estimatedRecordsPerItem,
            RecordCounter recordCounter,
            ExecutionContext context) {
        
        long totalRecords = switch (strategy) {
            case STATIC -> -1;
            case ESTIMATED -> {
                if (estimatedRecordsPerItem == null) {
                    throw new IllegalStateException("ESTIMATED sizing requires estimatedRecordsPerItem");
                }
                yield workItemCount * estimatedRecordsPerItem;
            }
            case DYNAMIC -> {
                if (recordCounter == null) {
                    throw new IllegalStateException("DYNAMIC sizing requires RecordCounter");
                }
                yield recordCounter.count(context);
            }
        };
        
        long averagePerItem = totalRecords > 0 ? totalRecords / workItemCount : -1;
        
        return WorkloadAnalysis.builder()
            .workItemCount(workItemCount)
            .totalRecords(totalRecords)
            .averageRecordsPerItem(averagePerItem)
            .build();
    }
}
