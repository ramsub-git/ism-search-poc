package com.sephora.services.ismsearchpoc.ipbatch.sizing;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class WorkloadAnalysis {
    private int workItemCount;
    private long totalRecords;
    private long averageRecordsPerItem;
    
    public WorkloadSize categorize() {
        if (workItemCount < 50) return WorkloadSize.SMALL;
        if (workItemCount < 500) return WorkloadSize.MEDIUM;
        return WorkloadSize.LARGE;
    }
}
