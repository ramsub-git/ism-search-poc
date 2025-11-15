package com.sephora.services.ismsearchpoc.ipbatch.strategy;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ConcurrencyLimits {
    private int minWorkItemConcurrency;
    private int maxWorkItemConcurrency;
    private int minProcessingConcurrency;
    private int maxProcessingConcurrency;
}
