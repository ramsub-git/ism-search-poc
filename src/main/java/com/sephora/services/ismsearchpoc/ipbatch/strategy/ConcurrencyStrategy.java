package com.sephora.services.ismsearchpoc.ipbatch.strategy;

import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadAnalysis;

public interface ConcurrencyStrategy {
    InitialConcurrency calculate(
        WorkloadAnalysis workload,
        ConcurrencyLimits limits,
        ResourceSnapshot resources
    );
}
