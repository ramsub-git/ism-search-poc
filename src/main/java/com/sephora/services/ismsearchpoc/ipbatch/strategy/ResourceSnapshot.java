package com.sephora.services.ismsearchpoc.ipbatch.strategy;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ResourceSnapshot {
    private int availableDbConnections;
    private double availableHeapPercent;
    private int availableCpuCores;
}
