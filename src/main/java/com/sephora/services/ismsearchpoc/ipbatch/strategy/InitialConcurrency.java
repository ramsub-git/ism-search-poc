package com.sephora.services.ismsearchpoc.ipbatch.strategy;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class InitialConcurrency {
    private int workItemConcurrency;
    private int processingConcurrency;
    private String rationale;
}
