package com.sephora.services.ismsearchpoc.ipbatch.sizing;

import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;

@FunctionalInterface
public interface RecordCounter {
    long count(ExecutionContext context);
}
