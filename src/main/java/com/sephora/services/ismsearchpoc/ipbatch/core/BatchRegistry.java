package com.sephora.services.ismsearchpoc.ipbatch.core;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Slf4j
public class BatchRegistry {
    
    private static final Map<String, BatchDefinition> batches = new ConcurrentHashMap<>();
    
    public static void register(BatchDefinition definition) {
        batches.put(definition.getBatchName(), definition);
        log.info("Registered batch: {}", definition.getBatchName());
    }
    
    public static BatchDefinition get(String batchName) {
        BatchDefinition definition = batches.get(batchName);
        if (definition == null) {
            throw new IllegalArgumentException("Batch not found: " + batchName);
        }
        return definition;
    }
    
    public static boolean exists(String batchName) {
        return batches.containsKey(batchName);
    }
    
    public static void clear() {
        batches.clear();
    }
}
