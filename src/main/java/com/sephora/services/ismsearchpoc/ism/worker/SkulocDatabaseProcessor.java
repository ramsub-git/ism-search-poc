package com.sephora.services.ismsearchpoc.ism.worker;

import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.ipbatch.model.ProcessingResult;
import com.sephora.services.ismsearchpoc.ipbatch.worker.BatchProcessor;
import com.sephora.services.ismsearchpoc.ism.model.SkulocRecord;
import com.sephora.services.ismsearchpoc.processing.ContextualStepGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * ISM-specific batch processor for Skuloc records
 * Handles validation and persistence to database
 */
@Component
public class SkulocDatabaseProcessor implements BatchProcessor<SkulocRecord, Void> {
    @Autowired
    @Qualifier("BATCH_PROCESS")
    private ContextualStepGuard<SkulocRecord> batchProcessGuard;

    private static final Logger log = LoggerFactory.getLogger(SkulocDatabaseProcessor.class);
    
    // TODO: Inject these once we have them
    // private final SkulocRepository repository;
    // private final LocationLookupService locationService;
    // private final ReserveCalcService reserveCalcService;
    
    public List<ProcessingResult<Void>> guardProcessBatch(List<SkulocRecord> records, ExecutionContext context)
            throws Exception {
        
        log.debug("Processing batch of {} skuloc records", records.size());
        
        List<ProcessingResult<Void>> results = new ArrayList<>();
        
        for (SkulocRecord record : records) {
            try {
                // Validate
                validateRecord(record);
                
                // Persist (mock for now - will implement with repository)
                persistRecord(record);
                
                // Post-process if needed (comingle, reserve calc, etc.)
                postProcessRecord(record);
                
                // Success
                results.add(ProcessingResult.success(null));
                
            } catch (Exception e) {
                log.error("Error processing skuloc record: {}", record, e);
                results.add(ProcessingResult.failure(e));
            }
        }
        
        return results;
    }

    public List<ProcessingResult<Void>> processBatch(List<SkulocRecord> records, ExecutionContext context) {
        return batchProcessGuard.execute(
                () -> {
                    try {
                        return guardProcessBatch(records, context);
                    } catch (Exception e) {
                        // wrap as unchecked so the lambda doesn't throw a checked exception
                        throw new RuntimeException("Error processBatch", e);
                    }
                },
                ""
        );
    }

    /**
     * Validate a skuloc record
     */
    private void validateRecord(SkulocRecord record) throws Exception {
        // Required fields
        if (record.getSkuId() == null) {
            throw new IllegalArgumentException("Missing required field: skuId");
        }
        
        if (record.getLocationNumber() == null) {
            throw new IllegalArgumentException("Missing required field: locationNumber");
        }
        
        // Business validations
        if (record.getSkuId() <= 0) {
            throw new IllegalArgumentException("Invalid skuId: " + record.getSkuId());
        }
        
        if (record.getLocationNumber() <= 0) {
            throw new IllegalArgumentException("Invalid locationNumber: " + record.getLocationNumber());
        }
        
        // Currency validation
        if (record.getCurrencyCode() != null) {
            String currency = record.getCurrencyCode().toUpperCase();
            if (!currency.equals("USD") && !currency.equals("CAD")) {
                throw new IllegalArgumentException("Invalid currency: " + record.getCurrencyCode());
            }
        }
        
        // TODO: Location existence check
        // if (!locationService.locationExists(record.getLocationNumber())) {
        //     throw new IllegalArgumentException("Invalid location: " + record.getLocationNumber());
        // }
        
        log.trace("Validation passed for record: sku={}, loc={}", 
            record.getSkuId(), record.getLocationNumber());
    }
    
    /**
     * Persist record to database
     */
    private void persistRecord(SkulocRecord record) throws Exception {
        // TODO: Implement actual persistence
        // 1. Convert SkulocRecord to Skuloc entity
        // 2. Use repository.save() or batch insert
        // 3. Handle insert/update logic
        
        log.trace("Persisting record: sku={}, loc={}, avail={}", 
            record.getSkuId(), record.getLocationNumber(), record.getAvailableQty());
        
        // Mock implementation for now
        // In real implementation:
        // Skuloc entity = toEntity(record);
        // repository.save(entity);
    }
    
    /**
     * Post-processing (comingle, reserve calc, events, etc.)
     */
    private void postProcessRecord(SkulocRecord record) {
        // TODO: Implement post-processing
        // 1. Check if location is comingle
        // 2. Trigger reserve calculation if needed
        // 3. Publish events if configured
        
        log.trace("Post-processing record: sku={}, loc={}", 
            record.getSkuId(), record.getLocationNumber());
        
        // Mock implementation for now
        // if (locationService.isComingleLocation(record.getLocationNumber())) {
        //     reserveCalcService.calculateReserves(record.getSkuId(), record.getLocationNumber());
        // }
    }
}
