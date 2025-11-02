package com.sephora.services.ismsearchpoc.ism.mapper;

import com.sephora.services.ismsearchpoc.framework.utility.mapper.RecordMapper;
import com.sephora.services.ismsearchpoc.ism.model.SkulocRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Maps CSV fields to SkulocRecord
 * ISM-specific field mapping logic
 * 
 * CSV Format (10 fields):
 * Sku, Loc, Avail, MR, Lost, Curr, WACLoc, WACAlt, StDt, EndDt
 */
@Component
public class SkulocRecordMapper implements RecordMapper<SkulocRecord> {
    
    private static final Logger log = LoggerFactory.getLogger(SkulocRecordMapper.class);
    private static final int EXPECTED_FIELDS = 10;
    
    @Override
    public SkulocRecord map(String[] fields) throws Exception {
        // Validate field count
        if (fields.length != EXPECTED_FIELDS) {
            throw new IllegalArgumentException(
                String.format("Expected %d fields, got %d", EXPECTED_FIELDS, fields.length));
        }
        
        try {
            return SkulocRecord.builder()
                .skuId(parseLong(fields[0], "skuId"))
                .locationNumber(parseInt(fields[1], "locationNumber"))
                .availableQty(parseLong(fields[2], "availableQty"))
                .merchReserveQty(parseLong(fields[3], "merchReserveQty"))
                .lostFoundQty(parseLong(fields[4], "lostFoundQty"))
                .currencyCode(parseString(fields[5], "currencyCode"))
                .wacInBase(parseBigDecimal(fields[6], "wacInBase"))
                .wacInAlternate(parseBigDecimal(fields[7], "wacInAlternate"))
                .startDate(parseDate(fields[8], "startDate"))
                .endDate(parseDate(fields[9], "endDate"))
                .build();
                
        } catch (Exception e) {
            log.error("Error mapping fields to SkulocRecord: {}", String.join(",", fields), e);
            throw new Exception("Failed to map CSV fields to SkulocRecord", e);
        }
    }
    
    private Long parseLong(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format("Invalid %s: '%s'", fieldName, value), e);
        }
    }
    
    private Integer parseInt(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format("Invalid %s: '%s'", fieldName, value), e);
        }
    }
    
    private BigDecimal parseBigDecimal(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return new BigDecimal(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format("Invalid %s: '%s'", fieldName, value), e);
        }
    }
    
    private LocalDate parseDate(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            // Expected format: yyyy-mm-dd
            return LocalDate.parse(value.trim());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Invalid %s: '%s' (expected yyyy-mm-dd)", fieldName, value), e);
        }
    }
    
    private String parseString(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }
}
