package com.sephora.services.ismsearchpoc.ipbatch.utility.mapper;

/**
 * Maps CSV fields to domain records
 * ISM implementations provide field-to-record mapping logic
 * 
 * @param <R> Record type to map to
 */
public interface RecordMapper<R> {
    
    /**
     * Map CSV fields to a record
     * @param fields Array of CSV field values
     * @return Mapped record
     * @throws Exception if mapping fails (validation, parsing, etc.)
     */
    R map(String[] fields) throws Exception;
}
