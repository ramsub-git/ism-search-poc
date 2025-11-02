package com.sephora.services.ismsearchpoc.framework.worker;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.sephora.services.ismsearchpoc.framework.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.framework.utility.mapper.RecordMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic CSV file reader for blob storage
 * Reusable for any CSV-based data load
 * 
 * @param <R> Record type to read into
 */
public class CsvFileReader<R> implements WorkItemReader<String, R> {
    
    private static final Logger log = LoggerFactory.getLogger(CsvFileReader.class);
    
    private final BlobContainerClient blobClient;
    private final RecordMapper<R> recordMapper;
    private final boolean skipHeader;
    private final String delimiter;
    
    /**
     * Constructor with default settings (skip header, comma delimiter)
     */
    public CsvFileReader(BlobContainerClient blobClient, RecordMapper<R> recordMapper) {
        this(blobClient, recordMapper, true, ",");
    }
    
    /**
     * Constructor with custom settings
     */
    public CsvFileReader(BlobContainerClient blobClient, 
                        RecordMapper<R> recordMapper,
                        boolean skipHeader,
                        String delimiter) {
        this.blobClient = blobClient;
        this.recordMapper = recordMapper;
        this.skipHeader = skipHeader;
        this.delimiter = delimiter;
    }
    
    @Override
    public List<R> readWorkItem(String fileName, ExecutionContext context) throws Exception {
        log.debug("Reading CSV file: {}", fileName);
        
        BlobClient blobClient = this.blobClient.getBlobClient(fileName);
        
        List<R> records = new ArrayList<>();
        
        try (InputStream inputStream = blobClient.openInputStream();
             InputStreamReader isr = new InputStreamReader(inputStream);
             BufferedReader reader = new BufferedReader(isr)) {
            
            String line;
            int lineNumber = 0;
            
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                
                // Skip header if configured
                if (lineNumber == 1 && skipHeader) {
                    continue;
                }
                
                // Skip empty lines
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                try {
                    // Split and map
                    String[] fields = line.split(delimiter, -1); // -1 to preserve trailing empty strings
                    R record = recordMapper.map(fields);
                    records.add(record);
                    
                } catch (Exception e) {
                    log.error("Error parsing line {} in file {}: {}", lineNumber, fileName, line, e);
                    throw new Exception("Failed to parse line " + lineNumber + " in " + fileName, e);
                }
            }
        }
        
        log.info("Read {} records from {}", records.size(), fileName);
        return records;
    }
}
