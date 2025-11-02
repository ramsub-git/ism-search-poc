package com.sephora.services.ismsearchpoc.framework.worker;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.sephora.services.ismsearchpoc.framework.model.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generic blob file fetcher for Azure Blob Storage
 * Reusable for any blob-based data load
 */
public class BlobFileFetcher implements WorkItemFetcher<String> {
    
    private static final Logger log = LoggerFactory.getLogger(BlobFileFetcher.class);
    
    private final BlobContainerClient blobClient;
    
    public BlobFileFetcher(BlobContainerClient blobClient) {
        this.blobClient = blobClient;
    }
    
    @Override
    public List<String> fetchWorkItems(ExecutionContext context) throws Exception {
        String folderPath = context.get("folderPath");
        String filePattern = context.get("filePattern"); // Optional
        
        log.info("Fetching blobs from folder: {}", folderPath);
        
        List<String> fileNames = new ArrayList<>();
        
        for (BlobItem blob : blobClient.listBlobs()) {
            String blobName = blob.getName();
            
            // Filter by folder path
            if (folderPath != null && !blobName.startsWith(folderPath)) {
                continue;
            }
            
            // Filter by pattern if provided
            if (filePattern != null && !blobName.matches(filePattern)) {
                continue;
            }
            
            fileNames.add(blobName);
        }
        
        log.info("Found {} blob files", fileNames.size());
        return fileNames;
    }
}
