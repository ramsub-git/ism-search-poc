package com.sephora.services.ismsearchpoc.ipbatch.worker;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.ism.model.SkulocRecord;
import com.sephora.services.ismsearchpoc.processing.ContextualStepGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Generic blob file fetcher for Azure Blob Storage
 * Reusable for any blob-based data load
 */
public class BlobFileFetcher implements WorkItemFetcher<String> {

    private static final Logger log = LoggerFactory.getLogger(BlobFileFetcher.class);
    @Autowired
    @Qualifier("FILE_FETCH")
    private ContextualStepGuard<SkulocRecord> fileFetchGuard;
    private final BlobContainerClient blobClient;
    
    public BlobFileFetcher(BlobContainerClient blobClient) {
        this.blobClient = blobClient;
    }

    @Override
    public List<String> fetchWorkItems(ExecutionContext context) throws Exception {
        return fileFetchGuard.execute(
                () -> {
                    try {
                        return guardedFetchWorkItems(context);
                    } catch (Exception e) {
                        // wrap as unchecked so the lambda doesn't throw a checked exception
                        throw new RuntimeException("Error fetching work items", e);
                    }
                },
                ""
        );
    }

    public List<String> guardedFetchWorkItems(ExecutionContext context) throws Exception {
        String folderPath = (String) context.getAttribute("folderPath");
        String filePattern = (String) context.getAttribute("filePattern"); // Optional

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
