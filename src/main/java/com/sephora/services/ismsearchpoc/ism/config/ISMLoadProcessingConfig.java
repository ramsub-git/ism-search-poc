package com.sephora.services.ismsearchpoc.ism.config;

import com.sephora.services.ismsearchpoc.processing.ProcessingContextConfigBase;
import com.sephora.services.ismsearchpoc.processing.ErrorHandlingPolicy;
import com.sephora.services.ismsearchpoc.ism.model.SkulocRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Slf4j
@Configuration
public class ISMLoadProcessingConfig extends ProcessingContextConfigBase {

    @Override
    protected void configureContexts() {
        // Main orchestration context
        context("SKULOC_DATA_LOAD")
                .withOriginMarker("ISM_INITIAL_LOAD")
                .withAuditSource("skuloc-batch-load")
                .withDescription("Skuloc initial data load orchestration")
                .withStepGuard("FILE_FETCH", SkulocRecord.class, fileFetchPolicy())
                .withStepGuard("CSV_PARSE", SkulocRecord.class, csvParsePolicy())
                .withStepGuard("BATCH_PROCESS", SkulocRecord.class, batchProcessPolicy())
                .withStepGuard("RUNTIME_ADJUST", SkulocRecord.class, runtimeAdjustPolicy())
                .register();
    }

    private ErrorHandlingPolicy fileFetchPolicy() {
        return ErrorHandlingPolicy.builder()
                .code("FILE_FETCH_ERROR")
                .description("Error fetching file from Azure Blob Storage")
                .businessImpact("HIGH - Blocks entire location load")
                .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P2)
                .responsibleTeam("ISM Data Team")
                .immediateAction("Check Azure blob connectivity and file permissions")
                .autoRetry(true)
                .maxRetries(3)
                .retryDelay(Duration.ofMillis(5000))
                .build();
    }

    private ErrorHandlingPolicy csvParsePolicy() {
        return ErrorHandlingPolicy.builder()
                .code("CSV_PARSE_ERROR")
                .description("Error parsing CSV file")
                .businessImpact("HIGH - Invalid data format blocks location load")
                .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P2)
                .responsibleTeam("ISM Data Team")
                .immediateAction("Validate CSV format and schema")
                .autoRetry(false)
                .build();
    }

    private ErrorHandlingPolicy batchProcessPolicy() {
        return ErrorHandlingPolicy.builder()
                .code("BATCH_PROCESS_ERROR")
                .description("Error during database batch insert/update")
                .businessImpact("CRITICAL - Data not persisted")
                .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P1)
                .responsibleTeam("ISM Data Team")
                .immediateAction("Check database connectivity and constraints")
                .autoRetry(true)
                .maxRetries(2)
                .retryDelay(Duration.ofMillis(5000))
                .build();
    }

    private ErrorHandlingPolicy runtimeAdjustPolicy() {
        return ErrorHandlingPolicy.builder()
                .code("RUNTIME_ADJUST_ERROR")
                .description("Error adjusting runtime concurrency")
                .businessImpact("MEDIUM - Performance degradation but processing continues")
                .escalationLevel(ErrorHandlingPolicy.EscalationLevel.P3)
                .responsibleTeam("ISM Data Team")
                .immediateAction("Review runtime manager metrics")
                .autoRetry(false)
                .build();
    }
}