package com.sephora.services.ismsearchpoc.processing;

import com.sephora.services.ismsearchpoc.processing.RunbookGeneratorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Automatically generates operational runbook on application startup.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RunbookAutoGenerator {

    private final RunbookGeneratorService runbookGenerator;

    @EventListener(ApplicationReadyEvent.class)
    public void generateRunbookOnStartup() {
        try {
            log.info("Generating operational runbook...");

            String runbook = runbookGenerator.generateOperationalRunbook();

            // Option A: Just log it
            log.info("Operational Runbook:\n{}", runbook);

            // Option B: Save to file (useful for containerized deployments)
            // Path runbookPath = Paths.get("docs/ISM_Supply_Runbook.md");
            // Files.createDirectories(runbookPath.getParent());
            // Files.writeString(runbookPath, runbook);
            // log.info("Runbook saved to: {}", runbookPath.toAbsolutePath());

            // Option C: Publish to S3, wiki, or documentation system
            // publishToWiki(runbook);

        } catch (Exception e) {
            log.error("Failed to generate runbook", e);
        }
    }
}