package com.sephora.services.ismsearchpoc.processing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Auto-generates operational runbook documentation from processing context configuration.
 * <p>
 * This service creates living documentation that stays in sync with code configuration,
 * providing operations teams with comprehensive troubleshooting guides, escalation procedures,
 * and operational queries generated directly from the Business Error Logging Framework setup.
 * <p>
 * The runbook includes:
 * - Complete processing context overview with code traceability
 * - Step guard details with business impact and escalation procedures
 * - Error handling policies with diagnostic and resolution steps
 * - Pre-built operational queries for common troubleshooting scenarios
 * - Team assignments and notification procedures
 * <p>
 * Key benefits:
 * - Always accurate: Generated from live configuration, never outdated
 * - Complete coverage: Every processing context and step guard documented
 * - Actionable: Specific diagnostic steps and resolution procedures
 * - Searchable: Origin markers link directly to source code
 * <p>
 * Example usage:
 * <pre>
 * &#64;Autowired
 * private RunbookGeneratorService runbookGenerator;
 *
 * // Generate complete operational runbook
 * String runbook = runbookGenerator.generateOperationalRunbook();
 *
 * // Export to file, wiki, or operational system
 * Files.writeString(Paths.get("ISM_Processing_Runbook.md"), runbook);
 * </pre>
 * <p>
 * The generated runbook can be exported to:
 * - Markdown files for version control and wiki systems
 * - HTML for internal operational portals
 * - PDF for printable reference guides
 * - JSON for integration with operational dashboards
 *
 * @author ISM Processing Framework
 * @see AppProcessingContext
 * @see ProcessingContextDefinition
 * @see ErrorHandlingPolicy
 * @since 1.0
 */
@Service
@Slf4j
public class RunbookGeneratorService {

    /**
     * Generates complete operational runbook from all registered processing contexts.
     * <p>
     * Creates comprehensive markdown documentation that includes processing context
     * overviews, step guard details, error handling procedures, and operational queries.
     *
     * @return complete runbook in markdown format
     */
    public String generateOperationalRunbook() {
        log.info("Generating operational runbook from {} registered processing contexts",
                AppProcessingContext.getAllContexts().size());

        StringBuilder runbook = new StringBuilder();

        // Generate runbook header
        generateRunbookHeader(runbook);

        // Generate table of contents
        generateTableOfContents(runbook);

        // Generate processing context sections
        AppProcessingContext.getAllContexts().forEach(context -> {
            generateContextSection(runbook, context);
        });

        // Generate operational queries section
        generateOperationalQueriesSection(runbook);

        // Generate escalation procedures
        generateEscalationProcedures(runbook);

        log.info("Generated operational runbook with {} processing contexts",
                AppProcessingContext.getAllContexts().size());

        return runbook.toString();
    }

    /**
     * Generates the runbook header with metadata and overview.
     */
    private void generateRunbookHeader(StringBuilder runbook) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        runbook.append("# ISM Processing Framework - Operational Runbook\n\n");
        runbook.append("**Auto-generated from code configuration**  \n");
        runbook.append("**Generated:** ").append(timestamp).append("  \n");
        runbook.append("**Framework Version:** Business Error Logging Framework v1.0  \n\n");

        runbook.append("## Overview\n\n");
        runbook.append("This runbook provides comprehensive operational guidance for troubleshooting and resolving ");
        runbook.append("processing errors in the ISM system. All information is automatically generated from ");
        runbook.append("the Business Error Logging Framework configuration and stays in sync with code changes.\n\n");

        runbook.append("**Key Features:**\n");
        runbook.append("- **Complete Traceability:** Every error links back to specific source code\n");
        runbook.append("- **Policy-Driven Response:** Escalation levels, teams, and procedures configured per error type\n");
        runbook.append("- **Business Context:** All errors include SKU, location, and other business identifiers\n");
        runbook.append("- **Pre-Built Queries:** Ready-to-use SQL queries for common troubleshooting scenarios\n\n");

        runbook.append("---\n\n");
    }

    /**
     * Generates table of contents with links to all processing contexts.
     */
    private void generateTableOfContents(StringBuilder runbook) {
        runbook.append("## Table of Contents\n\n");

        runbook.append("### Processing Contexts\n");
        AppProcessingContext.getAllContexts().forEach(context -> {
            runbook.append("- [").append(context.getContextName()).append("](#")
                    .append(context.getContextName().toLowerCase().replace("_", "-"))
                    .append(")\n");
        });

        runbook.append("\n### Operational Resources\n");
        runbook.append("- [Common Operational Queries](#common-operational-queries)\n");
        runbook.append("- [Escalation Procedures](#escalation-procedures)\n");
        runbook.append("- [Emergency Contacts](#emergency-contacts)\n\n");

        runbook.append("---\n\n");
    }

    /**
     * Generates detailed section for a specific processing context.
     */
    private void generateContextSection(StringBuilder runbook, ProcessingContextDefinition context) {
        runbook.append("## ").append(context.getContextName()).append("\n\n");

        // Context overview
        generateContextOverview(runbook, context);

        // Step guards
        context.getStepGuards().forEach((guardName, guardDef) -> {
            generateStepGuardSection(runbook, context, guardName, guardDef);
        });

        runbook.append("---\n\n");
    }

    /**
     * Generates overview section for a processing context.
     */
    private void generateContextOverview(StringBuilder runbook, ProcessingContextDefinition context) {
        runbook.append("### Context Overview\n\n");

        runbook.append("**Description:** ").append(context.getDescription() != null ?
                context.getDescription() : "No description provided").append("\n\n");

        runbook.append("**Code Traceability:**\n");
        runbook.append("- **Origin Marker:** `").append(context.getOriginMarker()).append("`\n");
        runbook.append("- **Search Command:** `grep -r \"").append(context.getOriginMarker()).append("\" src/`\n");
        if (context.getInitiatingClass() != null) {
            runbook.append("- **Initiating Class:** ").append(context.getInitiatingClass()).append("\n");
        }
        if (context.getInitiatingMethod() != null) {
            runbook.append("- **Initiating Method:** ").append(context.getInitiatingMethod()).append("\n");
        }
        runbook.append("\n");

        runbook.append("**Processing Details:**\n");
        if (context.getAuditSource() != null) {
            runbook.append("- **Audit Source:** ").append(context.getAuditSource()).append("\n");
        }
        runbook.append("- **Step Guards:** ").append(context.getStepGuards().size()).append(" configured\n");
        runbook.append("\n");
    }

    /**
     * Generates detailed section for a step guard.
     */
    private void generateStepGuardSection(StringBuilder runbook, ProcessingContextDefinition context,
                                          String guardName, StepGuardDefinition<?> guardDef) {
        ErrorHandlingPolicy policy = guardDef.getPolicy();

        runbook.append("### Step Guard: ").append(guardName).append("\n\n");

        // Entity and business context
        runbook.append("**Entity Type:** `").append(guardDef.getEntityType().getSimpleName()).append("`  \n");
        if (!guardDef.getBusinessFieldNames().isEmpty()) {
            runbook.append("**Business Fields:** ").append(String.join(", ", guardDef.getBusinessFieldNames())).append("  \n");
        }
        runbook.append("\n");

        // Error policy details
        runbook.append("**Error Policy:** `").append(policy.getCode()).append("`  \n");
        runbook.append("**Description:** ").append(policy.getDescription()).append("  \n");
        runbook.append("**Business Impact:** ").append(policy.getBusinessImpact()).append("  \n\n");

        // Escalation and team information
        runbook.append("**Response Details:**\n");
        runbook.append("- **Priority Level:** ").append(policy.getEscalationLevel()).append("\n");
        runbook.append("- **Responsible Team:** ").append(policy.getResponsibleTeam()).append("\n");
        runbook.append("- **Auto Retry:** ").append(policy.isAutoRetry() ?
                "Yes (" + policy.getMaxRetries() + " attempts)" : "No").append("\n");
        if (policy.isAutoRetry()) {
            runbook.append("- **Retry Delay:** ").append(policy.getRetryDelay().toMinutes()).append(" minutes\n");
        }
        runbook.append("\n");

        // Immediate action
        runbook.append("**Immediate Action:**  \n");
        runbook.append(policy.getImmediateAction()).append("\n\n");

        // Diagnostic steps if available
        if (policy.getDiagnosticSteps() != null && !policy.getDiagnosticSteps().isEmpty()) {
            runbook.append("**Diagnostic Steps:**\n");
            policy.getDiagnosticSteps().forEach(step -> {
                runbook.append("1. ").append(step).append("\n");
            });
            runbook.append("\n");
        }

        // Resolution steps if available
        if (policy.getResolutionSteps() != null && !policy.getResolutionSteps().isEmpty()) {
            runbook.append("**Resolution Steps:**\n");
            policy.getResolutionSteps().forEach(step -> {
                runbook.append("1. ").append(step).append("\n");
            });
            runbook.append("\n");
        }

        // Step guard specific queries
        generateStepGuardQueries(runbook, context, guardName, guardDef);

        runbook.append("\n");
    }

    /**
     * Generates operational queries specific to a step guard.
     */
    private void generateStepGuardQueries(StringBuilder runbook, ProcessingContextDefinition context,
                                          String guardName, StepGuardDefinition<?> guardDef) {
        runbook.append("**Operational Queries:**\n\n");

        String originMarker = context.getOriginMarker();
        String policyCode = guardDef.getPolicy().getCode();

        // Find all errors for this step guard
        runbook.append("```sql\n");
        runbook.append("-- Find all errors for this step guard in last 24 hours\n");
        runbook.append("SELECT processing_event_id, business_identifier, exception_message, timestamp\n");
        runbook.append("FROM processing_error_log \n");
        runbook.append("WHERE origin_marker = '").append(originMarker).append("'\n");
        runbook.append("  AND error_policy_code = '").append(policyCode).append("'\n");
        runbook.append("  AND timestamp > NOW() - INTERVAL 24 HOUR\n");
        runbook.append("ORDER BY timestamp DESC;\n");
        runbook.append("```\n\n");

        // Business identifier breakdown
        runbook.append("```sql\n");
        runbook.append("-- Business identifier breakdown for this step guard\n");
        runbook.append("SELECT business_identifier, COUNT(*) as error_count, MAX(timestamp) as latest_error\n");
        runbook.append("FROM processing_error_log \n");
        runbook.append("WHERE origin_marker = '").append(originMarker).append("'\n");
        runbook.append("  AND error_policy_code = '").append(policyCode).append("'\n");
        runbook.append("  AND timestamp > NOW() - INTERVAL 24 HOUR\n");
        runbook.append("GROUP BY business_identifier \n");
        runbook.append("ORDER BY error_count DESC;\n");
        runbook.append("```\n\n");
    }

    /**
     * Generates common operational queries section.
     */
    private void generateOperationalQueriesSection(StringBuilder runbook) {
        runbook.append("## Common Operational Queries\n\n");
        runbook.append("These queries work across all processing contexts and provide high-level operational insights.\n\n");

        // Complete processing event story
        runbook.append("### Complete Processing Event Analysis\n\n");
        runbook.append("Use this query to get the complete story for any processing event ID:\n\n");
        runbook.append("```sql\n");
        runbook.append("-- Replace 'YOUR_EVENT_ID_HERE' with actual processing event ID\n");
        runbook.append("WITH event_summary AS (\n");
        runbook.append("    SELECT processing_event_id, identifier, content, processing_outcome, timestamp as audit_timestamp\n");
        runbook.append("    FROM processing_audit_log \n");
        runbook.append("    WHERE processing_event_id = 'YOUR_EVENT_ID_HERE'\n");
        runbook.append("),\n");
        runbook.append("event_errors AS (\n");
        runbook.append("    SELECT business_identifier, error_policy_code, exception_message, \n");
        runbook.append("           responsible_team, timestamp as error_timestamp\n");
        runbook.append("    FROM processing_error_log \n");
        runbook.append("    WHERE processing_event_id = 'YOUR_EVENT_ID_HERE'\n");
        runbook.append(")\n");
        runbook.append("SELECT \n");
        runbook.append("    es.identifier as \"Message Topic\",\n");
        runbook.append("    es.processing_outcome as \"Overall Result\", \n");
        runbook.append("    es.audit_timestamp as \"Processing Time\",\n");
        runbook.append("    COUNT(ee.business_identifier) as \"Error Count\",\n");
        runbook.append("    STRING_AGG(DISTINCT ee.responsible_team, ', ') as \"Teams Involved\",\n");
        runbook.append("    STRING_AGG(ee.business_identifier, '; ') as \"Failed Items\"\n");
        runbook.append("FROM event_summary es\n");
        runbook.append("LEFT JOIN event_errors ee ON 1=1\n");
        runbook.append("GROUP BY es.identifier, es.processing_outcome, es.audit_timestamp;\n");
        runbook.append("```\n\n");

        // Top failing business identifiers
        runbook.append("### Top Failing Business Items\n\n");
        runbook.append("Find the most problematic SKU/location combinations:\n\n");
        runbook.append("```sql\n");
        runbook.append("SELECT \n");
        runbook.append("    business_identifier,\n");
        runbook.append("    origin_marker,\n");
        runbook.append("    error_policy_code,\n");
        runbook.append("    COUNT(*) as error_count,\n");
        runbook.append("    MAX(timestamp) as last_error_time,\n");
        runbook.append("    responsible_team\n");
        runbook.append("FROM processing_error_log \n");
        runbook.append("WHERE timestamp > NOW() - INTERVAL 24 HOUR\n");
        runbook.append("  AND business_identifier IS NOT NULL\n");
        runbook.append("GROUP BY business_identifier, origin_marker, error_policy_code, responsible_team\n");
        runbook.append("ORDER BY error_count DESC\n");
        runbook.append("LIMIT 20;\n");
        runbook.append("```\n\n");

        // P1 escalation errors
        runbook.append("### P1 Escalation Errors (Immediate Attention Required)\n\n");
        runbook.append("Critical errors that require immediate operational response:\n\n");
        runbook.append("```sql\n");
        runbook.append("SELECT \n");
        runbook.append("    origin_marker,\n");
        runbook.append("    business_identifier,\n");
        runbook.append("    error_policy_code,\n");
        runbook.append("    exception_message,\n");
        runbook.append("    responsible_team,\n");
        runbook.append("    timestamp,\n");
        runbook.append("    processing_event_id\n");
        runbook.append("FROM processing_error_log \n");
        runbook.append("WHERE escalation_level = 'P1'\n");
        runbook.append("  AND reprocess_status = 'NEW'\n");
        runbook.append("  AND timestamp > NOW() - INTERVAL 4 HOUR\n");
        runbook.append("ORDER BY timestamp DESC;\n");
        runbook.append("```\n\n");

        // SKU/Location specific investigation
        runbook.append("### Specific SKU/Location Investigation\n\n");
        runbook.append("Find all errors for a specific business item:\n\n");
        runbook.append("```sql\n");
        runbook.append("-- Replace SKU and location values with actual identifiers\n");
        runbook.append("SELECT \n");
        runbook.append("    processing_event_id,\n");
        runbook.append("    origin_marker,\n");
        runbook.append("    error_policy_code,\n");
        runbook.append("    exception_message,\n");
        runbook.append("    escalation_level,\n");
        runbook.append("    responsible_team,\n");
        runbook.append("    timestamp\n");
        runbook.append("FROM processing_error_log \n");
        runbook.append("WHERE business_identifier LIKE '%SKU:12345%'\n");
        runbook.append("  AND business_identifier LIKE '%LOC:456%'\n");
        runbook.append("ORDER BY timestamp DESC\n");
        runbook.append("LIMIT 50;\n");
        runbook.append("```\n\n");
    }

    /**
     * Generates escalation procedures section.
     */
    private void generateEscalationProcedures(StringBuilder runbook) {
        runbook.append("## Escalation Procedures\n\n");

        runbook.append("### P1 - Critical (Response within 15 minutes)\n");
        runbook.append("- **Trigger:** System outages, data corruption, financial discrepancies\n");
        runbook.append("- **Response:** Immediate pager alert to on-call engineer\n");
        runbook.append("- **Action:** Drop everything and investigate immediately\n");
        runbook.append("- **Escalation:** Manager and director notification after 30 minutes\n\n");

        runbook.append("### P2 - High (Response within 1-2 hours)\n");
        runbook.append("- **Trigger:** Functionality impaired but system operational\n");
        runbook.append("- **Response:** Slack notification and email to responsible team\n");
        runbook.append("- **Action:** Investigate during normal working hours or current shift\n");
        runbook.append("- **Escalation:** Manager notification if not resolved within 4 hours\n\n");

        runbook.append("### P3 - Medium (Response within business hours)\n");
        runbook.append("- **Trigger:** Minor issues that don't significantly impact operations\n");
        runbook.append("- **Response:** Email notification to responsible team\n");
        runbook.append("- **Action:** Address during next business day\n");
        runbook.append("- **Escalation:** Weekly review if pattern emerges\n\n");

        runbook.append("## Emergency Contacts\n\n");
        runbook.append("**Data Engineering Team:** data-engineering@company.com  \n");
        runbook.append("**Retail Systems Team:** retail-systems@company.com  \n");
        runbook.append("**Operations Team:** operations@company.com  \n");
        runbook.append("**Fulfillment Team:** fulfillment@company.com  \n\n");

        runbook.append("**On-Call Escalation:** +1-XXX-XXX-XXXX  \n");
        runbook.append("**Manager Escalation:** manager@company.com  \n\n");

        runbook.append("---\n\n");
        runbook.append("*This runbook is automatically generated from the Business Error Logging Framework configuration. ");
        runbook.append("To update procedures or add new contexts, modify the ProcessingContextConfiguration class and redeploy.*\n");
    }
}