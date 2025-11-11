package com.sephora.services.ismsearchpoc.processing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark fields that contain business identifiers for error logging context.
 * <p>
 * This annotation enables automatic extraction of business context from entities
 * during error logging, providing operational teams with the specific business
 * identifiers (SKU, location, order, etc.) related to processing failures.
 * <p>
 * The annotation value specifies the business identifier type, which is used
 * to build searchable business identifier strings in error logs.
 * <p>
 * Example usage:
 * <pre>
 * public class PosTransactionLine {
 *     &#64;BusinessID("SKU")
 *     private Long skuId;
 *
 *     &#64;BusinessID("LOC")
 *     private Integer storeNumber;
 *
 *     &#64;BusinessID("TRANSACTION")
 *     private String transactionId;
 *
 *     // Standard getters/setters - no additional methods required
 * }
 * </pre>
 * <p>
 * This results in error log entries with business_identifier values like:
 * "SKU:12345;LOC:456;TRANSACTION:TXN001"
 * <p>
 * Supported identifier types (by convention):
 * - "SKU" - Stock Keeping Unit identifier
 * - "LOC" - Location/Store number
 * - "ORDER" - Order identifier
 * - "SHIPMENT" - Shipment identifier
 * - "TRANSACTION" - Transaction identifier
 * - "RETURN" - Return identifier
 * - Custom types as needed for specific business domains
 * <p>
 * Performance characteristics:
 * - Reflection is performed ONCE during application startup via StepGuardDefinition.preInitialize()
 * - Runtime field extraction uses cached Function instances for optimal performance
 * - Zero reflection overhead during business processing operations
 *
 * @author ISM Processing Framework
 * @see StepGuardDefinition#preInitialize()
 * @see AppProcessingContext
 * @since 1.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface BusinessID {
    /**
     * The business identifier type (e.g., "sku", "location", "order").
     * Convention is lowercase for consistency in the formatted output.
     */
    String value();
}