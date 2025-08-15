package com.sephora.services.ismsearchpoc.search.config;

import lombok.*;
import java.util.List;

/**
 * Definition of a filter that can be applied to a dataset or view.
 * Specifies how a user-facing filter parameter maps to database columns.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class FilterDefinition {

    /**
     * Database column or expression to filter on.
     * Example: "skuloc.location_number"
     */
    private String column;

    /**
     * Data type of the filter.
     * Valid values: string, int, long, decimal, date, timestamp, boolean
     * Arrays indicated by [] suffix (e.g., "int[]")
     */
    private String type;

    /**
     * SQL operator to use.
     * Valid values: "=", "!=", ">", ">=", "<", "<=", "IN", "NOT IN", "LIKE", "BETWEEN"
     * Default: "=" for single values, "IN" for arrays
     */
    @Builder.Default
    private String op = "=";

    /**
     * Maximum number of values allowed for IN clauses.
     * Prevents excessive query size.
     */
    @Builder.Default
    private Integer max = 5000;

    /**
     * Whether this filter is required.
     * If true, requests without this filter will be rejected.
     */
    @Builder.Default
    private boolean required = false;

    /**
     * Allowed values for enum-style filters.
     * If specified, only these values are accepted.
     */
    private List<String> allowed;

    /**
     * Human-readable description of the filter.
     * Used for documentation and error messages.
     */
    private String description;

    /**
     * Validates the filter definition.
     *
     * @throws IllegalStateException if validation fails
     */
    public void validate() {
        if (column == null || column.trim().isEmpty()) {
            throw new IllegalStateException("Filter column cannot be null or empty");
        }

        if (type == null || type.trim().isEmpty()) {
            throw new IllegalStateException("Filter type cannot be null or empty");
        }

        if (op == null || op.trim().isEmpty()) {
            throw new IllegalStateException("Filter operator cannot be null or empty");
        }

        if (max != null && max <= 0) {
            throw new IllegalStateException("Filter max must be positive");
        }
    }

    /**
     * Checks if this filter accepts array values.
     *
     * @return true if type ends with []
     */
    public boolean isArrayType() {
        return type != null && type.endsWith("[]");
    }

    /**
     * Gets the base type without array suffix.
     *
     * @return base type (e.g., "int" for "int[]")
     */
    public String getBaseType() {
        if (isArrayType()) {
            return type.substring(0, type.length() - 2);
        }
        return type;
    }
}