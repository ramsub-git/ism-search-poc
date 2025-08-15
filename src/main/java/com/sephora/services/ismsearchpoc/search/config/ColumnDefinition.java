package com.sephora.services.ismsearchpoc.search.config;

import lombok.*;

/**
 * Definition of a column that can be selected from a dataset.
 * Maps API field names to SQL expressions.
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
public class ColumnDefinition {

    /**
     * SQL expression for this column.
     * Can be a simple column reference or complex expression.
     * Example: "skuloc.sku_id" or "COALESCE(qty1, 0)"
     */
    private String sql;

    /**
     * Data type of the column.
     * Valid values: string, int, long, decimal, date, timestamp, boolean
     */
    private String type;

    /**
     * Optional alias for the column in API responses.
     * If not specified, uses the column name from SQL.
     * Example: "skuId" as alias for "sku_id"
     */
    private String alias;

    /**
     * Whether this is a computed field.
     * Computed fields reference entries in the computed fields map.
     */
    @Builder.Default
    private boolean computed = false;

    /**
     * Human-readable description of the column.
     * Used for documentation.
     */
    private String description;

    /**
     * Validates the column definition.
     *
     * @throws IllegalStateException if validation fails
     */
    public void validate() {
        if (!computed && (sql == null || sql.trim().isEmpty())) {
            throw new IllegalStateException("Column SQL cannot be null or empty for non-computed fields");
        }

        if (type == null || type.trim().isEmpty()) {
            throw new IllegalStateException("Column type cannot be null or empty");
        }
    }

    /**
     * Gets the effective name for this column in API responses.
     *
     * @param defaultName default name if alias not specified
     * @return alias if specified, otherwise defaultName
     */
    public String getEffectiveName(String defaultName) {
        return (alias != null && !alias.trim().isEmpty()) ? alias : defaultName;
    }
}