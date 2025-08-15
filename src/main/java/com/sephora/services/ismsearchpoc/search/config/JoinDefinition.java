package com.sephora.services.ismsearchpoc.search.config;

import lombok.*;
import java.util.Map;

/**
 * Definition of a join that can be included in queries.
 * Specifies how to join additional tables for extra columns.
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
public class JoinDefinition {

    /**
     * Type of join.
     * Valid values: "inner", "left", "right"
     */
    @Builder.Default
    private String type = "left";

    /**
     * Table expression to join.
     * Can include alias, e.g., "location_master loc"
     */
    private String table;

    /**
     * Join condition SQL.
     * Example: "loc.location_number = skuloc.location_number"
     */
    private String on;

    /**
     * Additional columns available from this join.
     * Key is the API field name, value is the column definition.
     */
    private Map<String, ColumnDefinition> columns;

    /**
     * Validates the join definition.
     *
     * @throws IllegalStateException if validation fails
     */
    public void validate() {
        if (table == null || table.trim().isEmpty()) {
            throw new IllegalStateException("Join table cannot be null or empty");
        }

        if (on == null || on.trim().isEmpty()) {
            throw new IllegalStateException("Join condition cannot be null or empty");
        }

        if (type == null || (!type.equalsIgnoreCase("inner") &&
                !type.equalsIgnoreCase("left") &&
                !type.equalsIgnoreCase("right"))) {
            throw new IllegalStateException("Join type must be inner, left, or right");
        }

        // Validate columns if present
        if (columns != null) {
            columns.forEach((name, col) -> {
                try {
                    col.validate();
                } catch (Exception e) {
                    throw new IllegalStateException("Invalid join column '" + name + "': " + e.getMessage());
                }
            });
        }
    }

    /**
     * Gets the normalized join type in uppercase.
     *
     * @return normalized type (INNER, LEFT, RIGHT)
     */
    public String getNormalizedType() {
        return type != null ? type.toUpperCase() : "LEFT";
    }
}