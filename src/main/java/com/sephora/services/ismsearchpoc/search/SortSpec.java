package com.sephora.services.ismsearchpoc.search;

import lombok.*;

/**
 * Specification for sort ordering in search results.
 * Defines a field and its sort direction.
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
@EqualsAndHashCode
public class SortSpec {

    /**
     * Field name to sort by.
     * Must be a valid column or computed field in the dataset.
     */
    private String field;

    /**
     * Sort direction.
     * Valid values: "ASC" (ascending) or "DESC" (descending).
     * Case-insensitive.
     */
    @Builder.Default
    private String direction = "ASC";

    /**
     * Validates the sort specification.
     *
     * @throws IllegalArgumentException if field is null/empty or direction is invalid
     */
    public void validate() {
        if (field == null || field.trim().isEmpty()) {
            throw new IllegalArgumentException("Sort field cannot be null or empty");
        }

        if (direction == null ||
                (!direction.equalsIgnoreCase("ASC") && !direction.equalsIgnoreCase("DESC"))) {
            throw new IllegalArgumentException("Sort direction must be ASC or DESC");
        }
    }

    /**
     * Normalizes the direction to uppercase.
     *
     * @return normalized direction (ASC or DESC)
     */
    public String getNormalizedDirection() {
        return direction != null ? direction.toUpperCase() : "ASC";
    }
}