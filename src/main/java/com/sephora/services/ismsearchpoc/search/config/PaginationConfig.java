package com.sephora.services.ismsearchpoc.search.config;

import lombok.*;
import java.util.List;

/**
 * Pagination configuration for a dataset.
 * Defines keyset columns, size limits, and defaults.
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
public class PaginationConfig {

    /**
     * Column names that form the unique key for keyset pagination.
     * Must provide deterministic ordering.
     * Example: ["sku_id", "location_number"]
     */
    private List<String> keysetColumns;

    /**
     * Default page size when not specified in request.
     */
    @Builder.Default
    private int defaultPageSize = 50;

    /**
     * Maximum allowed page size for paginated requests.
     */
    @Builder.Default
    private int maxPageSize = 500;

    /**
     * Maximum rows returned for unpaginated requests.
     * Acts as safety cap to prevent memory issues.
     */
    @Builder.Default
    private int maxUnpaginatedRows = 10000;

    /**
     * Whether to allow OFFSET pagination mode.
     * May be disabled for performance reasons on large tables.
     */
    @Builder.Default
    private boolean allowOffset = true;

    /**
     * Validates the pagination configuration.
     *
     * @throws IllegalStateException if validation fails
     */
    public void validate() {
        if (keysetColumns == null || keysetColumns.isEmpty()) {
            throw new IllegalStateException("Keyset columns cannot be null or empty");
        }

        if (defaultPageSize <= 0) {
            throw new IllegalStateException("Default page size must be positive");
        }

        if (maxPageSize <= 0) {
            throw new IllegalStateException("Max page size must be positive");
        }

        if (maxPageSize < defaultPageSize) {
            throw new IllegalStateException("Max page size cannot be less than default page size");
        }

        if (maxUnpaginatedRows <= 0) {
            throw new IllegalStateException("Max unpaginated rows must be positive");
        }
    }

    /**
     * Gets the effective page size considering request and limits.
     *
     * @param requested requested page size (may be null)
     * @return effective page size
     */
    public int getEffectivePageSize(Integer requested) {
        if (requested == null) {
            return defaultPageSize;
        }
        return Math.min(requested, maxPageSize);
    }
}