package com.sephora.services.ismsearchpoc.search;

import lombok.*;
import java.util.List;
import java.util.Map;

/**
 * Search request for querying datasets.
 * Supports two modes:
 * 1. View mode - Simple preset queries using view name
 * 2. Ad-hoc mode - Advanced queries with explicit column/filter selection
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
public class SearchRequest {

    // ===== View Mode Fields =====

    /**
     * Name of the preset view to use.
     * When specified, view configuration determines columns, filters, and defaults.
     * Mutually exclusive with ad-hoc mode fields.
     */
    private String view;

    // ===== Common Fields =====

    /**
     * Filter criteria as key-value pairs.
     * Keys must match allowed filter names for the view or dataset.
     * Values can be single values or arrays for IN clauses.
     */
    private Map<String, Object> filters;

    /**
     * Whether to paginate results.
     * When false, returns all results up to safety cap.
     * When true, returns one page of results with navigation info.
     */
    @Builder.Default
    private boolean paginate = true;

    /**
     * Page size when paginate=true.
     * Subject to dataset's maxPageSize limit.
     */
    private Integer size;

    /**
     * Continuation cursor for KEYSET pagination.
     * Obtained from previous response's nextCursor.
     */
    private String cursor;

    /**
     * Page number for OFFSET pagination.
     * Zero-based. Only used when paginationMode=OFFSET.
     */
    private Integer page;

    /**
     * Pagination mode preference.
     * Defaults to KEYSET for better performance.
     */
    @Builder.Default
    private PaginationMode paginationMode = PaginationMode.KEYSET;

    /**
     * Whether to include total count in response.
     * May impact performance on large datasets.
     */
    @Builder.Default
    private boolean includeTotal = false;

    // ===== Ad-hoc Mode Fields =====

    /**
     * Explicit column selection for ad-hoc queries.
     * When specified, indicates ad-hoc mode.
     */
    private List<String> columns;

    /**
     * Sort specification for ad-hoc queries.
     * Overrides default sort from view or dataset.
     */
    private List<SortSpec> sort;

    /**
     * Inline computed field definitions for ad-hoc queries.
     * Map of field name to SQL expression.
     */
    private Map<String, String> computed;

    /**
     * Determines if this is a view-based or ad-hoc request.
     *
     * @return true if view mode, false if ad-hoc mode
     */
    public boolean isViewMode() {
        return view != null && !view.trim().isEmpty();
    }

    /**
     * Validates the request for consistency.
     *
     * @throws IllegalArgumentException if validation fails
     */
    public void validate() {
        // Check mode exclusivity
        if (isViewMode() && (columns != null || computed != null)) {
            throw new IllegalArgumentException(
                    "Cannot specify both view and ad-hoc fields (columns/computed)");
        }

        if (!isViewMode() && columns == null) {
            throw new IllegalArgumentException(
                    "Must specify either view name or columns for ad-hoc query");
        }

        // Validate pagination settings
        if (paginate) {
            if (size == null || size <= 0) {
                throw new IllegalArgumentException("Page size must be positive when paginate=true");
            }

            if (paginationMode == PaginationMode.OFFSET) {
                if (cursor != null) {
                    throw new IllegalArgumentException(
                            "Cannot use cursor with OFFSET pagination mode");
                }
            } else { // KEYSET
                if (page != null) {
                    throw new IllegalArgumentException(
                            "Cannot use page number with KEYSET pagination mode");
                }
            }
        }

        // Validate sort specs
        if (sort != null) {
            for (SortSpec s : sort) {
                s.validate();
            }
        }
    }
}