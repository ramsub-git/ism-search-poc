package com.sephora.services.ismsearchpoc.search.service;

import com.sephora.services.ismsearchpoc.search.PaginationMode;
import com.sephora.services.ismsearchpoc.search.SortSpec;
import com.sephora.services.ismsearchpoc.search.config.DatasetDefinition;
import com.sephora.services.ismsearchpoc.search.config.ViewDefinition;
import lombok.*;

import java.util.List;
import java.util.Map;

/**
 * Internal context object containing all resolved parameters for a search.
 * Created by SearchService after validating and merging request with configuration.
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
public class SearchContext {

    /**
     * Dataset definition containing all configuration.
     */
    private DatasetDefinition dataset;

    /**
     * View definition if using view mode.
     */
    private ViewDefinition view;

    /**
     * View name for metadata.
     */
    private String viewName;

    /**
     * Whether this is an ad-hoc query.
     */
    @Builder.Default
    private boolean adhoc = false;

    /**
     * Columns to select (resolved from view or request).
     */
    private List<String> columns;

    /**
     * Filters to apply (validated).
     */
    private Map<String, Object> filters;

    /**
     * Sort specification (resolved from view, request, or dataset default).
     */
    private List<SortSpec> sort;

    /**
     * Join names to include.
     */
    private List<String> includes;

    /**
     * Inline computed fields for ad-hoc mode.
     */
    private Map<String, String> inlineComputed;

    /**
     * Whether results should be paginated.
     */
    @Builder.Default
    private boolean paginated = true;

    /**
     * Page size (validated against limits).
     */
    private int pageSize;

    /**
     * Pagination mode.
     */
    @Builder.Default
    private PaginationMode paginationMode = PaginationMode.KEYSET;

    /**
     * Cursor for KEYSET pagination.
     */
    private String cursor;

    /**
     * Page number for OFFSET pagination.
     */
    private Integer pageNumber;

    /**
     * Whether to include total count.
     */
    @Builder.Default
    private boolean includeTotal = false;

    /**
     * Keyset columns for stable ordering.
     */
    private List<String> keysetColumns;

    /**
     * Gets the effective table name including alias.
     *
     * @return table name with alias
     */
    public String getEffectiveTable() {
        return dataset.getBaseTable() + " AS base";
    }

    /**
     * Checks if this search includes joins.
     *
     * @return true if joins are included
     */
    public boolean hasJoins() {
        return includes != null && !includes.isEmpty();
    }

    /**
     * Checks if this search has filters.
     *
     * @return true if filters are present
     */
    public boolean hasFilters() {
        return filters != null && !filters.isEmpty();
    }
}