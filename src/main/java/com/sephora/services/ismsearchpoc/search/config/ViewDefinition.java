package com.sephora.services.ismsearchpoc.search.config;

import com.sephora.services.ismsearchpoc.search.SortSpec;
import lombok.*;
import java.util.List;
import java.util.Set;

/**
 * Definition of a preset view for simplified querying.
 * Views encapsulate column selection, available filters, and defaults.
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
public class ViewDefinition {

    /**
     * Unique name of the view within a dataset.
     * Example: "EcommATS", "StoreBackroom"
     */
    private String name;

    /**
     * Human-readable description of what this view provides.
     */
    private String description;

    /**
     * List of column names to include in results.
     * Must reference columns defined in the dataset.
     */
    private List<String> columns;

    /**
     * Set of filter names allowed for this view.
     * Must reference filters defined in the dataset.
     */
    private Set<String> filters;

    /**
     * Set of computed field names to include.
     * Must reference computed fields defined in the dataset.
     */
    private Set<String> computed;

    /**
     * Set of join names to include.
     * Must reference joins defined in the dataset.
     */
    private Set<String> includes;

    /**
     * Default sort order for results.
     * If not specified, uses dataset default.
     */
    private List<SortSpec> defaultSort;

    /**
     * Default page size for paginated results.
     * If not specified, uses dataset default.
     */
    private Integer defaultPageSize;

    /**
     * Whether this view allows unpaginated results.
     * Some views may force pagination for performance.
     */
    @Builder.Default
    private boolean allowUnpaginated = true;

    /**
     * Maximum page size override for this view.
     * More restrictive than dataset maximum.
     */
    private Integer maxPageSize;

    /**
     * Required indexes for optimal performance.
     * Used for documentation and validation.
     */
    private List<String> requiredIndexes;
    
    // Added by SRS 1110

    /**
     * GROUP BY columns for aggregation queries.
     * When specified, enables aggregation functions like SUM, COUNT, AVG, etc.
     */
    private List<String> groupBy;


    // End Addition
    
    private boolean distinct = false;

    /**
     * Validates the view definition.
     *
     * @throws IllegalStateException if validation fails
     */
    public void validate() {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalStateException("View name cannot be null or empty");
        }

        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException("View must specify at least one column");
        }

        if (defaultPageSize != null && defaultPageSize <= 0) {
            throw new IllegalStateException("Default page size must be positive");
        }

        if (maxPageSize != null && maxPageSize <= 0) {
            throw new IllegalStateException("Max page size must be positive");
        }

        if (defaultSort != null) {
            for (SortSpec sort : defaultSort) {
                sort.validate();
            }
        }
    }

    /**
     * Checks if a filter is allowed for this view.
     *
     * @param filterName name of the filter
     * @return true if filter is allowed
     */
    public boolean isFilterAllowed(String filterName) {
        return filters == null || filters.contains(filterName);
    }

    /**
     * Gets effective page size considering defaults.
     *
     * @param datasetDefault default from dataset configuration
     * @return effective default page size
     */
    public int getEffectivePageSize(int datasetDefault) {
        return defaultPageSize != null ? defaultPageSize : datasetDefault;
    }
}