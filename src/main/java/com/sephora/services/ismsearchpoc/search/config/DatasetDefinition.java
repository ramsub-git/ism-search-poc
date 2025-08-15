package com.sephora.services.ismsearchpoc.search.config;

import com.sephora.services.ismsearchpoc.search.SortSpec;
import lombok.*;
import java.util.List;
import java.util.Map;

/**
 * Complete definition of a searchable dataset.
 * Contains all configuration for tables, columns, filters, joins, and views.
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
public class DatasetDefinition {

    /**
     * Primary database table name.
     * Example: "skuloc", "location_master"
     */
    private String baseTable;

    /**
     * Available columns mapped by name.
     * Key is the API field name, value is the column definition.
     */
    private Map<String, ColumnDefinition> columns;

    /**
     * Available filters mapped by name.
     * Key is the API filter name, value is the filter definition.
     */
    private Map<String, FilterDefinition> filters;

    /**
     * Available joins mapped by name.
     * Key is the join name, value is the join definition.
     */
    private Map<String, JoinDefinition> joins;

    /**
     * Computed field expressions mapped by name.
     * Key is the field name, value is the SQL expression.
     */
    private Map<String, String> computed;

    /**
     * Preset views mapped by name.
     * Key is the view name, value is the view definition.
     */
    private Map<String, ViewDefinition> views;

    /**
     * Pagination configuration.
     */
    private PaginationConfig pagination;

    /**
     * Default sort order when not specified by view or request.
     */
    private List<SortSpec> defaultSort;

    /**
     * Validates the dataset definition.
     *
     * @throws IllegalStateException if validation fails
     */
    public void validate() {
        if (baseTable == null || baseTable.trim().isEmpty()) {
            throw new IllegalStateException("Base table cannot be null or empty");
        }

        // Validate columns
        if (columns != null) {
            columns.forEach((name, col) -> {
                try {
                    col.validate();
                } catch (Exception e) {
                    throw new IllegalStateException("Invalid column '" + name + "': " + e.getMessage());
                }
            });
        }

        // Validate filters
        if (filters != null) {
            filters.forEach((name, filter) -> {
                try {
                    filter.validate();
                } catch (Exception e) {
                    throw new IllegalStateException("Invalid filter '" + name + "': " + e.getMessage());
                }
            });
        }

        // Validate joins
        if (joins != null) {
            joins.forEach((name, join) -> {
                try {
                    join.validate();
                } catch (Exception e) {
                    throw new IllegalStateException("Invalid join '" + name + "': " + e.getMessage());
                }
            });
        }

        // Validate views
        if (views != null) {
            views.forEach((name, view) -> {
                try {
                    view.validate();
                    // Ensure view references valid columns
                    validateViewReferences(name, view);
                } catch (Exception e) {
                    throw new IllegalStateException("Invalid view '" + name + "': " + e.getMessage());
                }
            });
        }

        // Validate pagination
        if (pagination != null) {
            pagination.validate();
        }

        // Validate default sort
        if (defaultSort != null) {
            for (SortSpec sort : defaultSort) {
                sort.validate();
            }
        }
    }

    /**
     * Validates that ViewReferences exist in dataset.
     */
    private void validateViewReferences(String viewName, ViewDefinition view) {
        // Check columns exist
        if (view.getColumns() != null) {
            for (String col : view.getColumns()) {
                if (columns != null && !columns.containsKey(col) &&
                        computed != null && !computed.containsKey(col)) {
                    throw new IllegalStateException(
                            "Column '" + col + "' not found in dataset");
                }
            }
        }

        // Check filters exist
        if (view.getFilters() != null) {
            for (String filter : view.getFilters()) {
                if (filters != null && !filters.containsKey(filter)) {
                    throw new IllegalStateException(
                            "Filter '" + filter + "' not found in dataset");
                }
            }
        }

        // Check computed fields exist
        if (view.getComputed() != null) {
            for (String comp : view.getComputed()) {
                if (computed != null && !computed.containsKey(comp)) {
                    throw new IllegalStateException(
                            "Computed field '" + comp + "' not found in dataset");
                }
            }
        }

        // Check joins exist
        if (view.getIncludes() != null) {
            for (String join : view.getIncludes()) {
                if (joins != null && !joins.containsKey(join)) {
                    throw new IllegalStateException(
                            "Join '" + join + "' not found in dataset");
                }
            }
        }
    }

    /**
     * Gets a view by name.
     *
     * @param viewName name of the view
     * @return view definition or null if not found
     */
    public ViewDefinition getView(String viewName) {
        return views != null ? views.get(viewName) : null;
    }

    /**
     * Checks if a column exists.
     *
     * @param columnName name of the column
     * @return true if column exists in regular columns or computed fields
     */
    public boolean hasColumn(String columnName) {
        return (columns != null && columns.containsKey(columnName)) ||
                (computed != null && computed.containsKey(columnName));
    }
}