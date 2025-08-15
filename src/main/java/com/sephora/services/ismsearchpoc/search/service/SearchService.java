package com.sephora.services.ismsearchpoc.search.service;

import com.sephora.services.ismsearchpoc.search.*;
import com.sephora.services.ismsearchpoc.search.config.*;
import com.sephora.services.ismsearchpoc.search.engine.SearchEngine;
import com.sephora.services.ismsearchpoc.search.exception.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Main service for executing search queries.
 * Handles view resolution, request validation, and query execution.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SearchService {

    private final ConfigurationLoader configLoader;
    private final SearchEngine searchEngine;

    /**
     * Executes a search query against the specified dataset.
     *
     * @param datasetKey dataset to search
     * @param request search request
     * @return search results
     * @throws SearchException if search fails
     */
    @Transactional(readOnly = true)
    public SearchResponse<Map<String, Object>> search(DatasetKey datasetKey, SearchRequest request) {
        log.debug("Search request for dataset {}: {}", datasetKey, request);

        try {
            // Validate request
            request.validate();

            // Get configuration
            SearchConfiguration config = configLoader.getConfiguration();
            DatasetDefinition dataset = config.getDataset(datasetKey);

            if (dataset == null) {
                log.error("Dataset not found: {}", datasetKey);
                throw new SearchException("UNKNOWN_DATASET",
                        "Dataset " + datasetKey + " not found",
                        404);
            }

            // Process based on mode
            SearchContext context;
            if (request.isViewMode()) {
                context = processViewMode(dataset, request);
            } else {
                context = processAdHocMode(dataset, request);
            }

            // Execute search
            SearchResponse<Map<String, Object>> response = searchEngine.execute(context);

            // Add metadata
            enrichResponse(response, context);

            log.debug("Search completed successfully for dataset {}", datasetKey);
            return response;

        } catch (SearchException e) {
            // Already a search exception, just propagate
            log.warn("Search failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            // Wrap unexpected exceptions
            log.error("Unexpected search error", e);
            throw new SearchException("SEARCH_ERROR",
                    "Search failed: " + e.getMessage(),
                    500, e);
        }
    }

    /**
     * Processes a view-based search request.
     *
     * @param dataset dataset definition
     * @param request search request
     * @return search context
     */
    private SearchContext processViewMode(DatasetDefinition dataset, SearchRequest request) {
        String viewName = request.getView();
        ViewDefinition view = dataset.getView(viewName);

        if (view == null) {
            List<String> validViews = dataset.getViews() != null
                    ? new ArrayList<>(dataset.getViews().keySet())
                    : Collections.emptyList();
            throw new ViewNotFoundException(viewName, dataset.getBaseTable(), validViews);
        }

        log.debug("Using view: {}", viewName);

        // Build context from view
        SearchContext.SearchContextBuilder builder = SearchContext.builder()
                .dataset(dataset)
                .view(view)
                .viewName(viewName);

        // Apply columns from view
        List<String> columns = resolveViewColumns(dataset, view);
        builder.columns(columns);

        // Apply and validate filters
        Map<String, Object> filters = validateViewFilters(dataset, view, request.getFilters());
        builder.filters(filters);

        // Apply sort (request overrides view default)
        List<SortSpec> sort = request.getSort() != null ? request.getSort() : view.getDefaultSort();
        if (sort == null) {
            sort = dataset.getDefaultSort();
        }
        builder.sort(sort);

        // Apply includes (joins)
        if (view.getIncludes() != null && !view.getIncludes().isEmpty()) {
            builder.includes(new ArrayList<>(view.getIncludes()));
        }

        // Apply pagination
        applyPagination(builder, dataset, view, request);

        return builder.build();
    }

    /**
     * Processes an ad-hoc search request.
     *
     * @param dataset dataset definition
     * @param request search request
     * @return search context
     */
    private SearchContext processAdHocMode(DatasetDefinition dataset, SearchRequest request) {
        log.debug("Processing ad-hoc query");

        SearchContext.SearchContextBuilder builder = SearchContext.builder()
                .dataset(dataset)
                .adhoc(true);

        // Validate and apply columns
        List<String> columns = validateColumns(dataset, request.getColumns());
        builder.columns(columns);

        // Validate and apply filters
        Map<String, Object> filters = validateFilters(dataset, request.getFilters());
        builder.filters(filters);

        // Apply sort or use default
        List<SortSpec> sort = request.getSort();
        if (sort == null || sort.isEmpty()) {
            sort = dataset.getDefaultSort();
        }
        builder.sort(sort);

        // Apply computed fields
        if (request.getComputed() != null && !request.getComputed().isEmpty()) {
            builder.inlineComputed(request.getComputed());
        }

        // Apply pagination
        applyPagination(builder, dataset, null, request);

        return builder.build();
    }

    /**
     * Resolves columns for a view including computed fields.
     */
    private List<String> resolveViewColumns(DatasetDefinition dataset, ViewDefinition view) {
        List<String> columns = new ArrayList<>();

        // Add regular columns
        if (view.getColumns() != null) {
            columns.addAll(view.getColumns());
        }

        // Add computed fields
        if (view.getComputed() != null) {
            for (String computed : view.getComputed()) {
                if (!columns.contains(computed)) {
                    columns.add(computed);
                }
            }
        }

        // Add join columns
        if (view.getIncludes() != null && dataset.getJoins() != null) {
            for (String joinName : view.getIncludes()) {
                JoinDefinition join = dataset.getJoins().get(joinName);
                if (join != null && join.getColumns() != null) {
                    columns.addAll(join.getColumns().keySet());
                }
            }
        }

        return columns;
    }

    /**
     * Validates filters for view mode.
     */
    private Map<String, Object> validateViewFilters(DatasetDefinition dataset,
                                                    ViewDefinition view,
                                                    Map<String, Object> requestFilters) {
        if (requestFilters == null || requestFilters.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Object> validated = new HashMap<>();

        for (Map.Entry<String, Object> entry : requestFilters.entrySet()) {
            String filterName = entry.getKey();

            // Check if filter is allowed for view
            if (!view.isFilterAllowed(filterName)) {
                throw new InvalidFilterException(filterName, view.getName());
            }

            // Validate filter value
            FilterDefinition filterDef = dataset.getFilters().get(filterName);
            if (filterDef == null) {
                throw new InvalidFilterException(filterName, view.getName());
            }

            Object value = validateFilterValue(filterName, filterDef, entry.getValue());
            validated.put(filterName, value);
        }

        return validated;
    }

    /**
     * Validates filters for ad-hoc mode.
     */
    private Map<String, Object> validateFilters(DatasetDefinition dataset,
                                                Map<String, Object> requestFilters) {
        if (requestFilters == null || requestFilters.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Object> validated = new HashMap<>();

        for (Map.Entry<String, Object> entry : requestFilters.entrySet()) {
            String filterName = entry.getKey();
            FilterDefinition filterDef = dataset.getFilters().get(filterName);
            if (filterDef == null) {
                throw new InvalidFilterException(filterName, null);
            }

            Object value = validateFilterValue(filterName, filterDef, entry.getValue());
            validated.put(filterName, value);
        }

        return validated;
    }

    /**
     * Validates a single filter value.
     */
    private Object validateFilterValue(String filterName, FilterDefinition filterDef, Object value) {
        // Check for null
        if (value == null) {
            if (filterDef.isRequired()) {
                throw new InvalidFilterException(filterName, "Required filter cannot be null", null);
            }
            return null;
        }

        // Check array size limits
        if (value instanceof Collection<?>) {
            Collection<?> collection = (Collection<?>) value;
            if (collection.size() > filterDef.getMax()) {
                throw QuerySizeException.filterListTooLarge(filterName, collection.size(), filterDef.getMax());
            }

            // Validate allowed values
            if (filterDef.getAllowed() != null && !filterDef.getAllowed().isEmpty()) {
                for (Object item : collection) {
                    if (!filterDef.getAllowed().contains(String.valueOf(item))) {
                        throw new InvalidFilterException(filterName,
                                "Value '" + item + "' not in allowed list", null);
                    }
                }
            }
        } else {
            // Single value - check allowed list
            if (filterDef.getAllowed() != null && !filterDef.getAllowed().isEmpty()) {
                if (!filterDef.getAllowed().contains(String.valueOf(value))) {
                    throw new InvalidFilterException(filterName,
                            "Value '" + value + "' not in allowed list", null);
                }
            }
        }

        return value;
    }

    /**
     * Validates column selection.
     */
    private List<String> validateColumns(DatasetDefinition dataset, List<String> requestColumns) {
        if (requestColumns == null || requestColumns.isEmpty()) {
            throw new SearchException("MISSING_COLUMNS",
                    "Columns must be specified for ad-hoc queries",
                    400);
        }

        List<String> validated = new ArrayList<>();

        for (String column : requestColumns) {
            if (!dataset.hasColumn(column)) {
                throw new SearchException("INVALID_COLUMN",
                        "Unknown column: " + column,
                        400);
            }
            validated.add(column);
        }

        return validated;
    }

    /**
     * Applies pagination settings to search context.
     */
    private void applyPagination(SearchContext.SearchContextBuilder builder,
                                 DatasetDefinition dataset,
                                 ViewDefinition view,
                                 SearchRequest request) {

        PaginationConfig paginationConfig = dataset.getPagination();

        // Determine if paginated
        boolean paginate = request.isPaginate();
        builder.paginated(paginate);

        if (paginate) {
            // Get effective page size
            int defaultSize = view != null
                    ? view.getEffectivePageSize(paginationConfig.getDefaultPageSize())
                    : paginationConfig.getDefaultPageSize();

            int pageSize = paginationConfig.getEffectivePageSize(
                    request.getSize() != null ? request.getSize() : defaultSize);

            // Check view-specific max
            if (view != null && view.getMaxPageSize() != null) {
                pageSize = Math.min(pageSize, view.getMaxPageSize());
            }

            builder.pageSize(pageSize);
            builder.paginationMode(request.getPaginationMode());

            // Apply cursor or page number
            if (request.getPaginationMode() == PaginationMode.KEYSET) {
                builder.cursor(request.getCursor());
            } else {
                builder.pageNumber(request.getPage() != null ? request.getPage() : 0);

                // Check if OFFSET is allowed
                if (!paginationConfig.isAllowOffset()) {
                    throw new SearchException("OFFSET_NOT_ALLOWED",
                            "OFFSET pagination not allowed for this dataset",
                            400);
                }
            }
        } else {
            // Unpaginated - check if allowed for view
            if (view != null && !view.isAllowUnpaginated()) {
                throw new SearchException("PAGINATION_REQUIRED",
                        "View " + view.getName() + " requires pagination",
                        400);
            }

            // Set safety cap
            builder.pageSize(paginationConfig.getMaxUnpaginatedRows());
        }

        builder.includeTotal(request.isIncludeTotal());
        builder.keysetColumns(paginationConfig.getKeysetColumns());
    }

    /**
     * Enriches response with metadata.
     */
    private void enrichResponse(SearchResponse<Map<String, Object>> response, SearchContext context) {
        Map<String, Object> meta = new HashMap<>(response.getMeta());

        // Add context metadata
        meta.put("dataset", context.getDataset().getBaseTable());

        if (context.getViewName() != null) {
            meta.put("view", context.getViewName());
        }

        if (context.isAdhoc()) {
            meta.put("mode", "adhoc");
        }

        response.setMeta(meta);
    }
}