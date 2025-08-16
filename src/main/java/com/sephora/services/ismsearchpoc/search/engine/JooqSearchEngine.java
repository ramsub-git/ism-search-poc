package com.sephora.services.ismsearchpoc.search.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sephora.services.ismsearchpoc.search.*;
import com.sephora.services.ismsearchpoc.search.config.*;
import com.sephora.services.ismsearchpoc.search.exception.*;
import com.sephora.services.ismsearchpoc.search.service.SearchContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * JooQ-based implementation of the search engine.
 * Provides dynamic SQL generation with type safety.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JooqSearchEngine implements SearchEngine {

    private final DSLContext dsl;
    private final ObjectMapper objectMapper;

    /**
     * Executes a search query using JooQ.
     *
     * @param context search context
     * @return search results
     */
    @Override
    public SearchResponse<Map<String, Object>> execute(SearchContext context) throws Exception {
        log.debug("Executing JooQ search for dataset: {}", context.getDataset().getBaseTable());

        long startTime = System.currentTimeMillis();
        SearchResponse<Map<String, Object>> response = null;

        try {
            // Build and execute query
            SelectQuery<?> query = buildQuery(context);

            // Add pagination
            PaginationResult paginationResult = applyPagination(query, context);

            // Execute query
            Result<?> result = query.fetch();

            // Process results
            List<Map<String, Object>> data = processResults(result, context, paginationResult);

            // Get total count if requested
            Long total = null;
            if (context.isIncludeTotal() && context.isPaginated()) {
                total = executeCountQuery(context);
            }

            // Build response
            PageInfo pageInfo = buildPageInfo(context, paginationResult, data.size(), total);

            response = SearchResponse.<Map<String, Object>>builder()
                    .data(data)
                    .pageInfo(pageInfo)
                    .meta(buildMetadata(context, startTime))
                    .build();

            return response;

        } catch (Exception e) {
            log.error("JooQ search execution failed", e);
            throw new SearchException("QUERY_EXECUTION_ERROR",
                    "Failed to execute search query: " + e.getMessage(),
                    500, e);
        } finally {
            if (log.isDebugEnabled()) {
                long elapsed = System.currentTimeMillis() - startTime;
                log.debug("Search completed in {} ms, returned {} rows",
                        elapsed, response != null ? response.getCount() : 0);
            }
        }
    }

    /**
     * Builds the JooQ query from context.
     */
    private SelectQuery<?> buildQuery(SearchContext context) {
        SelectQuery<?> query = dsl.selectQuery();
        DatasetDefinition dataset = context.getDataset();

        // Add base table
        Table<?> baseTable = DSL.table(DSL.name(dataset.getBaseTable())).as("base");
        query.addFrom(baseTable);

        // Add select fields
        addSelectFields(query, context);

        // Add joins
        if (context.hasJoins()) {
            addJoins(query, context);
        }

        // Add where conditions
        if (context.hasFilters()) {
            addWhereConditions(query, context);
        }

        // Add order by
        addOrderBy(query, context);

        log.debug("Built query: {}", query.getSQL());

        return query;
    }

    /**
     * Adds SELECT fields to the query.
     */
    private void addSelectFields(SelectQuery<?> query, SearchContext context) {
        DatasetDefinition dataset = context.getDataset();

        for (String columnName : context.getColumns()) {
            // Check if it's a computed field
            if (dataset.getComputed() != null && dataset.getComputed().containsKey(columnName)) {
                // Add computed field
                String expression = dataset.getComputed().get(columnName);
                Field<?> computedField = DSL.field(expression).as(columnName);
                query.addSelect(computedField);
            } else {
                // Regular column
                ColumnDefinition colDef = dataset.getColumns().get(columnName);
                if (colDef != null) {
                    Field<?> field; 
                    if (colDef.getSql().contains(".")) {
                        // Handle qualified column names (table.column)
                        String[] parts = colDef.getSql().split("\\.", 2);
                        field = DSL.field(DSL.name(parts[0], parts[1]));
                    } else {
                        field = DSL.field(DSL.name(colDef.getSql()));
                    }
                    String alias = colDef.getEffectiveName(columnName);
                    query.addSelect(field.as(alias));
                } else {
                    // Check join columns
                    Field<?> field = findJoinColumn(context, columnName);
                    if (field != null) {
                        query.addSelect(field.as(columnName));
                    } else {
                        log.warn("Column not found: {}", columnName);
                    }
                }
            }
        }

        // Add inline computed fields for ad-hoc mode
        if (context.getInlineComputed() != null) {
            for (Map.Entry<String, String> entry : context.getInlineComputed().entrySet()) {
                Field<?> computedField = DSL.field(entry.getValue()).as(entry.getKey());
                query.addSelect(computedField);
            }
        }
    }


    /**
     * Finds a column from join definitions.
     */
    private Field<?> findJoinColumn(SearchContext context, String columnName) {
        if (!context.hasJoins()) {
            return null;
        }

        Map<String, JoinDefinition> joins = context.getDataset().getJoins();
        for (String joinName : context.getIncludes()) {
            JoinDefinition join = joins.get(joinName);
            if (join != null && join.getColumns() != null) {
                ColumnDefinition colDef = join.getColumns().get(columnName);
                if (colDef != null) {
                    // Handle qualified column names properly
                    if (colDef.getSql().contains(".")) {
                        String[] parts = colDef.getSql().split("\\.", 2);
                        return DSL.field(DSL.name(parts[0], parts[1]));
                    } else {
                        return DSL.field(DSL.name(colDef.getSql()));
                    }
                }
            }
        }

        return null;
    }
    

    /**
     * Adds JOIN clauses to the query.
     */
    private void addJoins(SelectQuery<?> query, SearchContext context) {
        Map<String, JoinDefinition> joins = context.getDataset().getJoins();

        for (String joinName : context.getIncludes()) {
            JoinDefinition joinDef = joins.get(joinName);
            if (joinDef != null) {
                Table<?> joinTable = DSL.table(joinDef.getTable());
                Condition joinCondition = DSL.condition(joinDef.getOn());

                switch (joinDef.getNormalizedType()) {
                    case "INNER":
                        query.addJoin(joinTable, JoinType.JOIN, joinCondition);
                        break;
                    case "RIGHT":
                        query.addJoin(joinTable, JoinType.RIGHT_OUTER_JOIN, joinCondition);
                        break;
                    case "LEFT":
                    default:
                        query.addJoin(joinTable, JoinType.LEFT_OUTER_JOIN, joinCondition);
                        break;
                }
            }
        }
    }

    /**
     * Adds WHERE conditions to the query.
     */
    private void addWhereConditions(SelectQuery<?> query, SearchContext context) {
        Map<String, FilterDefinition> filterDefs = context.getDataset().getFilters();

        for (Map.Entry<String, Object> entry : context.getFilters().entrySet()) {
            String filterName = entry.getKey();
            Object value = entry.getValue();

            FilterDefinition filterDef = filterDefs.get(filterName);
            if (filterDef == null) {
                log.warn("Filter definition not found: {}", filterName);
                continue;
            }

            Field<Object> field = DSL.field(DSL.name(filterDef.getColumn()));
            Condition condition = buildCondition(field, filterDef, value);

            if (condition != null) {
                query.addConditions(condition);
            }
        }
    }

    /**
     * Builds a condition for a filter.
     */
    private Condition buildCondition(Field<Object> field, FilterDefinition filterDef, Object value) {
        if (value == null) {
            return field.isNull();
        }

        String op = filterDef.getOp().toUpperCase();

        switch (op) {
            case "=":
                return field.eq(value);
            case "!=":
                return field.ne(value);
            case ">":
                return field.gt(value);
            case ">=":
                return field.ge(value);
            case "<":
                return field.lt(value);
            case "<=":
                return field.le(value);
            case "IN":
                if (value instanceof Collection<?>) {
                    return field.in((Collection<?>) value);
                } else {
                    return field.eq(value);
                }
            case "NOT IN":
                if (value instanceof Collection<?>) {
                    return field.notIn((Collection<?>) value);
                } else {
                    return field.ne(value);
                }
            case "LIKE":
                return field.like(String.valueOf(value));
            case "BETWEEN":
                if (value instanceof List<?> && ((List<?>) value).size() == 2) {
                    List<?> range = (List<?>) value;
                    return field.between(range.get(0), range.get(1));
                }
                log.warn("BETWEEN requires exactly 2 values, got: {}", value);
                return null;
            default:
                log.warn("Unknown operator: {}", op);
                return field.eq(value);
        }
    }

    /**
     * Adds ORDER BY clause to the query.
     */
    private void addOrderBy(SelectQuery<?> query, SearchContext context) {
        List<SortSpec> sortSpecs = context.getSort();

        // Add explicit sort fields
        if (sortSpecs != null) {
            for (SortSpec sort : sortSpecs) {
                Field<?> field = DSL.field(DSL.name(sort.getField()));
                SortField<?> sortField = "DESC".equalsIgnoreCase(sort.getNormalizedDirection())
                        ? field.desc()
                        : field.asc();
                query.addOrderBy(sortField);
            }
        }

        // Add keyset columns for deterministic ordering
        if (context.isPaginated() && context.getPaginationMode() == PaginationMode.KEYSET) {
            for (String keysetCol : context.getKeysetColumns()) {
                // Check if already in sort
                boolean alreadySorted = sortSpecs != null && sortSpecs.stream()
                        .anyMatch(s -> s.getField().equalsIgnoreCase(keysetCol));

                if (!alreadySorted) {
                    query.addOrderBy(DSL.field(DSL.name(keysetCol)).asc());
                }
            }
        }
    }

    /**
     * Container for pagination state.
     */
    private static class PaginationResult {
        boolean hasMore = false;
        String nextCursor = null;
        Map<String, Object> lastRow = null;
    }

    /**
     * Applies pagination to the query.
     */
    private PaginationResult applyPagination(SelectQuery<?> query, SearchContext context) {
        PaginationResult result = new PaginationResult();

        if (!context.isPaginated()) {
            // Unpaginated - apply safety cap
            query.addLimit(context.getPageSize());
        } else if (context.getPaginationMode() == PaginationMode.KEYSET) {
            // Keyset pagination
            if (context.getCursor() != null) {
                applyKeysetCursor(query, context);
            }
            // Fetch one extra row to determine hasMore
            query.addLimit(context.getPageSize() + 1);
        } else {
            // Offset pagination
            int offset = context.getPageNumber() * context.getPageSize();
            query.addLimit(offset, context.getPageSize() + 1);
        }

        return result;
    }

    /**
     * Applies keyset cursor conditions.
     */
    private void applyKeysetCursor(SelectQuery<?> query, SearchContext context) {
        try {
            // Decode cursor
            String cursorJson = new String(
                    Base64.getDecoder().decode(context.getCursor()),
                    StandardCharsets.UTF_8
            );
            Map<String, Object> cursorData = objectMapper.readValue(
                    cursorJson,
                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}
            );

            // Build keyset condition
            List<Condition> conditions = new ArrayList<>();
            List<String> keysetColumns = context.getKeysetColumns();

            for (int i = 0; i < keysetColumns.size(); i++) {
                List<Condition> subConditions = new ArrayList<>();

                // Add equality conditions for previous columns
                for (int j = 0; j < i; j++) {
                    String col = keysetColumns.get(j);
                    Object val = cursorData.get(col);
                    subConditions.add(DSL.field(DSL.name(col)).eq(val));
                }

                // Add inequality for current column
                String col = keysetColumns.get(i);
                Object val = cursorData.get(col);

                // Determine direction from sort spec
                boolean descending = false;
                if (context.getSort() != null) {
                    Optional<SortSpec> sortSpec = context.getSort().stream()
                            .filter(s -> s.getField().equalsIgnoreCase(col))
                            .findFirst();
                    if (sortSpec.isPresent()) {
                        descending = "DESC".equalsIgnoreCase(sortSpec.get().getNormalizedDirection());
                    }
                }

                Field<Object> field = DSL.field(DSL.name(col));
                Condition inequality = descending ? field.lt(val) : field.gt(val);
                subConditions.add(inequality);

                // Combine with AND
                if (!subConditions.isEmpty()) {
                    conditions.add(DSL.and(subConditions));
                }
            }

            // Combine with OR
            if (!conditions.isEmpty()) {
                query.addConditions(DSL.or(conditions));
            }

        } catch (Exception e) {
            log.warn("Failed to parse cursor: {}", context.getCursor(), e);
            throw new SearchException("INVALID_CURSOR",
                    "Invalid pagination cursor",
                    400, e);
        }
    }

    /**
     * Processes query results.
     */
    private List<Map<String, Object>> processResults(Result<?> result,
                                                     SearchContext context,
                                                     PaginationResult paginationResult) {
        List<Map<String, Object>> data = new ArrayList<>();
        int limit = context.isPaginated() ? context.getPageSize() : result.size();

        for (int i = 0; i < Math.min(result.size(), limit); i++) {
            org.jooq.Record record = result.get(i);
            Map<String, Object> row = new LinkedHashMap<>();

            // Convert record to map
            for (Field<?> field : record.fields()) {
                String name = field.getName();
                Object value = record.get(field);
                row.put(name, value);
            }

            data.add(row);
        }

        // Check for more results
        if (context.isPaginated() && result.size() > limit) {
            paginationResult.hasMore = true;

            // Get last row for cursor
            if (context.getPaginationMode() == PaginationMode.KEYSET && !data.isEmpty()) {
                paginationResult.lastRow = data.get(data.size() - 1);
                paginationResult.nextCursor = encodeCursor(paginationResult.lastRow, context);
            }
        }

        return data;
    }

    /**
     * Encodes a cursor from the last row.
     */
    private String encodeCursor(Map<String, Object> lastRow, SearchContext context) {
        try {
            Map<String, Object> cursorData = new LinkedHashMap<>();

            for (String col : context.getKeysetColumns()) {
                Object value = lastRow.get(col);
                if (value == null) {
                    // Try lowercase
                    value = lastRow.get(col.toLowerCase());
                }
                if (value == null) {
                    // Try with base prefix
                    value = lastRow.get("base." + col);
                }
                cursorData.put(col, value);
            }

            String json = objectMapper.writeValueAsString(cursorData);
            return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            log.error("Failed to encode cursor", e);
            return null;
        }
    }

    /**
     * Executes a count query.
     */
    private Long executeCountQuery(SearchContext context) {
        try {
            // Build count query directly with select
            Table<?> baseTable = DSL.table(DSL.name(context.getDataset().getBaseTable())).as("base");

            // Start with select count(*)
            SelectJoinStep<?> selectStep = dsl.selectCount().from(baseTable);

            // Add joins if needed
            if (context.hasJoins() && context.getDataset().getJoins() != null) {
                for (String joinName : context.getIncludes()) {
                    JoinDefinition joinDef = context.getDataset().getJoins().get(joinName);
                    if (joinDef != null) {
                        Table<?> joinTable = DSL.table(joinDef.getTable());
                        Condition joinCondition = DSL.condition(joinDef.getOn());

                        switch (joinDef.getNormalizedType()) {
                            case "INNER":
                                selectStep = selectStep.join(joinTable).on(joinCondition);
                                break;
                            case "RIGHT":
                                selectStep = selectStep.rightJoin(joinTable).on(joinCondition);
                                break;
                            case "LEFT":
                            default:
                                selectStep = selectStep.leftJoin(joinTable).on(joinCondition);
                                break;
                        }
                    }
                }
            }

            // Add where conditions if present
            if (context.hasFilters()) {
                Map<String, FilterDefinition> filterDefs = context.getDataset().getFilters();
                List<Condition> conditions = new ArrayList<>();

                for (Map.Entry<String, Object> entry : context.getFilters().entrySet()) {
                    String filterName = entry.getKey();
                    Object value = entry.getValue();

                    FilterDefinition filterDef = filterDefs.get(filterName);
                    if (filterDef != null) {
                        Field<Object> field = DSL.field(DSL.name(filterDef.getColumn()));
                        Condition condition = buildCondition(field, filterDef, value);
                        if (condition != null) {
                            conditions.add(condition);
                        }
                    }
                }

                if (!conditions.isEmpty()) {
                    return selectStep.where(conditions).fetchOne(0, Long.class);
                }
            }

            return selectStep.fetchOne(0, Long.class);

        } catch (Exception e) {
            log.warn("Failed to execute count query", e);
            return null;
        }
    }

    /**
     * Builds page info for the response.
     */
    private PageInfo buildPageInfo(SearchContext context,
                                   PaginationResult paginationResult,
                                   int returnedCount,
                                   Long total) {
        PageInfo pageInfo = PageInfo.builder()
                .mode(context.getPaginationMode())
                .page(context.getPageNumber())
                .size(context.getPageSize())
                .returned(returnedCount)
                .total(total)
                .cursor(context.getCursor())
                .nextCursor(paginationResult.nextCursor)
                .hasMore(paginationResult.hasMore)
                .build();

        // Set paginated flag directly after building
        pageInfo.setPaginate(context.isPaginated());

        return pageInfo;
    }

    /**
     * Builds response metadata.
     */
    private Map<String, Object> buildMetadata(SearchContext context, long startTime) {
        Map<String, Object> meta = new HashMap<>();
        meta.put("tookMs", System.currentTimeMillis() - startTime);
        meta.put("engine", "jooq");
        return meta;
    }
}