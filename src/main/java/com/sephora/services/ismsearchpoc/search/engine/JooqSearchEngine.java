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

        // Added by SRS 1110
        // Add base table
        /*
        Table<?> baseTable = DSL.table(DSL.name(dataset.getBaseTable())).as("base");
        query.addFrom(baseTable);
        */
        // NEW CODE:
        Table<?> baseTable = createBaseTable(dataset);
        query.addFrom(baseTable);
        // End addition

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

        // Added by SRS 1110
        // Add GROUP BY if specified
        if (context.hasGroupBy()) {
            log.debug("Context has GROUP BY: {}", context.getGroupBy());
            addGroupBy(query, context);
        } else {
            log.debug("Context does not have GROUP BY");
        }
        // End Addition

        // Add order by
        addOrderBy(query, context);
        
        
        if (context.isDistinct()) {
            query.setDistinct(true);
        }        
        

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
//    private void addWhereConditions(SelectQuery<?> query, SearchContext context) {
//        Map<String, FilterDefinition> filterDefs = context.getDataset().getFilters();
//
//        for (Map.Entry<String, Object> entry : context.getFilters().entrySet()) {
//            String filterName = entry.getKey();
//            Object value = entry.getValue();
//
//            FilterDefinition filterDef = filterDefs.get(filterName);
//            if (filterDef == null) {
//                log.warn("Filter definition not found: {}", filterName);
//                continue;
//            }
//
//            Field<Object> field = DSL.field(DSL.name(filterDef.getColumn()));
//            Condition condition = buildCondition(field, filterDef, value);
//
//            if (condition != null) {
//                query.addConditions(condition);
//            }
//        }
//    }

    
    /**
     * Adds WHERE conditions to the query.
     * 
     * This is the UPDATED version that handles qualified column names (table.column)
     * properly for filters that reference joined tables.
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

            Condition condition = null;


            // Check if this is an expression-based filter
            if (filterDef.getExpression() != null && !filterDef.getExpression().trim().isEmpty()) {
                log.debug("Building expression-based condition for filter: {}", filterName);
//                condition = buildExpressionCondition(filterDef, value);
            } else  {
            // Create field with proper handling for qualified names
            Field<Object> field;
            String columnExpression = filterDef.getColumn();

            if (columnExpression.contains(".")) {
                // Handle qualified column names (table.column)
                String[] parts = columnExpression.split("\\.", 2);
                field = DSL.field(DSL.name(parts[0], parts[1]));
            } else {
                // Simple column name
                field = DSL.field(DSL.name(columnExpression));
            }


            condition = buildCondition(field, filterDef, value);
        }
            if (condition != null) {
                query.addConditions(condition);
            }
        }
    }


    
    
    
    /**
     * Builds a condition for a filter.
     */
//    private Condition buildCondition(Field<Object> field, FilterDefinition filterDef, Object value) {
//        if (value == null) {
//            return field.isNull();
//        }
//
//        String op = filterDef.getOp().toUpperCase();
//
//        switch (op) {
//            case "=":
//                return field.eq(value);
//            case "!=":
//                return field.ne(value);
//            case ">":
//                return field.gt(value);
//            case ">=":
//                return field.ge(value);
//            case "<":
//                return field.lt(value);
//            case "<=":
//                return field.le(value);
//            case "IN":
//                if (value instanceof Collection<?>) {
//                    return field.in((Collection<?>) value);
//                } else {
//                    return field.eq(value);
//                }
//            case "NOT IN":
//                if (value instanceof Collection<?>) {
//                    return field.notIn((Collection<?>) value);
//                } else {
//                    return field.ne(value);
//                }
//            case "LIKE":
//                return field.like(String.valueOf(value));
//            case "BETWEEN":
//                if (value instanceof List<?> && ((List<?>) value).size() == 2) {
//                    List<?> range = (List<?>) value;
//                    return field.between(range.get(0), range.get(1));
//                }
//                log.warn("BETWEEN requires exactly 2 values, got: {}", value);
//                return null;
//            default:
//                log.warn("Unknown operator: {}", op);
//                return field.eq(value);
//        }
//    }


    /**
     * Builds a condition for expression-based filters.
     *
     * @param filterDef filter definition with expression
     * @param value filter value from request
     * @return SQL condition
     */
    private Condition buildExpressionCondition(FilterDefinition filterDef, Object value) {
        String expression = filterDef.getExpression();
        String op = filterDef.getOp().toUpperCase();

        log.debug("Building expression condition: {} {} {}", expression, op, value);

        // For simple boolean/equality checks
        if ("=".equals(op)) {
            if (value == null) {
                return DSL.condition(expression + " IS NULL");
            }

            // For boolean type, treat the expression as the condition itself
            if ("boolean".equalsIgnoreCase(filterDef.getType())) {
                if (Boolean.TRUE.equals(value) || "1".equals(String.valueOf(value)) ||
                        "true".equalsIgnoreCase(String.valueOf(value))) {
                    // Return the expression as-is (it should evaluate to true)
                    return DSL.condition(expression);
                } else {
                    // Negate the expression
                    return DSL.condition("NOT (" + expression + ")");
                }
            }

            // For numeric comparisons, wrap expression in parentheses and compare
            return DSL.condition("(" + expression + ") = ?", value);
        }

        // For other operators, treat expression as a field
        if ("!=".equals(op)) {
            return DSL.condition("(" + expression + ") != ?", value);
        }

        if (">".equals(op)) {
            return DSL.condition("(" + expression + ") > ?", value);
        }

        if (">=".equals(op)) {
            return DSL.condition("(" + expression + ") >= ?", value);
        }

        if ("<".equals(op)) {
            return DSL.condition("(" + expression + ") < ?", value);
        }

        if ("<=".equals(op)) {
            return DSL.condition("(" + expression + ") <= ?", value);
        }

        if ("IN".equals(op)) {
            if (value instanceof Collection<?>) {
                Collection<?> values = (Collection<?>) value;
                // Build IN clause manually
                return DSL.condition("(" + expression + ") IN (" +
                                values.stream().map(v -> "?").collect(Collectors.joining(",")) + ")",
                        values.toArray());
            }
        }

        log.warn("Unsupported operator for expression filter: {}", op);
        return null;
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
//    private void addOrderBy(SelectQuery<?> query, SearchContext context) {
//        List<SortSpec> sortSpecs = context.getSort();
//
//        // Add explicit sort fields
//        if (sortSpecs != null) {
//            for (SortSpec sort : sortSpecs) {
//                Field<?> field = DSL.field(DSL.name(sort.getField()));
//                SortField<?> sortField = "DESC".equalsIgnoreCase(sort.getNormalizedDirection())
//                        ? field.desc()
//                        : field.asc();
//                query.addOrderBy(sortField);
//            }
//        }
//
//        // Add keyset columns for deterministic ordering
//        if (context.isPaginated() && context.getPaginationMode() == PaginationMode.KEYSET) {
//            for (String keysetCol : context.getKeysetColumns()) {
//                // Check if already in sort
//                boolean alreadySorted = sortSpecs != null && sortSpecs.stream()
//                        .anyMatch(s -> s.getField().equalsIgnoreCase(keysetCol));
//
//                if (!alreadySorted) {
//                    query.addOrderBy(DSL.field(DSL.name(keysetCol)).asc());
//                }
//            }
//        }
//    }

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
            // For DISTINCT queries, only add keyset columns that are in the SELECT list
            if (context.isDistinct()) {
                for (String keysetCol : context.getKeysetColumns()) {
                    // Check if the keyset column is already in the columns list
                    boolean isInSelect = context.getColumns().stream()
                            .anyMatch(col -> col.equalsIgnoreCase(keysetCol));
                    
                    // Check if already in sort
                    boolean alreadySorted = sortSpecs != null && sortSpecs.stream()
                            .anyMatch(s -> s.getField().equalsIgnoreCase(keysetCol));

                    if (isInSelect && !alreadySorted) {
                        query.addOrderBy(DSL.field(DSL.name(keysetCol)).asc());
                    }
                }
            } else {
                // Non-distinct queries can add all keyset columns
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
            // Changed by SRS 1110
            /*
            // Build count query directly with select
            Table<?> baseTable = DSL.table(DSL.name(context.getDataset().getBaseTable())).as("base");

            // Start with select count(*)
            SelectJoinStep<?> selectStep = dsl.selectCount().from(baseTable);
            */
            Table<?> baseTable = createBaseTable(context.getDataset());

            // Start with select count(*)
            SelectJoinStep<?> selectStep = dsl.selectCount().from(baseTable);
            // End Change

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


    // Added by SRS 1110
    /**
     * Adds GROUP BY clauses to the query.
     */
    private void addGroupBy(SelectQuery<?> query, SearchContext context) {
        if (context.getGroupBy() == null || context.getGroupBy().isEmpty()) {
            log.debug("No GROUP BY columns specified");
            return;
        }

        log.debug("Adding GROUP BY for columns: {}", context.getGroupBy());
        DatasetDefinition dataset = context.getDataset();

        for (String groupByColumn : context.getGroupBy()) {
            Field<?> field = null;

            // Check if it's a computed field
            if (dataset.getComputed() != null && dataset.getComputed().containsKey(groupByColumn)) {
                // Use computed field expression
                String expression = dataset.getComputed().get(groupByColumn);
                field = DSL.field(expression);
                log.debug("GROUP BY computed field {} -> {}", groupByColumn, expression);
            } else {
                // Regular column
                ColumnDefinition colDef = dataset.getColumns().get(groupByColumn);
                if (colDef != null) {
                    if (colDef.getSql().contains(".")) {
                        // Handle qualified column names (table.column)
                        String[] parts = colDef.getSql().split("\\.", 2);
                        field = DSL.field(DSL.name(parts[0], parts[1]));
                    } else {
                        field = DSL.field(DSL.name(colDef.getSql()));
                    }
                    log.debug("GROUP BY regular column {} -> {}", groupByColumn, colDef.getSql());
                } else {
                    // Check join columns
                    field = findJoinColumn(context, groupByColumn);
                    if (field != null) {
                        log.debug("GROUP BY join column {} found", groupByColumn);
                    } else {
                        log.warn("GROUP BY column not found: {}", groupByColumn);
                    }
                }
            }

            if (field != null) {
                query.addGroupBy(field);
                log.debug("Added GROUP BY: {}", groupByColumn);
            } else {
                log.warn("GROUP BY column not found: {}", groupByColumn);
            }
        }
    }
    // End Addition

    // Added SRS 1110
    /**
     * Creates the base table, supporting both simple table names and UNION queries.
     */
    private Table<?> createBaseTable(DatasetDefinition dataset) {
        String baseTableName = dataset.getBaseTable();

        // Check if this is a UNION query (contains UNION ALL)
        if (isUnionQuery(baseTableName)) {
            log.debug("Detected UNION query in baseTable: {}", baseTableName);
            return DSL.table(DSL.sql("(" + baseTableName + ")")).as("base");
        } else {
            // Regular table name
            return DSL.table(DSL.name(baseTableName)).as("base");
        }
    }

    /**
     * Checks if the baseTable string contains a UNION query.
     */
    private boolean isUnionQuery(String baseTable) {
        if (baseTable == null) {
            return false;
        }

        String upperCase = baseTable.toUpperCase().trim();
        return upperCase.contains("UNION ALL") || upperCase.contains("UNION");
    }
    // End Addition
}