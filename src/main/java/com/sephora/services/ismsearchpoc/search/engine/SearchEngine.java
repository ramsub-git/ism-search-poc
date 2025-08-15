package com.sephora.services.ismsearchpoc.search.engine;

import com.sephora.services.ismsearchpoc.search.SearchResponse;
import com.sephora.services.ismsearchpoc.search.service.SearchContext;

import java.util.Map;

/**
 * Interface for search engine implementations.
 * Executes queries based on resolved search context.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
public interface SearchEngine {

    /**
     * Executes a search query.
     *
     * @param context validated search context
     * @return search results
     * @throws Exception if query execution fails
     */
    SearchResponse<Map<String, Object>> execute(SearchContext context) throws Exception;
}