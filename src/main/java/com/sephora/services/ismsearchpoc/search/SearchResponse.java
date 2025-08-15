package com.sephora.services.ismsearchpoc.search;

import lombok.*;
import java.util.List;
import java.util.Map;

/**
 * Generic search response containing results and metadata.
 *
 * @param <T> Type of items in the result set
 * @author ISM Foundation Team
 * @since 1.0
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class SearchResponse<T> {

    /**
     * List of result items.
     * Each item is typically a Map<String, Object> representing a row.
     */
    private List<T> data;

    /**
     * Pagination information including cursors and counts.
     */
    private PageInfo pageInfo;

    /**
     * Response metadata.
     * May include:
     * - view: Name of view used
     * - dataset: Dataset queried
     * - tookMs: Query execution time
     * - warnings: Any non-fatal issues
     */
    @Builder.Default
    private Map<String, Object> meta = Map.of();

    /**
     * Convenience method to check if response has data.
     *
     * @return true if data is not null and not empty
     */
    public boolean hasData() {
        return data != null && !data.isEmpty();
    }

    /**
     * Convenience method to get result count.
     *
     * @return number of items in this response
     */
    public int getCount() {
        return data != null ? data.size() : 0;
    }

    /**
     * Convenience method to check if more pages available.
     *
     * @return true if there are more results to fetch
     */
    public boolean hasMore() {
        return pageInfo != null && pageInfo.isHasMore();
    }

    /**
     * Convenience method to get next cursor for pagination.
     *
     * @return next cursor or null if no more pages
     */
    public String getNextCursor() {
        return pageInfo != null ? pageInfo.getNextCursor() : null;
    }
}