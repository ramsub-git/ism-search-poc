package com.sephora.services.ismsearchpoc.search;

/**
 * Pagination modes supported by the search API.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
public enum PaginationMode {
    /**
     * Keyset (cursor-based) pagination.
     * Provides stable results and efficient performance for large datasets.
     * Recommended for most use cases.
     */
    KEYSET,

    /**
     * Traditional offset-based pagination.
     * May have performance issues with large offsets.
     * Provided for backward compatibility.
     */
    OFFSET
}