package com.sephora.services.ismsearchpoc.search.exception;

/**
 * Exception thrown when query size limits are exceeded.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
public class QuerySizeException extends SearchException {

    /**
     * Constructs exception for page size limit.
     *
     * @param requested requested size
     * @param maximum maximum allowed
     */
    public static QuerySizeException pageSizeTooLarge(int requested, int maximum) {
        return new QuerySizeException(
                "PAGE_SIZE_TOO_LARGE",
                String.format("Requested page size %d exceeds maximum %d", requested, maximum)
        );
    }

    /**
     * Constructs exception for result set limit.
     *
     * @param resultCount actual result count
     * @param maximum maximum allowed
     */
    public static QuerySizeException resultTooLarge(long resultCount, int maximum) {
        return new QuerySizeException(
                "RESULT_TOO_LARGE",
                String.format("Result set of %d rows exceeds unpaginated limit of %d. Use pagination.",
                        resultCount, maximum)
        );
    }

    /**
     * Constructs exception for filter list size.
     *
     * @param filterName filter name
     * @param size list size
     * @param maximum maximum allowed
     */
    public static QuerySizeException filterListTooLarge(String filterName, int size, int maximum) {
        return new QuerySizeException(
                "FILTER_LIST_TOO_LARGE",
                String.format("Filter '%s' has %d values, exceeds maximum %d", filterName, size, maximum)
        );
    }

    /**
     * Private constructor.
     *
     * @param errorCode error code
     * @param message error message
     */
    private QuerySizeException(String errorCode, String message) {
        super(errorCode, message, 400);
    }
}