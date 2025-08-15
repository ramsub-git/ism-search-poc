package com.sephora.services.ismsearchpoc.search.exception;

/**
 * Exception thrown when an invalid filter is used in a request.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
public class InvalidFilterException extends SearchException {

    /**
     * Constructs exception for unknown filter.
     *
     * @param filterName the invalid filter name
     * @param viewName the view name (may be null for ad-hoc)
     */
    public InvalidFilterException(String filterName, String viewName) {
        super("INVALID_FILTER",
                viewName != null
                        ? String.format("Filter '%s' not allowed for view '%s'", filterName, viewName)
                        : String.format("Unknown filter '%s'", filterName),
                400);
    }

    /**
     * Constructs exception for invalid filter value.
     *
     * @param filterName the filter name
     * @param message specific error message
     * @param cause underlying cause
     */
    public InvalidFilterException(String filterName, String message, Throwable cause) {
        super("INVALID_FILTER",
                String.format("Invalid value for filter '%s': %s", filterName, message),
                400,
                cause);
    }
}