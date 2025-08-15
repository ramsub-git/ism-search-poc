package com.sephora.services.ismsearchpoc.search.exception;

/**
 * Base exception for all search-related errors.
 * Provides structured error information for API responses.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
public class SearchException extends RuntimeException {

    /**
     * Error code for programmatic handling.
     */
    private final String errorCode;

    /**
     * HTTP status code suggestion.
     */
    private final int suggestedStatus;

    /**
     * Constructs a new search exception.
     *
     * @param errorCode programmatic error code
     * @param message human-readable message
     * @param suggestedStatus suggested HTTP status code
     */
    public SearchException(String errorCode, String message, int suggestedStatus) {
        super(message);
        this.errorCode = errorCode;
        this.suggestedStatus = suggestedStatus;
    }

    /**
     * Constructs a new search exception with cause.
     *
     * @param errorCode programmatic error code
     * @param message human-readable message
     * @param suggestedStatus suggested HTTP status code
     * @param cause underlying cause
     */
    public SearchException(String errorCode, String message, int suggestedStatus, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.suggestedStatus = suggestedStatus;
    }

    /**
     * Gets the error code.
     *
     * @return error code
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Gets the suggested HTTP status code.
     *
     * @return HTTP status code
     */
    public int getSuggestedStatus() {
        return suggestedStatus;
    }
}