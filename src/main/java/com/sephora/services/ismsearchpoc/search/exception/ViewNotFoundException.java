package com.sephora.services.ismsearchpoc.search.exception;

import java.util.List;

/**
 * Exception thrown when a requested view does not exist.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
public class ViewNotFoundException extends SearchException {

    /**
     * List of valid view names for the dataset.
     */
    private final List<String> validViews;

    /**
     * Constructs a new view not found exception.
     *
     * @param viewName the requested view name
     * @param dataset the dataset name
     * @param validViews list of valid view names
     */
    public ViewNotFoundException(String viewName, String dataset, List<String> validViews) {
        super("UNKNOWN_VIEW",
                String.format("View '%s' not found for dataset %s", viewName, dataset),
                400);
        this.validViews = validViews;
    }

    /**
     * Gets the list of valid views.
     *
     * @return valid view names
     */
    public List<String> getValidViews() {
        return validViews;
    }
}