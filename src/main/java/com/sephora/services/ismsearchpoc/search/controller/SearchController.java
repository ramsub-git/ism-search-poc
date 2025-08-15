package com.sephora.services.ismsearchpoc.search.controller;

import com.sephora.services.ismsearchpoc.search.*;
import com.sephora.services.ismsearchpoc.search.exception.*;
import com.sephora.services.ismsearchpoc.search.service.SearchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for search API endpoints.
 * Provides dataset-specific search functionality with view support.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
@Slf4j
@RestController
@RequestMapping("/api/search")
@RequiredArgsConstructor
@Validated
public class SearchController {

    private final SearchService searchService;

    /**
     * Search SKULOC dataset.
     * Primary endpoint for inventory and availability queries.
     *
     * @param request search request
     * @return search results
     */
    @PostMapping("/skuloc")
    public ResponseEntity<SearchResponse<Map<String, Object>>> searchSkuloc(
            @Valid @RequestBody SearchRequest request) {

        log.info("SKULOC search request: view={}, filters={}",
                request.getView(),
                request.getFilters() != null ? request.getFilters().keySet() : null);

        try {
            SearchResponse<Map<String, Object>> response =
                    searchService.search(DatasetKey.SKULOC, request);

            return ResponseEntity.ok(response);

        } catch (SearchException e) {
            log.warn("SKULOC search failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error in SKULOC search", e);
            throw new SearchException("INTERNAL_ERROR",
                    "An unexpected error occurred",
                    500, e);
        }
    }

    /**
     * Search Location Master dataset.
     * For location and store information queries.
     *
     * @param request search request
     * @return search results
     */
    @PostMapping("/location")
    public ResponseEntity<SearchResponse<Map<String, Object>>> searchLocation(
            @Valid @RequestBody SearchRequest request) {

        log.info("Location search request: view={}, filters={}",
                request.getView(),
                request.getFilters() != null ? request.getFilters().keySet() : null);

        try {
            SearchResponse<Map<String, Object>> response =
                    searchService.search(DatasetKey.LOCATION_MASTER, request);

            return ResponseEntity.ok(response);

        } catch (SearchException e) {
            log.warn("Location search failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error in location search", e);
            throw new SearchException("INTERNAL_ERROR",
                    "An unexpected error occurred",
                    500, e);
        }
    }

    /**
     * Search Reserve dataset.
     * For reserve calculation results and audit queries.
     *
     * @param request search request
     * @return search results
     */
    @PostMapping("/reserve")
    public ResponseEntity<SearchResponse<Map<String, Object>>> searchReserve(
            @Valid @RequestBody SearchRequest request) {

        log.info("Reserve search request: view={}, filters={}",
                request.getView(),
                request.getFilters() != null ? request.getFilters().keySet() : null);

        try {
            SearchResponse<Map<String, Object>> response =
                    searchService.search(DatasetKey.RSVEHR, request);

            return ResponseEntity.ok(response);

        } catch (SearchException e) {
            log.warn("Reserve search failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error in reserve search", e);
            throw new SearchException("INTERNAL_ERROR",
                    "An unexpected error occurred",
                    500, e);
        }
    }

    /**
     * Generic search endpoint by dataset name.
     * Alternative to dataset-specific endpoints.
     *
     * @param dataset dataset name
     * @param request search request
     * @return search results
     */
    @PostMapping("/{dataset}")
    public ResponseEntity<SearchResponse<Map<String, Object>>> searchDataset(
            @PathVariable("dataset") String dataset,
            @Valid @RequestBody SearchRequest request) {

        log.info("Generic search request for dataset {}: view={}, filters={}",
                dataset,
                request.getView(),
                request.getFilters() != null ? request.getFilters().keySet() : null);

        try {
            // Parse dataset key
            DatasetKey datasetKey;
            try {
                datasetKey = DatasetKey.valueOf(dataset.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new SearchException("UNKNOWN_DATASET",
                        "Unknown dataset: " + dataset,
                        404);
            }

            SearchResponse<Map<String, Object>> response =
                    searchService.search(datasetKey, request);

            return ResponseEntity.ok(response);

        } catch (SearchException e) {
            log.warn("Search failed for dataset {}: {}", dataset, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error in search for dataset " + dataset, e);
            throw new SearchException("INTERNAL_ERROR",
                    "An unexpected error occurred",
                    500, e);
        }
    }

    /**
     * Exception handler for search-specific exceptions.
     *
     * @param e search exception
     * @return error response
     */
    @ExceptionHandler(SearchException.class)
    public ResponseEntity<Map<String, Object>> handleSearchException(SearchException e) {
        Map<String, Object> error = new HashMap<>();
        error.put("error", e.getErrorCode());
        error.put("message", e.getMessage());
        error.put("timestamp", System.currentTimeMillis());

        // Add additional context for specific errors
        if (e instanceof ViewNotFoundException) {
            ViewNotFoundException vnfe = (ViewNotFoundException) e;
            error.put("validViews", vnfe.getValidViews());
        }

        return ResponseEntity.status(e.getSuggestedStatus()).body(error);
    }

    /**
     * Exception handler for validation errors.
     *
     * @param e validation exception
     * @return error response
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleValidationException(IllegalArgumentException e) {
        Map<String, Object> error = new HashMap<>();
        error.put("error", "VALIDATION_ERROR");
        error.put("message", e.getMessage());
        error.put("timestamp", System.currentTimeMillis());

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    /**
     * Global exception handler for unexpected errors.
     *
     * @param e exception
     * @return error response
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneralException(Exception e) {
        log.error("Unexpected error in search controller", e);

        Map<String, Object> error = new HashMap<>();
        error.put("error", "INTERNAL_ERROR");
        error.put("message", "An unexpected error occurred");
        error.put("timestamp", System.currentTimeMillis());

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
    }
}