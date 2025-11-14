package com.sephora.services.ismsearchpoc.search;

/**
 * Enumeration of available datasets for the search API.
 * Each dataset corresponds to a database table with its own set of views and configurations.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
public enum DatasetKey {
    /**
     * SKU Location inventory data.
     * Primary table for inventory availability and reserve information.
     */
    SKULOC,

    /**
     * Location master data.
     * Contains store and warehouse information.
     */
    LOCATION_MASTER,

    /**
     * Reserve calculation results.
     * Historical data from reserve calculation runs.
     */
    RESERVE_CALC_RESULT,

    /**
     * Reserve header records.
     * Active and historical reserve information.
     */
    RSVEHR,


    /**
     * Logical reserve maintenance dataset combining hard and soft reserves.
     * Backed by the UNION query in reserve_maintenance.yml.
     */
    RESERVE_MAINTENANCE
}