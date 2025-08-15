package com.sephora.services.ismsearchpoc.search.config;

import com.sephora.services.ismsearchpoc.search.DatasetKey;
import lombok.*;
import java.util.Map;

/**
 * Root configuration object for search datasets.
 * Loaded from YAML configuration files.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class SearchConfiguration {

    /**
     * Map of dataset configurations by key.
     */
    private Map<DatasetKey, DatasetDefinition> datasets;

    /**
     * Validates all dataset configurations.
     *
     * @throws IllegalStateException if validation fails
     */
    public void validate() {
        if (datasets == null || datasets.isEmpty()) {
            throw new IllegalStateException("No datasets configured");
        }

        datasets.forEach((key, dataset) -> {
            try {
                dataset.validate();
            } catch (Exception e) {
                throw new IllegalStateException("Invalid dataset " + key + ": " + e.getMessage(), e);
            }
        });
    }

    /**
     * Gets a dataset definition by key.
     *
     * @param key dataset key
     * @return dataset definition or null if not found
     */
    public DatasetDefinition getDataset(DatasetKey key) {
        return datasets != null ? datasets.get(key) : null;
    }

    /**
     * Checks if a dataset exists.
     *
     * @param key dataset key
     * @return true if dataset is configured
     */
    public boolean hasDataset(DatasetKey key) {
        return datasets != null && datasets.containsKey(key);
    }
}