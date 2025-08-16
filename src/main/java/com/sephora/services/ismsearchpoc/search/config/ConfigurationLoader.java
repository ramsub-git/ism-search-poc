package com.sephora.services.ismsearchpoc.search.config;

import com.sephora.services.ismsearchpoc.search.DatasetKey;
import com.sephora.services.ismsearchpoc.search.exception.SearchException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.yaml.snakeyaml.LoaderOptions;

/**
 * Loads and manages search configuration from YAML files.
 * Provides caching and hot-reload capabilities in development.
 * Enhanced to support multi-file configuration loading.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConfigurationLoader {

    /**
     * Path to configuration file or directory.
     * Default: classpath:search-config.yml
     * For multi-file: classpath:search-config/
     */
    @Value("${ism.search.config.path:classpath:search-config.yml}")
    private String configPath;

    /**
     * Whether to enable hot reload in development.
     */
    @Value("${ism.search.config.hot-reload:false}")
    private boolean hotReloadEnabled;

    /**
     * Current loaded configuration.
     */
    private volatile SearchConfiguration configuration;

    /**
     * Last modified timestamp for hot reload.
     */
    private long lastModified = 0;

    /**
     * Resource pattern resolver for finding config files.
     */
    private final PathMatchingResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();

    /**
     * Initializes the configuration loader.
     * Loads initial configuration on startup.
     */
    @PostConstruct
    public void initialize() {
        log.info("Initializing search configuration from: {}", configPath);

        try {
            loadConfiguration();
            log.info("Search configuration loaded successfully");
        } catch (Exception e) {
            log.error("Failed to load search configuration", e);
            throw new SearchException("CONFIG_LOAD_ERROR",
                    "Failed to load search configuration",
                    500, e);
        }
    }

    /**
     * Gets the current configuration.
     * Checks for updates if hot reload is enabled.
     *
     * @return current search configuration
     */
    public SearchConfiguration getConfiguration() {
        // Check for hot reload in development
        if (hotReloadEnabled) {
            try {
                // Check if any config file has been modified
                boolean needsReload = false;

                if (configPath.endsWith("/")) {
                    // Directory mode - check all YAML files
                    Resource[] resources = resourceResolver.getResources(configPath + "**/*.yml");
                    for (Resource resource : resources) {
                        if (resource.exists() && resource.lastModified() > lastModified) {
                            needsReload = true;
                            break;
                        }
                    }
                } else {
                    // Single file mode
                    Resource resource = resourceResolver.getResource(configPath);
                    if (resource.exists() && resource.lastModified() > lastModified) {
                        needsReload = true;
                    }
                }

                if (needsReload) {
                    log.info("Configuration changed, reloading");
                    loadConfiguration();
                }
            } catch (IOException e) {
                log.warn("Failed to check configuration modification", e);
            }
        }

        return configuration;
    }

    /**
     * Loads configuration from the YAML file(s).
     *
     * @throws IOException if file cannot be read
     */
    private void loadConfiguration() throws IOException {
        log.debug("Loading configuration from: {}", configPath);

        SearchConfiguration newConfig = null;

        try {
            if (configPath.endsWith("/")) {
                // Multi-file mode
                newConfig = loadMultiFileConfiguration();
            } else {
                // Single file mode (backward compatible)
                newConfig = loadSingleFileConfiguration();
            }

            // Validate
            if (newConfig == null) {
                throw new IllegalStateException("Configuration is empty");
            }

            newConfig.validate();

            // Update configuration atomically
            this.configuration = newConfig;
            updateLastModified();

            log.info("Loaded {} datasets from configuration",
                    newConfig.getDatasets() != null ? newConfig.getDatasets().size() : 0);

        } catch (Exception e) {
            log.error("Failed to parse configuration", e);
            throw new SearchException("CONFIG_PARSE_ERROR",
                    "Failed to parse configuration: " + e.getMessage(),
                    500, e);
        }
    }

    /**
     * Loads configuration from a single file (backward compatible).
     */
    private SearchConfiguration loadSingleFileConfiguration() throws IOException {
        Resource resource = resourceResolver.getResource(configPath);

        try (InputStream input = resource.getInputStream()) {
            Yaml yaml = new Yaml(new Constructor(SearchConfiguration.class, new LoaderOptions()));
            return yaml.load(input);
        }
    }

    /**
     * Loads configuration from multiple files in a directory.
     */
    /**
     * Loads configuration from multiple files in a directory.
     */
    private SearchConfiguration loadMultiFileConfiguration() throws IOException {
        SearchConfiguration mergedConfig = new SearchConfiguration();
        Map<DatasetKey, DatasetDefinition> allDatasets = new HashMap<>();

        // First, check for main config file
        Resource mainConfig = resourceResolver.getResource(configPath + "search-config.yml");
        if (mainConfig.exists()) {
            log.debug("Loading main configuration file");
            try (InputStream input = mainConfig.getInputStream()) {
                Yaml yaml = new Yaml(new Constructor(SearchConfiguration.class, new LoaderOptions()));
                mergedConfig = yaml.load(input);
                if (mergedConfig != null && mergedConfig.getDatasets() != null) {
                    allDatasets.putAll(mergedConfig.getDatasets());
                }
            }
        }

        // Load dataset files
        Resource[] datasetFiles = resourceResolver.getResources(configPath + "datasets/*.yml");
        log.info("Found {} dataset configuration files", datasetFiles.length);

        for (Resource datasetFile : datasetFiles) {
            log.debug("Loading dataset configuration from: {}", datasetFile.getFilename());

            try (InputStream input = datasetFile.getInputStream()) {
                Yaml yaml = new Yaml(new Constructor(SearchConfiguration.class, new LoaderOptions()));
                SearchConfiguration datasetConfig = yaml.load(input);

                if (datasetConfig != null && datasetConfig.getDatasets() != null) {
                    // Merge datasets
                    for (Map.Entry<DatasetKey, DatasetDefinition> entry : datasetConfig.getDatasets().entrySet()) {
                        if (allDatasets.containsKey(entry.getKey())) {
                            log.warn("Dataset {} defined in multiple files, using definition from {}",
                                    entry.getKey(), datasetFile.getFilename());
                        }
                        allDatasets.put(entry.getKey(), entry.getValue());
                    }
                }
            } catch (Exception e) {
                log.error("Failed to load dataset file: {}", datasetFile.getFilename(), e);
                throw new SearchException("CONFIG_PARSE_ERROR",
                        "Failed to parse dataset file: " + datasetFile.getFilename(),
                        500, e);
            }
        }

        // Set merged datasets
        mergedConfig.setDatasets(allDatasets);

        return mergedConfig;
    }
    
    /**
     * Updates the last modified timestamp.
     */
    private void updateLastModified() throws IOException {
        long maxModified = 0;

        if (configPath.endsWith("/")) {
            // Directory mode
            Resource[] resources = resourceResolver.getResources(configPath + "**/*.yml");
            for (Resource resource : resources) {
                if (resource.exists()) {
                    maxModified = Math.max(maxModified, resource.lastModified());
                }
            }
        } else {
            // Single file mode
            Resource resource = resourceResolver.getResource(configPath);
            if (resource.exists()) {
                maxModified = resource.lastModified();
            }
        }

        this.lastModified = maxModified;
    }

    /**
     * Reloads configuration manually.
     * Useful for administrative operations.
     *
     * @throws IOException if reload fails
     */
    public void reload() throws IOException {
        log.info("Manual configuration reload requested");
        loadConfiguration();
    }
}