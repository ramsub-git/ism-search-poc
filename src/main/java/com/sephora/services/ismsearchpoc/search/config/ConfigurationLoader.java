package com.sephora.services.ismsearchpoc.search.config;

import com.sephora.services.ismsearchpoc.search.exception.SearchException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import org.yaml.snakeyaml.LoaderOptions;

/**
 * Loads and manages search configuration from YAML files.
 * Provides caching and hot-reload capabilities in development.
 *
 * @author ISM Foundation Team
 * @since 1.0
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConfigurationLoader {

    /**
     * Path to configuration file.
     * Default: classpath:search-config.yml
     */
    @Value("${ism.search.config.path:classpath:search-config.yml}")
    private Resource configResource;

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
     * Initializes the configuration loader.
     * Loads initial configuration on startup.
     */
    @PostConstruct
    public void initialize() {
        log.info("Initializing search configuration from: {}", configResource);

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
                long currentModified = configResource.lastModified();
                if (currentModified > lastModified) {
                    log.info("Configuration file changed, reloading");
                    loadConfiguration();
                }
            } catch (IOException e) {
                log.warn("Failed to check configuration file modification", e);
            }
        }

        return configuration;
    }

    /**
     * Loads configuration from the YAML file.
     *
     * @throws IOException if file cannot be read
     */
    private void loadConfiguration() throws IOException {
        log.debug("Loading configuration from: {}", configResource.getFilename());

        try (InputStream input = configResource.getInputStream()) {
            // Create YAML parser with custom constructor
            Yaml yaml = new Yaml(new Constructor(SearchConfiguration.class, new LoaderOptions()));
            // Parse configuration
            SearchConfiguration newConfig = yaml.load(input);

            // Validate
            if (newConfig == null) {
                throw new IllegalStateException("Configuration file is empty");
            }

            newConfig.validate();

            // Update configuration atomically
            this.configuration = newConfig;
            this.lastModified = configResource.lastModified();

            log.info("Loaded {} datasets from configuration",
                    newConfig.getDatasets() != null ? newConfig.getDatasets().size() : 0);

        } catch (Exception e) {
            log.error("Failed to parse configuration file", e);
            throw new SearchException("CONFIG_PARSE_ERROR",
                    "Failed to parse configuration: " + e.getMessage(),
                    500, e);
        }
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