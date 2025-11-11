package com.sephora.services.processing;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Minimal Spring configuration for testing the Processing Framework in isolation.
 * This configuration only loads the components needed for processing framework tests,
 * avoiding service-specific beans like schedulers, controllers, or business services.
 * <p>
 * Usage: Copy this entire processing folder to any repo and tests will work without
 * modification (just update package names).
 */

/**
 * Minimal Spring Boot configuration for testing ONLY the processing framework
 * in isolation from the full finance application context.
 * <p>
 * This configuration:
 * 1. Only scans the processing package and its components
 * 2. Only loads JPA entities from the processing package
 * 3. Only enables repositories from the processing package
 * 4. Excludes all other finance service components (schedulers, kafka consumers, etc.)
 */

@SpringBootApplication
@ComponentScan(basePackages = {"com.sephora.services.ismsearchpoc.processing"
})
@EntityScan(basePackages = {"com.sephora.services.ismsearchpoc.processing"})
@EnableJpaRepositories(basePackages = {"com.sephora.services.ismsearchpoc.processing"})
public class ProcessingTestConfiguration {

    // This configuration class needs no additional beans.
    // All processing framework components will be auto-discovered and wired
    // through the annotations above:
    //
    // @EnableJpaRepositories - Discovers ProcessingAuditLogRepository, ProcessingErrorLogRepository
    // @EntityScan - Scans ProcessingAuditLogEntry, ProcessingErrorLogEntry entities
    // @ComponentScan - Discovers ProcessingAuditService, ProcessingErrorService, ProcessingAuditHelper
    // @EnableConfigurationProperties - Loads ProcessingLoggingProperties from application properties
    //
    // The @DataJpaTest annotation in the test class will provide:
    // - In-memory H2 database (or configured test database)
    // - JPA/Hibernate configuration
    // - Transaction management
    // - Repository layer testing support
}