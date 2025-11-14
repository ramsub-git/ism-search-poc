package com.sephora.services.ismsearchpoc;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@ComponentScan(basePackages = {
        "com.sephora.services.ismsearchpoc.search"
})
@EntityScan(basePackages = {
        "com.sephora.services.ismsearchpoc.search"
})
@EnableJpaRepositories(basePackages = {
        "com.sephora.services.ismsearchpoc.search"
})
public class SearchTestConfiguration {
    // No additional beans – Boot auto-config + application-test.yaml
    // will provide DataSource, ObjectMapper, etc.
}
