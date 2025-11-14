package com.sephora.services.ismsearchpoc;

import com.sephora.services.ismsearchpoc.SearchTestConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = SearchTestConfiguration.class)
@ActiveProfiles("test")
class IsmSearchPocApplicationTests {

	@Test
	void contextLoads() {
		// This will now work with SearchTestConfiguration!
	}
}
