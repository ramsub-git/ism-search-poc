package com.sephora.services.ismsearchpoc;

import com.sephora.services.ismsearchpoc.search.DatasetKey;
import com.sephora.services.ismsearchpoc.search.SearchRequest;
import com.sephora.services.ismsearchpoc.search.SearchResponse;
import com.sephora.services.ismsearchpoc.search.SortSpec;
import com.sephora.services.ismsearchpoc.search.service.SearchService;
import org.hibernate.query.SortDirection;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for Search Framework.
 * Converted from shell scripts, uses existing search-config.yml and same database as ProcessingFrameworkConsolidatedTest.
 */
@SpringBootTest(classes = SearchTestConfiguration.class)
@ActiveProfiles("test")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(SearchTestConfiguration.class)
@TestPropertySource(properties = {
        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
        // Uses existing search-config.yml from classpath
        "ism.search.config.hot-reload=false"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SearchFrameworkIntegrationTest {

    @Autowired
    private SearchService searchService;

    @Nested
    @DisplayName("Dropdown Filter Tests")
    class DropdownFilterTests {

        @Test
        @Order(1)
        @DisplayName("Region Dropdown - All Regions")
        void testRegionDropdown() {
            SearchRequest request = SearchRequest.builder()
                    .view("RegionDropdown")
                    .paginate(false)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.LOCATION_MASTER, request);

            assertThat(response).isNotNull();
            assertThat(response.getData()).isNotEmpty();
        }

        @Test
        @Order(2)
        @DisplayName("Store Dropdown - All Stores")
        void testStoreDropdownAll() {
            SearchRequest request = SearchRequest.builder()
                    .view("StoreDropdown")
                    .paginate(false)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.LOCATION_MASTER, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(3)
        @DisplayName("Store Dropdown - Filtered by Region")
        void testStoreDropdownFilteredByRegion() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("regionNumberIn", Arrays.asList(1, 2));

            SearchRequest request = SearchRequest.builder()
                    .view("StoreDropdown")
                    .filters(filters)
                    .paginate(false)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.LOCATION_MASTER, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(4)
        @DisplayName("District Dropdown - All Districts")
        void testDistrictDropdownAll() {
            SearchRequest request = SearchRequest.builder()
                    .view("DistrictDropdown")
                    .paginate(false)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.LOCATION_MASTER, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(5)
        @DisplayName("District Dropdown - Filtered by Region")
        void testDistrictDropdownFilteredByRegion() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("regionNumberIn", Collections.singletonList(1));

            SearchRequest request = SearchRequest.builder()
                    .view("DistrictDropdown")
                    .filters(filters)
                    .paginate(false)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.LOCATION_MASTER, request);

            assertThat(response).isNotNull();
        }

//        @Test
//        @Order(8)
//        @DisplayName("Reserve Search - Active Reserves by Channel & Status")
//        void testReserveSearch() {
//            Map<String, Object> filters = new HashMap<>();
//            filters.put("channel", "D");             // matches filters.channel in rsvehr.yml
//            filters.put("reservationStatus", "A");   // matches filters.reservationStatus
//
//            SearchRequest request = SearchRequest.builder()
//                    .view("ActiveReserves")          // matches view in rsvehr.yml
//                    .filters(filters)
//                    // Let the default sort from config apply (lastModified desc)
//                    .size(10)
//                    .build();
//
//            SearchResponse<Map<String, Object>> response = searchService.search(
//                    DatasetKey.RSVEHR, request);
//
//            assertThat(response).isNotNull();
//        }


        @Test
        @Order(7)
        @DisplayName("Paginated Dropdown")
        void testPaginatedDropdown() {
            SearchRequest request = SearchRequest.builder()
                    .view("RegionDropdown")
                    .paginate(true)
                    .size(10)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.LOCATION_MASTER, request);

            assertThat(response).isNotNull();
            assertThat(response.getPageInfo().isPaginate()).isTrue();
        }

        @Test
        @Order(8)
        @DisplayName("Ad-hoc Distinct Query")
        void testAdHocDistinctQuery() {
            SearchRequest request = SearchRequest.builder()
                    .columns(Arrays.asList("region_number", "region_name"))
                    .distinct(true)
                    .sort(Collections.singletonList(
                            SortSpec.builder()
                                    .field("region_name")
                                    .direction("ASC")
                                    .build()
                    ))
                    .paginate(false)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.LOCATION_MASTER, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(9)
        @DisplayName("Empty Filter Result")
        void testEmptyFilterResult() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("regionNumberIn", Collections.singletonList(99999));

            SearchRequest request = SearchRequest.builder()
                    .view("StoreDropdown")
                    .filters(filters)
                    .paginate(false)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.LOCATION_MASTER, request);

            assertThat(response).isNotNull();
        }
    }

    @Nested
    @DisplayName("Search Feature Tests")
    class SearchFeatureTests {

        @Test
        @Order(1)
        @DisplayName("Computed Fields - E-commerce ATS")
        void testComputedFields() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("locationNumberIn", Arrays.asList(1001, 1002, 1003));
            filters.put("availableQtyGte", 10);

            SearchRequest request = SearchRequest.builder()
                    .view("EcommATS")
                    .filters(filters)
                    .size(5)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.SKULOC, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(2)
        @DisplayName("Joins - Location Details")
        void testJoins() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("locationNumberIn", Arrays.asList(1001, 1002));
            filters.put("skuIdIn", Arrays.asList(10001, 10002, 10003));

            SearchRequest request = SearchRequest.builder()
                    .view("LocationView")
                    .filters(filters)
                    .size(5)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.SKULOC, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(3)
        @DisplayName("Complex Filters")
        void testComplexFilters() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("locationNumber", 1001);
            filters.put("skuIdIn", Arrays.asList(10001, 10002, 10003, 10004, 10005));

            SearchRequest request = SearchRequest.builder()
                    .view("StoreBackroom")
                    .filters(filters)
                    .paginate(false)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.SKULOC, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(4)
        @DisplayName("Ad-hoc Mode with Custom Columns")
        void testAdHocMode() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("locationNumberIn", Arrays.asList(1001, 1002));
            filters.put("availableQtyGte", 50);

            SearchRequest request = SearchRequest.builder()
                    .columns(Arrays.asList("sku_id", "location_number", "available_qty", "ecomm_pick_reserve"))
                    .filters(filters)
                    .sort(Arrays.asList(
                            SortSpec.builder()
                                    .field("available_qty")
                                    .direction("DESC")
                                    .build(),
                            SortSpec.builder()
                                    .field("sku_id")
                                    .direction("ASC")
                                    .build()
                    ))
                    .size(10)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.SKULOC, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(5)
        @DisplayName("Ad-hoc with Inline Computed Fields")
        void testAdHocWithInlineComputed() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("locationNumber", 1001);

            Map<String, String> computed = new HashMap<>();
            computed.put("totalReserve", "ecomm_pick_reserve + ecomm_pack_reserve + merch_reserve_qty");

            SearchRequest request = SearchRequest.builder()
                    .columns(Arrays.asList("sku_id", "location_number", "available_qty", "totalReserve"))
                    .computed(computed)
                    .filters(filters)
                    .size(5)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.SKULOC, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(6)
        @DisplayName("Different Sort Orders")
        void testSorting() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("locationNumberIn", Arrays.asList(1001, 1002, 1003));

            SearchRequest request = SearchRequest.builder()
                    .view("ReserveAudit")
                    .filters(filters)
                    .sort(Collections.singletonList(
                            SortSpec.builder()
                                    .field("totalReserve")
                                    .direction("DESC")
                                    .build()
                    ))
                    .size(5)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.SKULOC, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(7)
        @DisplayName("Location Master Search")
        void testLocationMasterSearch() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("locationTypeIn", Collections.singletonList("STORE"));
            filters.put("country", "US");

            SearchRequest request = SearchRequest.builder()
                    .view("StoreDirectory")
                    .filters(filters)
                    .size(10)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.LOCATION_MASTER, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(6)
        @DisplayName("Location Type Dropdown")
        void testLocationTypeDropdown() {
            SearchRequest request = SearchRequest.builder()
                    .view("LocationTypeDropdown")   // matches location_master.yml
                    .paginate(false)                // allowUnpaginated: true in config
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.LOCATION_MASTER, request);

            assertThat(response).isNotNull();
        }


        @Test
        @Order(9)
        @DisplayName("Time-based Filter")
        void testTimeBasedFilter() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("updatedAfter", "2024-01-01T00:00:00Z");

            SearchRequest request = SearchRequest.builder()
                    .columns(Arrays.asList("sku_id", "location_number", "available_qty", "updated_ts"))
                    .filters(filters)
                    .sort(Collections.singletonList(
                            SortSpec.builder()
                                    .field("updated_ts")
                                    .direction("DESC")
                                    .build()
                    ))
                    .size(5)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.SKULOC, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(10)
        @DisplayName("Unpaginated Results")
        void testUnpaginatedResults() {
            Map<String, Object> filters = new HashMap<>();
            filters.put("locationNumber", 1001);

            SearchRequest request = SearchRequest.builder()
                    .view("StoreBackroom")
                    .filters(filters)
                    .paginate(false)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.SKULOC, request);

            assertThat(response).isNotNull();
        }

        @Test
        @Order(11)
        @DisplayName("Large IN Clause")
        void testLargeInClause() {
            List<Integer> skuIds = new ArrayList<>();
            for (int i = 10001; i <= 10100; i++) {
                skuIds.add(i);
            }

            Map<String, Object> filters = new HashMap<>();
            filters.put("skuIdIn", skuIds);
            filters.put("locationNumberIn", Collections.singletonList(1001));

            SearchRequest request = SearchRequest.builder()
                    .view("EcommATS")
                    .filters(filters)
                    .size(10)
                    .build();

            SearchResponse<Map<String, Object>> response = searchService.search(
                    DatasetKey.SKULOC, request);

            assertThat(response).isNotNull();
        }
    }
}
