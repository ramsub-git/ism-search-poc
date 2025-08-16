#!/bin/bash

# ISM Search Framework Feature Testing Script - CORRECTED
# Tests various search capabilities beyond basic queries

echo "================================="
echo "ISM Search Framework Feature Test"
echo "================================="
echo ""

# Base URL for the API
BASE_URL="http://localhost:8080/api/search"

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to make API call and pretty print
test_search() {
    local endpoint=$1
    local description=$2
    local data=$3
    
    echo -e "${BLUE}Test: ${description}${NC}"
    echo "Endpoint: POST ${BASE_URL}${endpoint}"
    echo "Request:"
    echo "$data" | jq .
    echo ""
    echo "Response:"
    
    response=$(curl -s -X POST "${BASE_URL}${endpoint}" \
        -H "Content-Type: application/json" \
        -d "$data")
    
    if [ $? -eq 0 ]; then
        echo "$response" | jq . 2>/dev/null || echo "$response"
        echo -e "${GREEN}✓ Success${NC}"
    else
        echo -e "${RED}✗ Failed${NC}"
    fi
    echo "================================="
    echo ""
}

# 1. Test Computed Fields
echo -e "${GREEN}=== 1. COMPUTED FIELDS TEST ===${NC}"

test_search "/skuloc" "E-commerce ATS with computed field" '{
  "view": "EcommATS",
  "filters": {
    "locationNumberIn": [1001, 1002, 1003],
    "availableQtyGte": 10
  },
  "size": 5
}'

# 2. Test Joins (LocationView includes location details)
echo -e "${GREEN}=== 2. JOIN TEST - Location Details ===${NC}"

test_search "/skuloc" "SKULOC with Location Join" '{
  "view": "LocationView",
  "filters": {
    "locationNumberIn": [1001, 1002],
    "skuIdIn": [10001, 10002, 10003]
  },
  "size": 5
}'

# 3. Test Multiple Filters with Different Operators
echo -e "${GREEN}=== 3. COMPLEX FILTERS TEST ===${NC}"

test_search "/skuloc" "Multiple filters with >= and IN operators" '{
  "view": "StoreBackroom",
  "filters": {
    "locationNumber": 1001,
    "skuIdIn": [10001, 10002, 10003, 10004, 10005]
  },
  "paginate": false
}'

# 4. Test Ad-hoc Mode with Custom Columns
echo -e "${GREEN}=== 4. AD-HOC MODE TEST ===${NC}"

test_search "/skuloc" "Ad-hoc query with specific columns" '{
  "columns": ["sku_id", "location_number", "available_qty", "ecomm_pick_reserve"],
  "filters": {
    "locationNumberIn": [1001, 1002],
    "availableQtyGte": 50
  },
  "sort": [
    {"field": "available_qty", "direction": "DESC"},
    {"field": "sku_id", "direction": "ASC"}
  ],
  "size": 10
}'

# 5. Test Ad-hoc Mode with Inline Computed Fields
echo -e "${GREEN}=== 5. AD-HOC WITH INLINE COMPUTED FIELDS ===${NC}"

test_search "/skuloc" "Ad-hoc with custom computed field" '{
  "columns": ["sku_id", "location_number", "available_qty", "totalReserved"],
  "computed": {
    "totalReserved": "ecomm_pick_reserve + ecomm_pack_reserve + merch_reserve_qty"
  },
  "filters": {
    "locationNumber": 1001
  },
  "size": 5
}'

# 6. Test Different Sort Orders
echo -e "${GREEN}=== 6. SORTING TEST ===${NC}"

test_search "/skuloc" "Reserve Audit view with custom sort" '{
  "view": "ReserveAudit",
  "filters": {
    "locationNumberIn": [1001, 1002, 1003]
  },
  "sort": [
    {"field": "totalReserve", "direction": "DESC"}
  ],
  "size": 5
}'

# 7. Test Location Master Search
echo -e "${GREEN}=== 7. LOCATION MASTER SEARCH ===${NC}"

test_search "/location" "Store Directory view" '{
  "view": "StoreDirectory",
  "filters": {
    "locationTypeIn": ["STORE"],
    "country": "US"
  },
  "size": 10
}'

# 8. Test RSVEHR (Reserve) Search - CORRECTED
echo -e "${GREEN}=== 8. RESERVE SEARCH TEST ===${NC}"

test_search "/reserve" "Active Reserves view" '{
  "view": "ActiveReserves",
  "filters": {
    "channel": "D",
    "reservationStatus": "A"
  },
  "sort": [
    {"field": "hard_reservation_start_ts", "direction": "DESC"}
  ],
  "size": 10
}'

# 9. Test Time-based Filters - CORRECTED
echo -e "${GREEN}=== 9. TIME-BASED FILTER TEST ===${NC}"

test_search "/skuloc" "Recently updated items" '{
  "columns": ["sku_id", "location_number", "available_qty", "updated_ts"],
  "filters": {
    "updatedAfter": "2024-01-01T00:00:00Z"
  },
  "sort": [
    {"field": "updated_ts", "direction": "DESC"}
  ],
  "size": 5
}'

# 10. Test Unpaginated Results
echo -e "${GREEN}=== 10. UNPAGINATED RESULTS TEST ===${NC}"

test_search "/skuloc" "All items at location (unpaginated)" '{
  "view": "StoreBackroom",
  "filters": {
    "locationNumber": 1001
  },
  "paginate": false
}'

# 11. Test Channel and ATS Flag Filters - REMOVED
# Note: These columns don't exist in the skuloc table based on the CREATE TABLE statement

# 12. Test Error Handling - Invalid View
echo -e "${GREEN}=== 12. ERROR HANDLING - Invalid View ===${NC}"

test_search "/skuloc" "Invalid view name (should fail)" '{
  "view": "NonExistentView",
  "filters": {
    "locationNumber": 1001
  },
  "size": 50
}'

# 13. Test Error Handling - Invalid Filter
echo -e "${GREEN}=== 13. ERROR HANDLING - Invalid Filter ===${NC}"

test_search "/skuloc" "Invalid filter for view (should fail)" '{
  "view": "EcommATS",
  "filters": {
    "invalidFilter": "test"
  },
  "size": 50
}'

# 14. Test Large IN Clause - CORRECTED
echo -e "${GREEN}=== 14. LARGE IN CLAUSE TEST ===${NC}"

# Generate array of 100 SKU IDs
sku_array=$(seq 10001 10100 | jq -R . | jq -s .)

test_search "/skuloc" "Large IN clause with 100 SKUs" "{
  \"view\": \"EcommATS\",
  \"filters\": {
    \"skuIdIn\": $sku_array,
    \"locationNumberIn\": [1001]
  },
  \"size\": 10
}"

# 15. Test Reserve History with Date Range
echo -e "${GREEN}=== 15. DATE RANGE FILTER TEST ===${NC}"

test_search "/reserve" "Reserve history with date range" '{
  "view": "ReserveHistory",
  "filters": {
    "startAfter": "2024-01-01T00:00:00Z",
    "endBefore": "2024-12-31T23:59:59Z"
  },
  "size": 5
}'

# 16. Test Cross-Table Filter (RSVEHR filtered by location type)
echo -e "${GREEN}=== 16. CROSS-TABLE FILTER TEST ===${NC}"

test_search "/reserve" "Reserves filtered by location type" '{
  "view": "ReservesByLocation",
  "filters": {
    "locationTypeIn": ["DC", "STORE"],
    "reservationStatus": "A"
  },
  "size": 10
}'

echo ""
echo "================================="
echo "Feature testing complete!"
echo "================================="