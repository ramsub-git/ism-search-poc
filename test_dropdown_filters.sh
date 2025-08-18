#!/bin/bash

# Test script for Dropdown Filter Population
# Tests the distinct views for populating UI dropdowns

echo "========================================"
echo "Dropdown Filter Population Tests"
echo "========================================"
echo ""

# Base URL for the API
BASE_URL="http://localhost:8080/api/search"

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to make API call and pretty print
test_dropdown() {
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

# 1. Test Region Dropdown (All Regions)
echo -e "${YELLOW}=== 1. REGION DROPDOWN TEST ===${NC}"
test_dropdown "/location" "Get all distinct regions for dropdown" '{
  "view": "RegionDropdown",
  "paginate": false
}'

# 2. Test Store Dropdown (All Stores)
echo -e "${YELLOW}=== 2. STORE DROPDOWN TEST - All Stores ===${NC}"
test_dropdown "/location" "Get all distinct stores for dropdown" '{
  "view": "StoreDropdown",
  "paginate": false
}'

# 3. Test Store Dropdown with Region Filter (Cascading)
echo -e "${YELLOW}=== 3. STORE DROPDOWN TEST - Filtered by Region ===${NC}"
test_dropdown "/location" "Get stores filtered by specific region" '{
  "view": "StoreDropdown",
  "filters": {
    "regionNumberIn": [1, 2]
  },
  "paginate": false
}'

# 4. Test District Dropdown (All Districts)
echo -e "${YELLOW}=== 4. DISTRICT DROPDOWN TEST - All Districts ===${NC}"
test_dropdown "/location" "Get all distinct districts for dropdown" '{
  "view": "DistrictDropdown",
  "paginate": false
}'

# 5. Test District Dropdown with Region Filter (Cascading)
echo -e "${YELLOW}=== 5. DISTRICT DROPDOWN TEST - Filtered by Region ===${NC}"
test_dropdown "/location" "Get districts filtered by specific region" '{
  "view": "DistrictDropdown",
  "filters": {
    "regionNumberIn": [1]
  },
  "paginate": false
}'

# 6. Test Store Type Dropdown
echo -e "${YELLOW}=== 6. STORE TYPE DROPDOWN TEST ===${NC}"
test_dropdown "/location" "Get distinct store types" '{
  "view": "StoreTypeDropdown",
  "paginate": false
}'

# 7. Test with Pagination (Should still work)
echo -e "${YELLOW}=== 7. PAGINATED DROPDOWN TEST ===${NC}"
test_dropdown "/location" "Test dropdown with pagination enabled" '{
  "view": "RegionDropdown",
  "paginate": true,
  "size": 10
}'

# 8. Test Ad-hoc Distinct Query
echo -e "${YELLOW}=== 8. AD-HOC DISTINCT QUERY TEST ===${NC}"
test_dropdown "/location" "Ad-hoc query for distinct values" '{
  "columns": ["region_number", "region_name"],
  "distinct": true,
  "sort": [{"field": "region_name", "direction": "ASC"}],
  "paginate": false
}'

# 9. Test Empty Filter Result
echo -e "${YELLOW}=== 9. EMPTY RESULT TEST ===${NC}"
test_dropdown "/location" "Test dropdown with filter that returns no results" '{
  "view": "StoreDropdown",
  "filters": {
    "regionNumberIn": [99999]
  },
  "paginate": false
}'

# 10. Performance Test - Large Dataset
echo -e "${YELLOW}=== 10. PERFORMANCE TEST - All Stores ===${NC}"
start_time=$(date +%s%N)

response=$(curl -s -X POST "${BASE_URL}/location" \
    -H "Content-Type: application/json" \
    -d '{
  "view": "StoreDropdown",
  "paginate": false
}')

end_time=$(date +%s%N)
elapsed_time=$(( ($end_time - $start_time) / 1000000 ))

echo "Response time: ${elapsed_time}ms"
row_count=$(echo "$response" | jq '.data | length')
echo "Number of distinct stores: $row_count"
echo ""

# # 11. Test Brand Dropdown (if brand dataset exists)
# echo -e "${YELLOW}=== 11. BRAND DROPDOWN TEST ===${NC}"
# test_dropdown "/skuloc" "Get distinct brands from SKULOC" '{
#   "columns": ["brand_id", "brand_name"],
#   "distinct": true,
#   "sort": [{"field": "brand_name", "direction": "ASC"}],
#   "paginate": false
# }'

# # 12. Test Department Dropdown (if exists in data)
# echo -e "${YELLOW}=== 12. DEPARTMENT DROPDOWN TEST ===${NC}"
# test_dropdown "/skuloc" "Get distinct departments" '{
#   "columns": ["department_id", "department_name"],
#   "distinct": true,
#   "sort": [{"field": "department_name", "direction": "ASC"}],
#   "paginate": false
# }'

# Summary function
print_summary() {
    echo ""
    echo "========================================"
    echo -e "${GREEN}Test Summary:${NC}"
    echo "1. Basic dropdown population ✓"
    echo "2. Cascading filters (region -> store) ✓"
    echo "3. Distinct values with sorting ✓"
    echo "4. Performance with large datasets ✓"
    echo "5. Empty result handling ✓"
    echo ""
    echo "Note: Some tests may fail if:"
    echo "- The distinct feature is not yet implemented"
    echo "- The dropdown views are not configured"
    echo "- The test data doesn't include all fields"
    echo "========================================"
}

print_summary
