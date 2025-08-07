#!/bin/bash

# Test API calls for UAB Scholars
# Using a known faculty ID (Elizabeth Worthey: 3694)
# Run from tests/ directory

BASE_URL="https://scholars.uab.edu/api"
TEST_ID="3694"

echo "🧪 Testing UAB Scholars API calls..."
echo "=================================="

echo ""
echo "1. Testing User Details API:"
echo "curl -X GET \"$BASE_URL/users/$TEST_ID\""
curl -X GET "$BASE_URL/users/$TEST_ID" | jq '.email' 2>/dev/null || echo "Failed to get email"

echo ""
echo "2. Testing Publications API:"
echo "curl -X POST \"$BASE_URL/publications/linkedTo\" -H \"Content-Type: application/json\" -d '{\"objectId\": \"$TEST_ID\", \"category\": \"user\", \"pagination\": {\"perPage\": 5, \"startFrom\": 0}, \"sort\": \"dateDesc\"}'"
curl -X POST "$BASE_URL/publications/linkedTo" \
  -H "Content-Type: application/json" \
  -d "{\"objectId\": \"$TEST_ID\", \"category\": \"user\", \"pagination\": {\"perPage\": 5, \"startFrom\": 0}, \"sort\": \"dateDesc\"}" \
  | jq '.resource | length' 2>/dev/null || echo "Failed to get publications"

echo ""
echo "3. Testing Grants API:"
echo "curl -X POST \"$BASE_URL/grants/linkedTo\" -H \"Content-Type: application/json\" -d '{\"objectId\": \"$TEST_ID\", \"category\": \"user\", \"pagination\": {\"perPage\": 10, \"startFrom\": 0}, \"sort\": \"dateDesc\"}'"
curl -X POST "$BASE_URL/grants/linkedTo" \
  -H "Content-Type: application/json" \
  -d "{\"objectId\": \"$TEST_ID\", \"category\": \"user\", \"pagination\": {\"perPage\": 10, \"startFrom\": 0}, \"sort\": \"dateDesc\"}" \
  | jq '.resource | length' 2>/dev/null || echo "Failed to get grants"

echo ""
echo "✅ API test complete!" 