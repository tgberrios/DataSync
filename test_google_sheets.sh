#!/bin/bash

SPREADSHEET_ID="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
API_KEY="AIzaSyCd4AFiqUtWL2VHPHCmdn7PEStLcz85F2U"
RANGE="Class Data"

echo "Testing Google Sheets API..."
echo "Spreadsheet ID: $SPREADSHEET_ID"
echo "Range: $RANGE"
echo ""

URL="https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${RANGE}?key=${API_KEY}"

echo "Full URL: $URL"
echo ""
echo "Making request..."
curl -v "$URL" 2>&1 | head -100

