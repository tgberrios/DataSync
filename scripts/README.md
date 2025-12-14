# Python Scripts for Custom Jobs

This folder contains Python scripts that can be used with DataSync Custom Jobs.

## How to Use

1. **Create a Custom Job** in the DataSync UI
2. Set `source_db_engine` to **"Python"**
3. Copy the entire content of a Python script from this folder into the `query_sql` field
4. Set your `target_db_engine` and `target_table` where you want the data inserted
5. Save and execute the job

## Script Examples

### `example_data_generator.py`

Generates sample data with:

- id, name, value, status, created_at, score

### `api_data_fetcher.py`

Fetches data from an external API (JSONPlaceholder) and transforms it.

### `calculate_metrics.py`

Generates daily metrics data for analytics.

## Script Requirements

- Scripts must print **JSON to stdout**
- Output can be:
  - **Array of objects**: `[{"col1": "val1"}, {"col1": "val2"}]`
  - **Single object**: `{"col1": "val1"}` (will be wrapped in array)
- Use `json.dumps()` to output JSON
- Scripts run with Python 3
- Standard library available (json, urllib, datetime, etc.)

## Example Output Format

```json
[
  {
    "id": 1,
    "name": "Item 1",
    "value": 500,
    "status": "active"
  },
  {
    "id": 2,
    "name": "Item 2",
    "value": 300,
    "status": "inactive"
  }
]
```

## Notes

- Scripts are executed in a temporary directory
- All output to stdout will be captured as JSON
- Errors should be handled within the script
- The script will be wrapped with error handling automatically
