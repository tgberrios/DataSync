#!/bin/bash
cd /home/iks/OneDrive/DataSync

echo "=== Testing Custom Job Execution ==="
echo ""

echo "1. Checking if job exists..."
PGPASSWORD='Yucaquemada1' psql -h localhost -U tomy.berrios -d DataLake -c "SELECT job_name, active, enabled FROM metadata.custom_jobs WHERE job_name = 'test_job_postgres';"

echo ""
echo "2. Executing job via DataSync (will run for 5 seconds)..."
timeout 5 ./DataSync > /tmp/datasync_test.log 2>&1 &
DATASYNC_PID=$!
sleep 2
pkill -f DataSync
wait $DATASYNC_PID 2>/dev/null

echo ""
echo "3. Checking process_log..."
PGPASSWORD='Yucaquemada1' psql -h localhost -U tomy.berrios -d DataLake -c "SELECT process_type, process_name, status, total_rows_processed, LEFT(error_message, 50) as error FROM metadata.process_log WHERE process_type = 'CUSTOM_JOB' ORDER BY created_at DESC LIMIT 3;"

echo ""
echo "4. Checking job_results..."
PGPASSWORD='Yucaquemada1' psql -h localhost -U tomy.berrios -d DataLake -c "SELECT job_name, row_count, full_result_stored FROM metadata.job_results ORDER BY created_at DESC LIMIT 3;"

echo ""
echo "5. Checking if target table was created..."
PGPASSWORD='Yucaquemada1' psql -h localhost -U tomy.berrios -d DataLake -c "SELECT COUNT(*) as table_exists FROM information_schema.tables WHERE table_schema = 'test_jobs' AND table_name = 'config_copy';"

echo ""
echo "6. Checking row count in target table..."
PGPASSWORD='Yucaquemada1' psql -h localhost -U tomy.berrios -d DataLake -c "SELECT COUNT(*) as row_count FROM test_jobs.config_copy;" 2>&1 || echo "Table does not exist yet"

echo ""
echo "=== Test Complete ==="

