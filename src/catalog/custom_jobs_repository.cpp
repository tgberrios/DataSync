#include "catalog/custom_jobs_repository.h"
#include "core/logger.h"
#include <algorithm>
#include <stdexcept>

CustomJobsRepository::CustomJobsRepository(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

pqxx::connection CustomJobsRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

void CustomJobsRepository::createCustomJobsTable() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.custom_jobs ("
        "id SERIAL PRIMARY KEY,"
        "job_name VARCHAR(255) UNIQUE NOT NULL,"
        "description TEXT,"
        "source_db_engine VARCHAR(50) NOT NULL,"
        "source_connection_string TEXT NOT NULL,"
        "query_sql TEXT NOT NULL,"
        "target_db_engine VARCHAR(50) NOT NULL,"
        "target_connection_string TEXT NOT NULL,"
        "target_schema VARCHAR(100) NOT NULL,"
        "target_table VARCHAR(100) NOT NULL,"
        "schedule_cron VARCHAR(100),"
        "active BOOLEAN NOT NULL DEFAULT true,"
        "enabled BOOLEAN NOT NULL DEFAULT true,"
        "transform_config JSONB DEFAULT '{}'::jsonb,"
        "metadata JSONB DEFAULT '{}'::jsonb,"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "updated_at TIMESTAMP DEFAULT NOW()"
        ");";

    txn.exec(createTableSQL);

    std::string createIndexesSQL =
        "CREATE INDEX IF NOT EXISTS idx_custom_jobs_job_name ON "
        "metadata.custom_jobs (job_name);"
        "CREATE INDEX IF NOT EXISTS idx_custom_jobs_active ON "
        "metadata.custom_jobs (active);"
        "CREATE INDEX IF NOT EXISTS idx_custom_jobs_enabled ON "
        "metadata.custom_jobs (enabled);"
        "CREATE INDEX IF NOT EXISTS idx_custom_jobs_schedule ON "
        "metadata.custom_jobs (schedule_cron) WHERE schedule_cron IS NOT NULL;";

    txn.exec(createIndexesSQL);
    txn.commit();

    Logger::info(LogCategory::DATABASE, "CustomJobsRepository",
                 "Custom jobs table created successfully");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "createCustomJobsTable",
                  "Error creating custom jobs table: " + std::string(e.what()));
  }
}

void CustomJobsRepository::createJobResultsTable() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.job_results ("
        "id SERIAL PRIMARY KEY,"
        "job_name VARCHAR(255) NOT NULL,"
        "process_log_id BIGINT,"
        "row_count BIGINT NOT NULL DEFAULT 0,"
        "result_sample JSONB,"
        "full_result_stored BOOLEAN NOT NULL DEFAULT true,"
        "created_at TIMESTAMP DEFAULT NOW()"
        ");";

    txn.exec(createTableSQL);

    std::string createIndexesSQL =
        "CREATE INDEX IF NOT EXISTS idx_job_results_job_name ON "
        "metadata.job_results (job_name);"
        "CREATE INDEX IF NOT EXISTS idx_job_results_process_log_id ON "
        "metadata.job_results (process_log_id);"
        "CREATE INDEX IF NOT EXISTS idx_job_results_created_at ON "
        "metadata.job_results (created_at);";

    txn.exec(createIndexesSQL);
    txn.commit();

    Logger::info(LogCategory::DATABASE, "CustomJobsRepository",
                 "Job results table created successfully");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "createJobResultsTable",
                  "Error creating job results table: " + std::string(e.what()));
  }
}

std::vector<CustomJob> CustomJobsRepository::getActiveJobs() {
  std::vector<CustomJob> jobs;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT id, job_name, description, source_db_engine, "
        "source_connection_string, query_sql, target_db_engine, "
        "target_connection_string, target_schema, target_table, "
        "schedule_cron, active, enabled, transform_config, metadata, "
        "created_at, updated_at "
        "FROM metadata.custom_jobs WHERE active = true AND enabled = true");

    for (const auto &row : results) {
      jobs.push_back(rowToJob(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getActiveJobs",
                  "Error getting active jobs: " + std::string(e.what()));
  }
  return jobs;
}

std::vector<CustomJob> CustomJobsRepository::getScheduledJobs() {
  std::vector<CustomJob> jobs;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results =
        txn.exec("SELECT id, job_name, description, source_db_engine, "
                 "source_connection_string, query_sql, target_db_engine, "
                 "target_connection_string, target_schema, target_table, "
                 "schedule_cron, active, enabled, transform_config, metadata, "
                 "created_at, updated_at "
                 "FROM metadata.custom_jobs WHERE schedule_cron IS NOT NULL "
                 "AND active = true AND enabled = true");

    for (const auto &row : results) {
      jobs.push_back(rowToJob(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getScheduledJobs",
                  "Error getting scheduled jobs: " + std::string(e.what()));
  }
  return jobs;
}

CustomJob CustomJobsRepository::getJob(const std::string &jobName) {
  CustomJob job;
  job.job_name = "";
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT id, job_name, description, source_db_engine, "
        "source_connection_string, query_sql, target_db_engine, "
        "target_connection_string, target_schema, target_table, "
        "schedule_cron, active, enabled, transform_config, metadata, "
        "created_at, updated_at "
        "FROM metadata.custom_jobs WHERE job_name = $1",
        jobName);

    if (!results.empty()) {
      job = rowToJob(results[0]);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getJob",
                  "Error getting job: " + std::string(e.what()));
  }
  return job;
}

void CustomJobsRepository::insertOrUpdateJob(const CustomJob &job) {
  if (job.job_name.empty() || job.source_db_engine.empty() ||
      job.source_connection_string.empty() || job.query_sql.empty() ||
      job.target_db_engine.empty() || job.target_connection_string.empty() ||
      job.target_schema.empty() || job.target_table.empty()) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateJob",
                  "Invalid input: required fields must not be empty");
    throw std::invalid_argument("Required job fields must not be empty");
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string transformConfigStr = job.transform_config.dump();
    std::string metadataStr = job.metadata.dump();
    std::string scheduleCron =
        job.schedule_cron.empty() ? "" : job.schedule_cron;

    auto existing = txn.exec_params(
        "SELECT id FROM metadata.custom_jobs WHERE job_name = $1",
        job.job_name);

    if (existing.empty()) {
      if (scheduleCron.empty()) {
        txn.exec_params(
            "INSERT INTO metadata.custom_jobs (job_name, description, "
            "source_db_engine, source_connection_string, query_sql, "
            "target_db_engine, target_connection_string, target_schema, "
            "target_table, schedule_cron, active, enabled, transform_config, "
            "metadata) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10, $11, "
            "$12::jsonb, $13::jsonb)",
            job.job_name, job.description, job.source_db_engine,
            job.source_connection_string, job.query_sql, job.target_db_engine,
            job.target_connection_string, job.target_schema, job.target_table,
            job.active, job.enabled, transformConfigStr, metadataStr);
      } else {
        txn.exec_params(
            "INSERT INTO metadata.custom_jobs (job_name, description, "
            "source_db_engine, source_connection_string, query_sql, "
            "target_db_engine, target_connection_string, target_schema, "
            "target_table, schedule_cron, active, enabled, transform_config, "
            "metadata) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, "
            "$13::jsonb, $14::jsonb)",
            job.job_name, job.description, job.source_db_engine,
            job.source_connection_string, job.query_sql, job.target_db_engine,
            job.target_connection_string, job.target_schema, job.target_table,
            scheduleCron, job.active, job.enabled, transformConfigStr,
            metadataStr);
      }
    } else {
      if (scheduleCron.empty()) {
        txn.exec_params(
            "UPDATE metadata.custom_jobs SET description = $1, "
            "source_db_engine = $2, source_connection_string = $3, "
            "query_sql = $4, target_db_engine = $5, "
            "target_connection_string = $6, target_schema = $7, "
            "target_table = $8, schedule_cron = NULL, active = $9, "
            "enabled = $10, transform_config = $11::jsonb, "
            "metadata = $12::jsonb, updated_at = NOW() WHERE job_name = $13",
            job.description, job.source_db_engine, job.source_connection_string,
            job.query_sql, job.target_db_engine, job.target_connection_string,
            job.target_schema, job.target_table, job.active, job.enabled,
            transformConfigStr, metadataStr, job.job_name);
      } else {
        txn.exec_params(
            "UPDATE metadata.custom_jobs SET description = $1, "
            "source_db_engine = $2, source_connection_string = $3, "
            "query_sql = $4, target_db_engine = $5, "
            "target_connection_string = $6, target_schema = $7, "
            "target_table = $8, schedule_cron = $9, active = $10, "
            "enabled = $11, transform_config = $12::jsonb, "
            "metadata = $13::jsonb, updated_at = NOW() WHERE job_name = $14",
            job.description, job.source_db_engine, job.source_connection_string,
            job.query_sql, job.target_db_engine, job.target_connection_string,
            job.target_schema, job.target_table, scheduleCron, job.active,
            job.enabled, transformConfigStr, metadataStr, job.job_name);
      }
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateJob",
                  "Error inserting/updating job: " + std::string(e.what()));
    throw;
  }
}

void CustomJobsRepository::deleteJob(const std::string &jobName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params("DELETE FROM metadata.custom_jobs WHERE job_name = $1",
                    jobName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "deleteJob",
                  "Error deleting job: " + std::string(e.what()));
    throw;
  }
}

void CustomJobsRepository::updateJobActive(const std::string &jobName,
                                           bool active) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params(
        "UPDATE metadata.custom_jobs SET active = $1, updated_at = NOW() "
        "WHERE job_name = $2",
        active, jobName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateJobActive",
                  "Error updating job active status: " + std::string(e.what()));
    throw;
  }
}

CustomJob CustomJobsRepository::rowToJob(const pqxx::row &row) {
  CustomJob job;
  job.id = row[0].as<int>();
  job.job_name = row[1].as<std::string>();
  job.description = row[2].is_null() ? "" : row[2].as<std::string>();
  job.source_db_engine = row[3].as<std::string>();
  job.source_connection_string = row[4].as<std::string>();
  job.query_sql = row[5].as<std::string>();
  job.target_db_engine = row[6].as<std::string>();
  job.target_connection_string = row[7].as<std::string>();
  job.target_schema = row[8].as<std::string>();
  job.target_table = row[9].as<std::string>();
  job.schedule_cron = row[10].is_null() ? "" : row[10].as<std::string>();
  job.active = row[11].as<bool>();
  job.enabled = row[12].as<bool>();
  if (!row[13].is_null()) {
    try {
      job.transform_config = json::parse(row[13].as<std::string>());
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "CustomJobsRepository",
                    "Error parsing transform_config JSON: " +
                        std::string(e.what()));
      job.transform_config = json{};
    }
  } else {
    job.transform_config = json{};
  }
  if (!row[14].is_null()) {
    try {
      job.metadata = json::parse(row[14].as<std::string>());
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "CustomJobsRepository",
                    "Error parsing metadata JSON: " + std::string(e.what()));
      job.metadata = json{};
    }
  } else {
    job.metadata = json{};
  }
  job.created_at = row[15].is_null() ? "" : row[15].as<std::string>();
  job.updated_at = row[16].is_null() ? "" : row[16].as<std::string>();
  return job;
}
