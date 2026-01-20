#include "backup/backup_scheduler.h"
#include "core/logger.h"
#include "core/database_config.h"
#include "backup/backup_manager.h"
#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <algorithm>
#include <cstdlib>
#include <filesystem>

using json = nlohmann::json;

std::atomic<bool> BackupScheduler::running_{false};
std::thread BackupScheduler::scheduler_thread_;

bool BackupScheduler::matchesCronField(const std::string& field, int current_value) {
  if (field == "*") {
    return true;
  }
  
  size_t dash_pos = field.find("-");
  size_t comma_pos = field.find(",");
  size_t slash_pos = field.find("/");
  
  if (dash_pos != std::string::npos) {
    try {
      int start = std::stoi(field.substr(0, dash_pos));
      int end = std::stoi(field.substr(dash_pos + 1));
      return current_value >= start && current_value <= end;
    } catch (...) {
      return false;
    }
  }
  
  if (comma_pos != std::string::npos) {
    std::istringstream iss(field);
    std::string item;
    while (std::getline(iss, item, ',')) {
      try {
        if (std::stoi(item) == current_value) {
          return true;
        }
      } catch (...) {
        continue;
      }
    }
    return false;
  }
  
  if (slash_pos != std::string::npos) {
    try {
      std::string base = field.substr(0, slash_pos);
      int step = std::stoi(field.substr(slash_pos + 1));
      if (base == "*") {
        return current_value % step == 0;
      } else {
        int start = std::stoi(base);
        return (current_value - start) % step == 0 && current_value >= start;
      }
    } catch (...) {
      return false;
    }
  }
  
  try {
    return std::stoi(field) == current_value;
  } catch (...) {
    return false;
  }
}

bool BackupScheduler::shouldRunCron(const std::string& cron_schedule) {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  std::tm* utc_time = std::gmtime(&time_t);
  
  std::istringstream iss(cron_schedule);
  std::string minute, hour, day, month, dow;
  iss >> minute >> hour >> day >> month >> dow;
  
  if (iss.fail()) {
    return false;
  }
  
  int current_minute = utc_time->tm_min;
  int current_hour = utc_time->tm_hour;
  int current_day = utc_time->tm_mday;
  int current_month = utc_time->tm_mon + 1;
  int current_dow = utc_time->tm_wday;
  
  return matchesCronField(minute, current_minute) &&
         matchesCronField(hour, current_hour) &&
         matchesCronField(day, current_day) &&
         matchesCronField(month, current_month) &&
         matchesCronField(dow, current_dow);
}

std::string BackupScheduler::calculateNextRunTime(const std::string& cron_schedule) {
  std::istringstream iss(cron_schedule);
  std::string minute, hour, day, month, dow;
  iss >> minute >> hour >> day >> month >> dow;
  
  if (iss.fail()) {
    return "";
  }
  
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  std::tm next_run = *std::gmtime(&time_t);
  
  next_run.tm_sec = 0;
  next_run.tm_min += 1;
  
  int max_iterations = 10000;
  int iterations = 0;
  
  while (iterations < max_iterations) {
    std::time_t next_time_t = std::mktime(&next_run);
    std::tm* next_utc = std::gmtime(&next_time_t);
    
    int current_minute = next_utc->tm_min;
    int current_hour = next_utc->tm_hour;
    int current_day = next_utc->tm_mday;
    int current_month = next_utc->tm_mon + 1;
    int current_dow = next_utc->tm_wday;
    
    if (matchesCronField(minute, current_minute) &&
        matchesCronField(hour, current_hour) &&
        matchesCronField(day, current_day) &&
        matchesCronField(month, current_month) &&
        matchesCronField(dow, current_dow)) {
      std::ostringstream oss;
      oss << std::put_time(next_utc, "%Y-%m-%d %H:%M:%S");
      return oss.str();
    }
    
    next_run.tm_min += 1;
    if (next_run.tm_min >= 60) {
      next_run.tm_min = 0;
      next_run.tm_hour += 1;
      if (next_run.tm_hour >= 24) {
        next_run.tm_hour = 0;
        next_run.tm_mday += 1;
      }
    }
    
    iterations++;
  }
  
  return "";
}

void BackupScheduler::checkAndExecuteScheduledBackups() {
  try {
    std::string conn_str = "postgresql://" + DatabaseConfig::getPostgresUser() + ":" +
                           DatabaseConfig::getPostgresPassword() + "@" +
                           DatabaseConfig::getPostgresHost() + ":" +
                           DatabaseConfig::getPostgresPort() + "/" +
                           DatabaseConfig::getPostgresDB();
    
    pqxx::connection conn(conn_str);
    pqxx::work txn(conn);
    
    auto result = txn.exec(
      "SELECT backup_id, backup_name, db_engine, connection_string, "
      "database_name, backup_type, file_path, cron_schedule "
      "FROM metadata.backups "
      "WHERE is_scheduled = true AND status != 'in_progress'"
    );
    
    for (const auto& row : result) {
      std::string cron_schedule = row["cron_schedule"].as<std::string>();
      
      if (shouldRunCron(cron_schedule)) {
        int backup_id = row["backup_id"].as<int>();
        std::string backup_name = row["backup_name"].as<std::string>();
        std::string db_engine = row["db_engine"].as<std::string>();
        std::string connection_string = row["connection_string"].as<std::string>();
        std::string database_name = row["database_name"].as<std::string>();
        std::string backup_type_str = row["backup_type"].as<std::string>();
        std::string base_file_path = row["file_path"].as<std::string>();
        
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        std::tm* utc_time = std::gmtime(&time_t);
        std::ostringstream timestamp_oss;
        timestamp_oss << std::put_time(utc_time, "%Y-%m-%dT%H-%M-%S");
        std::string timestamp = timestamp_oss.str();
        
        std::filesystem::path base_path(base_file_path);
        std::string extension = base_path.extension().string();
        std::string file_path = base_path.parent_path().string() + "/" +
                               base_path.stem().string() + "_" + timestamp + extension;
        
        Logger::info(LogCategory::SYSTEM, "BackupScheduler",
                     "Executing scheduled backup: " + backup_name);
        
        pqxx::work history_txn(conn);
        pqxx::params history_params;
        history_params.append(backup_id);
        history_params.append(backup_name);
        history_params.append("in_progress");
        history_params.append("scheduled");
        auto history_result = history_txn.exec(
          pqxx::zview("INSERT INTO metadata.backup_history "
                      "(backup_id, backup_name, status, started_at, triggered_by) "
                      "VALUES ($1, $2, $3, NOW(), $4) RETURNING id"),
          history_params
        );
        int history_id = history_result[0]["id"].as<int>();
        history_txn.commit();
        
        pqxx::params update_params;
        update_params.append(backup_id);
        txn.exec(
          pqxx::zview("UPDATE metadata.backups "
                      "SET last_run_at = NOW(), run_count = run_count + 1 "
                      "WHERE backup_id = $1"),
          update_params
        );
        txn.commit();
        
        BackupConfig config;
        config.backup_name = backup_name;
        config.db_engine = db_engine;
        config.connection_string = connection_string;
        config.database_name = database_name;
        config.backup_type = BackupManager::parseBackupType(backup_type_str);
        config.file_path = file_path;
        
        auto start_time = std::chrono::steady_clock::now();
        BackupResult backup_result = BackupManager::createBackup(config);
        auto end_time = std::chrono::steady_clock::now();
        int duration = std::chrono::duration_cast<std::chrono::seconds>(
          end_time - start_time).count();
        
        pqxx::work update_txn(conn);
        if (backup_result.success) {
          pqxx::params backup_update_params;
          backup_update_params.append(backup_result.file_size);
          backup_update_params.append(backup_id);
          update_txn.exec(
            pqxx::zview("UPDATE metadata.backups "
                        "SET status = 'completed', completed_at = NOW(), "
                        "file_size = $1, error_message = NULL "
                        "WHERE backup_id = $2"),
            backup_update_params
          );
          
          pqxx::params history_update_params;
          history_update_params.append(duration);
          history_update_params.append(backup_result.file_path);
          history_update_params.append(backup_result.file_size);
          history_update_params.append(history_id);
          update_txn.exec(
            pqxx::zview("UPDATE metadata.backup_history "
                        "SET status = 'completed', completed_at = NOW(), "
                        "duration_seconds = $1, file_path = $2, file_size = $3 "
                        "WHERE id = $4"),
            history_update_params
          );
          
          Logger::info(LogCategory::SYSTEM, "BackupScheduler",
                       "Scheduled backup " + backup_name + " completed successfully");
        } else {
          pqxx::params backup_fail_params;
          backup_fail_params.append(backup_result.error_message);
          backup_fail_params.append(backup_id);
          update_txn.exec(
            pqxx::zview("UPDATE metadata.backups "
                        "SET status = 'failed', error_message = $1, completed_at = NOW() "
                        "WHERE backup_id = $2"),
            backup_fail_params
          );
          
          pqxx::params history_fail_params;
          history_fail_params.append(backup_result.error_message);
          history_fail_params.append(history_id);
          update_txn.exec(
            pqxx::zview("UPDATE metadata.backup_history "
                        "SET status = 'failed', completed_at = NOW(), error_message = $1 "
                        "WHERE id = $2"),
            history_fail_params
          );
          
          Logger::error(LogCategory::SYSTEM, "BackupScheduler",
                        "Error executing scheduled backup " + backup_name + ": " +
                        backup_result.error_message);
        }
        update_txn.commit();
        
        std::string next_run = calculateNextRunTime(cron_schedule);
        if (!next_run.empty()) {
          pqxx::work next_txn(conn);
          pqxx::params next_params;
          next_params.append(next_run);
          next_params.append(backup_id);
          next_txn.exec(
            pqxx::zview("UPDATE metadata.backups "
                        "SET next_run_at = $1::timestamp "
                        "WHERE backup_id = $2"),
            next_params
          );
          next_txn.commit();
        }
      }
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "BackupScheduler",
                  "Error checking scheduled backups: " + std::string(e.what()));
  }
}

void BackupScheduler::schedulerLoop() {
  while (running_.load()) {
    checkAndExecuteScheduledBackups();
    
    for (int i = 0; i < 60 && running_.load(); i++) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

void BackupScheduler::start() {
  if (running_.load()) {
    return;
  }
  
  running_.store(true);
  scheduler_thread_ = std::thread(schedulerLoop);
  Logger::info(LogCategory::SYSTEM, "BackupScheduler", "Backup scheduler started");
}

void BackupScheduler::stop() {
  if (!running_.load()) {
    return;
  }
  
  running_.store(false);
  if (scheduler_thread_.joinable()) {
    scheduler_thread_.join();
  }
  Logger::info(LogCategory::SYSTEM, "BackupScheduler", "Backup scheduler stopped");
}

bool BackupScheduler::isRunning() {
  return running_.load();
}
