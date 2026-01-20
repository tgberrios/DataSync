#include "sync/BackfillManager.h"
#include "sync/WorkflowExecutor.h"
#include "core/database_config.h"
#include <sstream>
#include <iomanip>
#include <thread>
#include <algorithm>

BackfillManager& BackfillManager::getInstance() {
  static BackfillManager instance;
  return instance;
}

void BackfillManager::executeBackfill(const BackfillConfig& config) {
  Logger::info(LogCategory::MONITORING, "executeBackfill",
               "Starting backfill for workflow: " + config.workflow_name +
               " from " + config.start_date + " to " + config.end_date);
  
  auto periods = generateBackfillPeriods(config);
  
  if (config.parallel && config.max_parallel_jobs > 1) {
    std::vector<std::thread> threads;
    int activeThreads = 0;
    std::mutex threadMutex;
    
    for (size_t i = 0; i < periods.size(); i += 2) {
      while (activeThreads >= config.max_parallel_jobs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      
      std::lock_guard<std::mutex> lock(threadMutex);
      activeThreads++;
      
      std::string periodStart = periods[i];
      std::string periodEnd = (i + 1 < periods.size()) ? periods[i + 1] : config.end_date;
      
      threads.emplace_back([this, config, periodStart, periodEnd, &activeThreads, &threadMutex]() {
        executeBackfillPeriod(config.workflow_name, periodStart, periodEnd);
        std::lock_guard<std::mutex> lock(threadMutex);
        activeThreads--;
      });
    }
    
    for (auto& thread : threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  } else {
    for (size_t i = 0; i < periods.size(); i += 2) {
      std::string periodStart = periods[i];
      std::string periodEnd = (i + 1 < periods.size()) ? periods[i + 1] : config.end_date;
      executeBackfillPeriod(config.workflow_name, periodStart, periodEnd);
    }
  }
  
  Logger::info(LogCategory::MONITORING, "executeBackfill",
               "Backfill completed for workflow: " + config.workflow_name);
}

std::vector<BackfillExecution> BackfillManager::getBackfillExecutions(const std::string& workflowName) const {
  std::vector<BackfillExecution> executions;
  return executions;
}

void BackfillManager::cancelBackfill(const std::string& workflowName) {
  Logger::info(LogCategory::MONITORING, "cancelBackfill",
               "Cancelling backfill for workflow: " + workflowName);
}

std::vector<std::string> BackfillManager::generateBackfillPeriods(const BackfillConfig& config) {
  std::vector<std::string> periods;
  
  std::tm startTm = {};
  std::tm endTm = {};
  std::istringstream startStream(config.start_date);
  std::istringstream endStream(config.end_date);
  
  startStream >> std::get_time(&startTm, "%Y-%m-%d");
  endStream >> std::get_time(&endTm, "%Y-%m-%d");
  
  if (startStream.fail() || endStream.fail()) {
    Logger::error(LogCategory::MONITORING, "generateBackfillPeriods",
                  "Invalid date format");
    return periods;
  }
  
  auto startTime = std::mktime(&startTm);
  auto endTime = std::mktime(&endTm);
  
  auto currentTime = startTime;
  
  while (currentTime <= endTime) {
    std::ostringstream oss;
    oss << std::put_time(std::localtime(&currentTime), "%Y-%m-%d");
    periods.push_back(oss.str());
    
    if (config.interval == "daily") {
      currentTime += 24 * 60 * 60;
    } else if (config.interval == "weekly") {
      currentTime += 7 * 24 * 60 * 60;
    } else if (config.interval == "monthly") {
      std::tm* tm = std::localtime(&currentTime);
      tm->tm_mon++;
      if (tm->tm_mon > 11) {
        tm->tm_mon = 0;
        tm->tm_year++;
      }
      currentTime = std::mktime(tm);
    } else {
      currentTime += 24 * 60 * 60;
    }
  }
  
  return periods;
}

void BackfillManager::executeBackfillPeriod(const std::string& workflowName,
                                           const std::string& periodStart,
                                           const std::string& periodEnd) {
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    WorkflowExecutor executor(connStr);
    
    Logger::info(LogCategory::MONITORING, "executeBackfillPeriod",
                 "Executing backfill period for " + workflowName +
                 " from " + periodStart + " to " + periodEnd);
    
    executor.executeWorkflowAsync(workflowName, TriggerType::MANUAL);
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "executeBackfillPeriod",
                  "Error executing backfill period: " + std::string(e.what()));
  }
}
