#include "sync/DataDrivenScheduler.h"
#include "sync/WorkflowExecutor.h"
#include "core/database_config.h"
#include <pqxx/pqxx>
#include <algorithm>

DataDrivenScheduler& DataDrivenScheduler::getInstance() {
  static DataDrivenScheduler instance;
  return instance;
}

DataDrivenScheduler::~DataDrivenScheduler() {
  stop();
}

void DataDrivenScheduler::start() {
  if (running_.exchange(true)) {
    return;
  }
  
  schedulerThread_ = std::thread(&DataDrivenScheduler::schedulerLoop, this);
  Logger::info(LogCategory::MONITORING, "DataDrivenScheduler",
               "Data-driven scheduler started");
}

void DataDrivenScheduler::stop() {
  if (!running_.exchange(false)) {
    return;
  }
  
  if (schedulerThread_.joinable()) {
    schedulerThread_.join();
  }
  
  Logger::info(LogCategory::MONITORING, "DataDrivenScheduler",
               "Data-driven scheduler stopped");
}

bool DataDrivenScheduler::isRunning() const {
  return running_.load();
}

void DataDrivenScheduler::registerSchedule(const DataDrivenSchedule& schedule) {
  std::lock_guard<std::mutex> lock(schedulesMutex_);
  auto it = std::find_if(schedules_.begin(), schedules_.end(),
                        [&schedule](const DataDrivenSchedule& s) {
                          return s.workflow_name == schedule.workflow_name;
                        });
  if (it != schedules_.end()) {
    *it = schedule;
  } else {
    schedules_.push_back(schedule);
  }
  Logger::info(LogCategory::MONITORING, "registerSchedule",
               "Registered data-driven schedule for workflow: " + schedule.workflow_name);
}

void DataDrivenScheduler::unregisterSchedule(const std::string& workflowName) {
  std::lock_guard<std::mutex> lock(schedulesMutex_);
  schedules_.erase(
    std::remove_if(schedules_.begin(), schedules_.end(),
                  [&workflowName](const DataDrivenSchedule& s) {
                    return s.workflow_name == workflowName;
                  }),
    schedules_.end()
  );
}

std::vector<DataDrivenScheduler::DataDrivenSchedule> DataDrivenScheduler::getSchedules() const {
  std::lock_guard<std::mutex> lock(schedulesMutex_);
  return schedules_;
}

void DataDrivenScheduler::schedulerLoop() {
  while (running_.load()) {
    try {
      std::vector<DataDrivenSchedule> activeSchedules;
      {
        std::lock_guard<std::mutex> lock(schedulesMutex_);
        for (const auto& schedule : schedules_) {
          if (schedule.active) {
            activeSchedules.push_back(schedule);
          }
        }
      }
      
      for (const auto& schedule : activeSchedules) {
        if (checkSchedule(schedule)) {
          executeWorkflowIfConditionMet(schedule);
        }
      }
    } catch (const std::exception& e) {
      Logger::error(LogCategory::MONITORING, "schedulerLoop",
                    "Error in data-driven scheduler loop: " + std::string(e.what()));
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(30));
  }
}

bool DataDrivenScheduler::checkSchedule(const DataDrivenSchedule& schedule) {
  try {
    pqxx::connection conn(schedule.connection_string);
    pqxx::work txn(conn);
    
    auto result = txn.exec(schedule.query);
    
    for (const auto& row : result) {
      if (!schedule.condition_field.empty() && !schedule.condition_value.empty()) {
        try {
          std::string fieldValue = row[schedule.condition_field].as<std::string>();
          if (fieldValue == schedule.condition_value) {
            return true;
          }
        } catch (...) {
        }
      } else {
        if (result.size() > 0) {
          return true;
        }
      }
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "checkSchedule",
                  "Error checking schedule for workflow " + schedule.workflow_name + ": " + std::string(e.what()));
  }
  
  return false;
}

void DataDrivenScheduler::executeWorkflowIfConditionMet(const DataDrivenSchedule& schedule) {
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    WorkflowExecutor executor(connStr);
    executor.executeWorkflowAsync(schedule.workflow_name, TriggerType::SCHEDULED);
    Logger::info(LogCategory::MONITORING, "executeWorkflowIfConditionMet",
                 "Data-driven condition met, executing workflow: " + schedule.workflow_name);
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "executeWorkflowIfConditionMet",
                  "Error executing workflow " + schedule.workflow_name + ": " + std::string(e.what()));
  }
}
