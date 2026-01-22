#ifndef DATA_DRIVEN_SCHEDULER_H
#define DATA_DRIVEN_SCHEDULER_H

#include "catalog/workflow_repository.h"
#include "core/logger.h"
#include "engines/postgres_engine.h"
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>

class DataDrivenScheduler {
public:
  struct DataDrivenSchedule {
    std::string workflow_name;
    std::string query;
    std::string connection_string;
    std::string condition_field;
    std::string condition_value;
    int check_interval_seconds;
    bool active;
  };
  
  static DataDrivenScheduler& getInstance();
  
  void start();
  void stop();
  bool isRunning() const;
  
  void registerSchedule(const DataDrivenSchedule& schedule);
  void unregisterSchedule(const std::string& workflowName);
  std::vector<DataDrivenSchedule> getSchedules() const;
  
private:
  DataDrivenScheduler() = default;
  ~DataDrivenScheduler();
  DataDrivenScheduler(const DataDrivenScheduler&) = delete;
  DataDrivenScheduler& operator=(const DataDrivenScheduler&) = delete;
  
  std::atomic<bool> running_;
  std::thread schedulerThread_;
  std::vector<DataDrivenSchedule> schedules_;
  mutable std::mutex schedulesMutex_;
  
  void schedulerLoop();
  bool checkSchedule(const DataDrivenSchedule& schedule);
  void executeWorkflowIfConditionMet(const DataDrivenSchedule& schedule);
};

#endif
