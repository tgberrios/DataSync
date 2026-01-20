#ifndef BACKFILL_MANAGER_H
#define BACKFILL_MANAGER_H

#include "catalog/workflow_repository.h"
#include "core/logger.h"
#include <string>
#include <chrono>
#include <vector>

class BackfillManager {
public:
  struct BackfillConfig {
    std::string workflow_name;
    std::string start_date;
    std::string end_date;
    std::string date_field;
    std::string interval;
    bool parallel;
    int max_parallel_jobs;
  };
  
  struct BackfillExecution {
    std::string execution_id;
    std::string period_start;
    std::string period_end;
    ExecutionStatus status;
    std::string error_message;
  };
  
  static BackfillManager& getInstance();
  
  void executeBackfill(const BackfillConfig& config);
  std::vector<BackfillExecution> getBackfillExecutions(const std::string& workflowName) const;
  void cancelBackfill(const std::string& workflowName);
  
private:
  BackfillManager() = default;
  ~BackfillManager() = default;
  BackfillManager(const BackfillManager&) = delete;
  BackfillManager& operator=(const BackfillManager&) = delete;
  
  std::vector<std::string> generateBackfillPeriods(const BackfillConfig& config);
  void executeBackfillPeriod(const std::string& workflowName, 
                            const std::string& periodStart, 
                            const std::string& periodEnd);
};

#endif
