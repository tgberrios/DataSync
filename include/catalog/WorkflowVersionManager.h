#ifndef WORKFLOW_VERSION_MANAGER_H
#define WORKFLOW_VERSION_MANAGER_H

#include "catalog/workflow_repository.h"
#include <string>
#include <vector>
#include <map>
#include <mutex>

class WorkflowVersionManager {
public:
  struct WorkflowVersion {
    int version;
    std::string workflow_name;
    std::string description;
    std::string created_at;
    std::string created_by;
    bool is_current;
    json workflow_definition;
  };
  
  static WorkflowVersionManager& getInstance();
  
  int createVersion(const std::string& workflowName, const std::string& createdBy, const std::string& description = "");
  std::vector<WorkflowVersion> getVersions(const std::string& workflowName) const;
  WorkflowVersion getVersion(const std::string& workflowName, int version) const;
  bool restoreVersion(const std::string& workflowName, int version);
  bool deleteVersion(const std::string& workflowName, int version);
  int getCurrentVersion(const std::string& workflowName) const;
  
private:
  WorkflowVersionManager() = default;
  ~WorkflowVersionManager() = default;
  WorkflowVersionManager(const WorkflowVersionManager&) = delete;
  WorkflowVersionManager& operator=(const WorkflowVersionManager&) = delete;
  
  std::map<std::string, int> currentVersions_;
  mutable std::mutex versionsMutex_;
};

#endif
