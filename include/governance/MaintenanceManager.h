#ifndef MAINTENANCE_MANAGER_H
#define MAINTENANCE_MANAGER_H

#include <string>
#include <vector>

class MaintenanceManager {
public:
  MaintenanceManager();
  ~MaintenanceManager();

  void detectMaintenanceNeeds(const std::string &connectionString);
  void executeMaintenance();
  void storeMetrics();
  void generateReport();

private:
  void detectVacuumNeeds();
  void detectReindexNeeds();
  void detectAnalyzeNeeds();
  void calculatePriority();
};

#endif
