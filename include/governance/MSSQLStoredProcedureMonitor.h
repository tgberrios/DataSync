#ifndef MSSQL_STORED_PROCEDURE_MONITOR_H
#define MSSQL_STORED_PROCEDURE_MONITOR_H

#include <string>
#include <vector>

class MSSQLStoredProcedureMonitor {
public:
  MSSQLStoredProcedureMonitor();
  ~MSSQLStoredProcedureMonitor();

  void monitorStoredProcedures(const std::string &connectionString);
  void storeExecutionHistory();
  void detectAlerts();
  void identifyExpensiveSPs();
  void detectTimeouts();

private:
  void queryExecutionStats();
  void compareWithPrevious();
  void generateAlerts();
  void calculatePerformanceMetrics();
};

#endif
