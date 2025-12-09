#ifndef DATA_GOVERNANCE_MSSQL_H
#define DATA_GOVERNANCE_MSSQL_H

#include <string>
#include <vector>

class DataGovernanceMSSQL {
public:
  DataGovernanceMSSQL();
  ~DataGovernanceMSSQL();

  void collectGovernanceData(const std::string &connectionString);
  void storeGovernanceData();
  void generateReport();

private:
  void queryIndexPhysicalStats();
  void queryIndexUsageStats();
  void queryMissingIndexes();
  void queryBackupInfo();
  void queryDatabaseConfig();
  void queryStoredProcedures();
  void calculateHealthScores();
};

#endif
