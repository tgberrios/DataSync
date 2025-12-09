#ifndef DATA_GOVERNANCE_MARIADB_H
#define DATA_GOVERNANCE_MARIADB_H

#include <string>
#include <vector>

class DataGovernanceMariaDB {
public:
  DataGovernanceMariaDB();
  ~DataGovernanceMariaDB();

  void collectGovernanceData(const std::string &connectionString);
  void storeGovernanceData();
  void generateReport();

private:
  void queryTableStats();
  void queryIndexStats();
  void queryUserInfo();
  void calculateHealthScores();
};

#endif
