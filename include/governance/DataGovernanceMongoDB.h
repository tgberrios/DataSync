#ifndef DATA_GOVERNANCE_MONGODB_H
#define DATA_GOVERNANCE_MONGODB_H

#include <string>
#include <vector>

class DataGovernanceMongoDB {
public:
  DataGovernanceMongoDB();
  ~DataGovernanceMongoDB();

  void collectGovernanceData(const std::string &connectionString);
  void storeGovernanceData();
  void generateReport();

private:
  void queryCollectionStats();
  void queryIndexStats();
  void queryReplicaSetInfo();
  void calculateHealthScores();
};

#endif
