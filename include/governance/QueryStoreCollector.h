#ifndef QUERY_STORE_COLLECTOR_H
#define QUERY_STORE_COLLECTOR_H

#include <string>
#include <vector>

class QueryStoreCollector {
public:
  QueryStoreCollector();
  ~QueryStoreCollector();

  void collectQuerySnapshots(const std::string &connectionString);
  void storeSnapshots();
  void analyzeQueries();

private:
  void queryPgStatStatements();
  void parseQueryText();
  void extractQueryMetadata();
};

#endif
