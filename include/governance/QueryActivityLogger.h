#ifndef QUERY_ACTIVITY_LOGGER_H
#define QUERY_ACTIVITY_LOGGER_H

#include <string>
#include <vector>

class QueryActivityLogger {
public:
  QueryActivityLogger();
  ~QueryActivityLogger();

  void logActiveQueries(const std::string &connectionString);
  void storeActivityLog();
  void analyzeActivity();

private:
  void queryPgStatActivity();
  void extractQueryInfo();
  void categorizeQueries();
};

#endif
