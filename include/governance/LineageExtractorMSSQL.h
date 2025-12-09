#ifndef LINEAGE_EXTRACTOR_MSSQL_H
#define LINEAGE_EXTRACTOR_MSSQL_H

#include <string>
#include <vector>

class LineageExtractorMSSQL {
public:
  LineageExtractorMSSQL();
  ~LineageExtractorMSSQL();

  void extractLineage(const std::string &connectionString);
  void storeLineage();
  void analyzeDependencies();

private:
  void extractTableDependencies();
  void extractStoredProcedureDependencies();
  void extractViewDependencies();
  void buildDependencyGraph();
};

#endif
