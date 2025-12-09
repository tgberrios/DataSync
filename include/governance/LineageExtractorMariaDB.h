#ifndef LINEAGE_EXTRACTOR_MARIADB_H
#define LINEAGE_EXTRACTOR_MARIADB_H

#include <string>
#include <vector>

class LineageExtractorMariaDB {
public:
  LineageExtractorMariaDB();
  ~LineageExtractorMariaDB();

  void extractLineage(const std::string &connectionString);
  void storeLineage();
  void analyzeDependencies();

private:
  void extractTableDependencies();
  void extractViewDependencies();
  void extractTriggerDependencies();
  void buildDependencyGraph();
};

#endif
