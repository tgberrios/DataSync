#ifndef LINEAGE_EXTRACTOR_MONGODB_H
#define LINEAGE_EXTRACTOR_MONGODB_H

#include <string>
#include <vector>

class LineageExtractorMongoDB {
public:
  LineageExtractorMongoDB();
  ~LineageExtractorMongoDB();

  void extractLineage(const std::string &connectionString);
  void storeLineage();
  void analyzeDependencies();

private:
  void extractCollectionDependencies();
  void extractViewDependencies();
  void extractPipelineDependencies();
  void buildDependencyGraph();
};

#endif
