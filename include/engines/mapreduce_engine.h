#ifndef MAPREDUCE_ENGINE_H
#define MAPREDUCE_ENGINE_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>

using json = nlohmann::json;

#ifdef HAVE_HADOOP

// MapReduceEngine: Integración con Hadoop MapReduce (legacy)
class MapReduceEngine {
public:
  struct MapReduceJob {
    std::string jobId;
    std::string mapperScript;
    std::string reducerScript;
    std::string inputPath;
    std::string outputPath;
  };

  struct MapReduceResult {
    bool success{false};
    std::string jobId;
    std::string errorMessage;
  };

  // Ejecutar job MapReduce
  static MapReduceResult executeJob(const MapReduceJob& job);
};

#else

// Stub cuando Hadoop no está disponible
class MapReduceEngine {
public:
  struct MapReduceJob {
    std::string jobId;
  };
  struct MapReduceResult {
    bool success{false};
    std::string errorMessage;
  };
  static MapReduceResult executeJob(const MapReduceJob&) {
    return MapReduceResult{false, "Hadoop/MapReduce not available"};
  }
};

#endif // HAVE_HADOOP

#endif // MAPREDUCE_ENGINE_H
