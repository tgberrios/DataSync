#include "engines/mapreduce_engine.h"
#include "core/logger.h"
#include <cstdlib>
#include <fstream>

#ifdef HAVE_HADOOP

MapReduceEngine::MapReduceResult
MapReduceEngine::executeJob(const MapReduceJob& job) {
  MapReduceResult result;
  result.jobId = job.jobId;

  // Ejecutar via hadoop command
  std::string command = "hadoop jar ... " + job.inputPath + " " + job.outputPath;
  
  Logger::info(LogCategory::SYSTEM, "MapReduceEngine",
               "Executing MapReduce job: " + job.jobId);
  
  int exitCode = std::system(command.c_str());
  result.success = (exitCode == 0);
  
  if (!result.success) {
    result.errorMessage = "MapReduce job failed with exit code: " + std::to_string(exitCode);
  }
  
  return result;
}

#endif // HAVE_HADOOP
