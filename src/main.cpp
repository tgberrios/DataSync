#include "Config.h"
#include "StreamingData.h"
#include <iostream>

int main() {
  Logger::initialize();
  Logger::info(LogCategory::SYSTEM, "main", "DataSync started");

  // Load configuration from config.json
  DatabaseConfig::loadFromConfig();
  SyncConfig::loadFromConfig();

  Logger::info(LogCategory::SYSTEM, "main",
               "Configuration loaded successfully");

  std::cout << "Running... :)" << std::endl;

  StreamingData sd;
  sd.initialize();
  sd.run();

  return 0;
}
