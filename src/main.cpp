#include "Config.h"
#include "StreamingData.h"
#include <iostream>

int main() {
  try {
    // Initialize logger with error handling
    Logger::getInstance().initialize();
    Logger::getInstance().info(LogCategory::SYSTEM, "main", "DataSync started");

    // Load configuration from database with error handling
    try {
      DatabaseConfig::loadFromDatabase();
      SyncConfig::loadFromDatabase();
      Logger::getInstance().info(LogCategory::SYSTEM, "main",
                   "Configuration loaded successfully from database");
    } catch (const std::exception &e) {
      Logger::getInstance().error(LogCategory::SYSTEM, "main",
                    "Failed to load configuration from database: " + std::string(e.what()));
      std::cerr << "Configuration error: " << e.what() << std::endl;
      Logger::getInstance().shutdown();
      return 1;
    }

    std::cout << "Running... :)" << std::endl;

    // Initialize StreamingData with error handling
    StreamingData sd;
    try {
      sd.initialize();
      Logger::getInstance().info(LogCategory::SYSTEM, "main",
                   "StreamingData initialized successfully");
    } catch (const std::exception &e) {
      Logger::getInstance().error(LogCategory::SYSTEM, "main",
                    "Exception during StreamingData initialization: " +
                        std::string(e.what()));
      std::cerr << "Initialization error: " << e.what() << std::endl;
      Logger::getInstance().shutdown();
      return 2;
    }

    // Run StreamingData with error handling
    try {
      sd.run();
      Logger::getInstance().info(LogCategory::SYSTEM, "main",
                   "DataSync completed successfully");
    } catch (const std::exception &e) {
      Logger::getInstance().error(LogCategory::SYSTEM, "main",
                    "Exception during DataSync execution: " +
                        std::string(e.what()));
      std::cerr << "Execution error: " << e.what() << std::endl;
      Logger::getInstance().shutdown();
      return 3;
    }

    // Clean shutdown
    Logger::getInstance().shutdown();
    return 0;

  } catch (const std::exception &e) {
    std::cerr << "Critical error in main: " << e.what() << std::endl;
    try {
      Logger::getInstance().shutdown();
    } catch (...) {
      // Ignore errors during shutdown
    }
    return 4;
  } catch (...) {
    std::cerr << "Unknown critical error in main" << std::endl;
    try {
      Logger::getInstance().shutdown();
    } catch (...) {
      // Ignore errors during shutdown
    }
    return 5;
  }
}
