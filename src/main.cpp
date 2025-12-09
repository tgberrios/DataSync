#include "core/Config.h"
#include "sync/StreamingData.h"
#include <iostream>

// Main entry point for the DataSync application. Initializes the system by
// loading configuration from config.json, initializing the logger, displaying
// a startup banner, and launching the StreamingData system. Handles exceptions
// at multiple levels: initialization errors (return 2), execution errors
// (return 3), critical errors (return 4), and unknown errors (return 5).
// Ensures proper cleanup by calling Logger::shutdown() even on errors.
// Returns 0 on successful completion. This is the primary orchestrator that
// coordinates all system components.
int main() {
  try {
    DatabaseConfig::loadFromFile("config.json");

    Logger::initialize();
    Logger::info(LogCategory::SYSTEM, "main", "DataSync started");
    Logger::info(LogCategory::SYSTEM, "main",
                 "Configuration loaded from config.json (DB: " +
                     DatabaseConfig::getPostgresDB() + "@" +
                     DatabaseConfig::getPostgresHost() + ")");

    std::cout << "\n";
    std::cout << "██████╗  █████╗ ████████╗ █████╗ ███████╗██╗   ██╗███╗   ██╗ "
                 "██████╗\n";
    std::cout << "██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔════╝╚██╗ ██╔╝████╗  "
                 "██║██╔════╝\n";
    std::cout << "██║  ██║███████║   ██║   ███████║███████╗ ╚████╔╝ ██╔██╗ "
                 "██║██║     \n";
    std::cout << "██║  ██║██╔══██║   ██║   ██╔══██║╚════██║  ╚██╔╝  "
                 "██║╚██╗██║██║     \n";
    std::cout << "██████╔╝██║  ██║   ██║   ██║  ██║███████║   ██║   ██║ "
                 "╚████║╚██████╗\n";
    std::cout << "╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═══╝ "
                 "╚═════╝\n";
    std::cout << "\n";
    std::cout
        << "          Enterprise Data Synchronization & Replication System\n";
    std::cout << "                            Version 1.0.0\n";
    std::cout << "\n";

    StreamingData sd;
    try {
      sd.initialize();
      Logger::info(LogCategory::SYSTEM, "main",
                   "StreamingData initialized successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::SYSTEM, "main",
                    "Exception during StreamingData initialization: " +
                        std::string(e.what()));
      std::cerr << "Initialization error: " << e.what() << std::endl;
      Logger::shutdown();
      return 2;
    }

    try {
      sd.run();
      Logger::info(LogCategory::SYSTEM, "main",
                   "DataSync completed successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::SYSTEM, "main",
                    "Exception during DataSync execution: " +
                        std::string(e.what()));
      std::cerr << "Execution error: " << e.what() << std::endl;
      Logger::shutdown();
      return 3;
    }

    Logger::shutdown();
    return 0;

  } catch (const std::exception &e) {
    std::cerr << "Critical error in main: " << e.what() << std::endl;
    try {
      Logger::shutdown();
    } catch (...) {
    }
    return 4;
  } catch (...) {
    std::cerr << "Unknown critical error in main" << std::endl;
    try {
      Logger::shutdown();
    } catch (...) {
    }
    return 5;
  }
}
