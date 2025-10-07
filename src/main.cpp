#include "Config.h"
#include "StreamingData.h"
#include <iostream>

int main() {
  try {
    // Initialize logger with error handling
    Logger::initialize();
    Logger::info(LogCategory::SYSTEM, "main", "DataSync started");

    Logger::info(LogCategory::SYSTEM, "main", "Using hardcoded configuration");

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
    std::cout << "\n" << std::endl;

    // Initialize StreamingData with error handling
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

    // Run StreamingData with error handling
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

    // Clean shutdown
    Logger::shutdown();
    return 0;

  } catch (const std::exception &e) {
    std::cerr << "Critical error in main: " << e.what() << std::endl;
    try {
      Logger::shutdown();
    } catch (...) {
      // Ignore errors during shutdown
    }
    return 4;
  } catch (...) {
    std::cerr << "Unknown critical error in main" << std::endl;
    try {
      Logger::shutdown();
    } catch (...) {
      // Ignore errors during shutdown
    }
    return 5;
  }
}
