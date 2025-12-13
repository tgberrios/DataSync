#include "core/Config.h"
#include "sync/StreamingData.h"
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

namespace {
std::atomic<bool> g_shutdownRequested{false};
StreamingData *g_streamingData = nullptr;
std::atomic<bool> g_forceExit{false};

void signalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    std::cerr << "\n\nReceived shutdown signal (" << signal
              << "). Initiating graceful shutdown...\n";
    g_shutdownRequested = true;
    if (g_streamingData) {
      g_streamingData->shutdown();
    }
  }
}
} // namespace

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

    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    StreamingData sd;
    g_streamingData = &sd;
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
    } catch (const std::exception &e) {
      std::cerr << "Error during logger shutdown: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "Unknown error during logger shutdown" << std::endl;
    }
    return 5;
  }
}
