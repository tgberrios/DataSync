#include "ThreadManager.h"
#include <algorithm>

ThreadManager::ThreadManager() {
  Logger::getInstance().info(LogCategory::MONITORING,
                             "ThreadManager initialized");
}

ThreadManager::~ThreadManager() {
  stop();
  joinAllThreads();
}

void ThreadManager::start() {
  std::lock_guard<std::mutex> lock(threadMutex);
  running = true;
  Logger::getInstance().info(LogCategory::MONITORING, "ThreadManager started");
}

void ThreadManager::stop() {
  std::lock_guard<std::mutex> lock(threadMutex);
  running = false;
  threadCV.notify_all();
  Logger::getInstance().info(LogCategory::MONITORING, "ThreadManager stopped");
}

void ThreadManager::waitForAll() {
  joinAllThreads();
  Logger::getInstance().info(LogCategory::MONITORING, "All threads completed");
}

bool ThreadManager::isRunning() const { return running.load(); }

void ThreadManager::addThread(std::function<void()> threadFunction) {
  addThread("unnamed", threadFunction);
}

void ThreadManager::addThread(const std::string &name,
                              std::function<void()> threadFunction) {
  std::lock_guard<std::mutex> lock(threadMutex);

  if (!running) {
    Logger::getInstance().warning(LogCategory::MONITORING,
                                  "Cannot add thread '" + name +
                                      "' - ThreadManager is stopped");
    return;
  }

  try {
    threads.emplace_back([this, name, threadFunction]() {
      try {
        Logger::getInstance().info(LogCategory::MONITORING,
                                   "Thread '" + name + "' started");
        threadFunction();
        Logger::getInstance().info(LogCategory::MONITORING,
                                   "Thread '" + name +
                                       "' completed successfully");
      } catch (const std::exception &e) {
        Logger::getInstance().error(LogCategory::MONITORING,
                                    "Thread '" + name +
                                        "' failed: " + std::string(e.what()));
      }
    });

    Logger::getInstance().info(LogCategory::MONITORING,
                               "Thread '" + name + "' added successfully");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Failed to add thread '" + name +
                                    "': " + std::string(e.what()));
  }
}

void ThreadManager::sleepFor(int seconds) { sleepFor("current", seconds); }

void ThreadManager::sleepFor(const std::string &threadName, int seconds) {
  if (seconds <= 0)
    return;

  Logger::getInstance().debug(LogCategory::MONITORING,
                              "Thread '" + threadName + "' sleeping for " +
                                  std::to_string(seconds) + " seconds");

  std::unique_lock<std::mutex> lock(threadMutex);
  threadCV.wait_for(lock, std::chrono::seconds(seconds),
                    [this] { return !running; });
}

void ThreadManager::joinAllThreads() {
  std::lock_guard<std::mutex> lock(threadMutex);

  for (auto &thread : threads) {
    if (thread.joinable()) {
      try {
        thread.join();
      } catch (const std::exception &e) {
        Logger::getInstance().error(LogCategory::MONITORING,
                                    "Error joining thread: " +
                                        std::string(e.what()));
      }
    }
  }

  threads.clear();
  Logger::getInstance().info(LogCategory::MONITORING,
                             "All threads joined and cleared");
}

void ThreadManager::logThreadStatus(const std::string &action,
                                    const std::string &threadName) {
  std::string message = action;
  if (!threadName.empty()) {
    message += " for thread '" + threadName + "'";
  }
  Logger::getInstance().info(LogCategory::MONITORING, message);
}
