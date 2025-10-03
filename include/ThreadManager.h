#ifndef THREADMANAGER_H
#define THREADMANAGER_H

#include "logger.h"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

class ThreadManager {
public:
  ThreadManager();
  ~ThreadManager();

  // Thread control
  void start();
  void stop();
  void waitForAll();
  bool isRunning() const;

  // Thread management
  void addThread(std::function<void()> threadFunction);
  void addThread(const std::string &name, std::function<void()> threadFunction);

  // Thread utilities
  void sleepFor(int seconds);
  void sleepFor(const std::string &threadName, int seconds);

private:
  std::atomic<bool> running{true};
  std::vector<std::thread> threads;
  std::mutex threadMutex;
  std::condition_variable threadCV;

  // Helper methods
  void joinAllThreads();
  void logThreadStatus(const std::string &action,
                       const std::string &threadName = "");
};

#endif // THREADMANAGER_H
