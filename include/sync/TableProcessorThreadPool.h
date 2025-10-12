#ifndef TABLEPROCESSORTHREADPOOL_H
#define TABLEPROCESSORTHREADPOOL_H

#include "core/logger.h"
#include "sync/DatabaseToPostgresSync.h"
#include "sync/ParallelProcessing.h"
#include <atomic>
#include <functional>
#include <thread>
#include <vector>

struct TableTask {
  DatabaseToPostgresSync::TableInfo table;
  std::function<void(const DatabaseToPostgresSync::TableInfo &)> processor;
};

class TableProcessorThreadPool {
private:
  std::vector<std::thread> workers_;
  ThreadSafeQueue<TableTask> tasks_;
  std::atomic<size_t> activeWorkers_{0};
  std::atomic<size_t> completedTasks_{0};
  std::atomic<size_t> failedTasks_{0};
  std::atomic<bool> shutdown_{false};

  void workerThread(size_t workerId);

public:
  explicit TableProcessorThreadPool(size_t numWorkers);
  ~TableProcessorThreadPool();

  TableProcessorThreadPool(const TableProcessorThreadPool &) = delete;
  TableProcessorThreadPool &
  operator=(const TableProcessorThreadPool &) = delete;

  void submitTask(const DatabaseToPostgresSync::TableInfo &table,
                  std::function<void(const DatabaseToPostgresSync::TableInfo &)>
                      processor);

  void waitForCompletion();
  void shutdown();

  size_t activeWorkers() const { return activeWorkers_.load(); }
  size_t completedTasks() const { return completedTasks_.load(); }
  size_t failedTasks() const { return failedTasks_.load(); }
  size_t pendingTasks() const { return tasks_.size(); }
  size_t totalWorkers() const { return workers_.size(); }
};

#endif

