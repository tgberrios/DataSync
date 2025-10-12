#include "sync/TableProcessorThreadPool.h"

TableProcessorThreadPool::TableProcessorThreadPool(size_t numWorkers) {
  if (numWorkers == 0) {
    numWorkers = std::max<size_t>(1, std::thread::hardware_concurrency());
    Logger::warning(LogCategory::TRANSFER, "TableProcessorThreadPool",
                    "numWorkers was 0, using hardware_concurrency: " +
                        std::to_string(numWorkers));
  }

  startTime_ = std::chrono::steady_clock::now();

  workers_.reserve(numWorkers);
  for (size_t i = 0; i < numWorkers; ++i) {
    workers_.emplace_back(&TableProcessorThreadPool::workerThread, this, i);
  }

  Logger::info(LogCategory::TRANSFER, "TableProcessorThreadPool",
               "Created thread pool with " + std::to_string(numWorkers) +
                   " workers");
}

TableProcessorThreadPool::~TableProcessorThreadPool() { shutdown(); }

void TableProcessorThreadPool::workerThread(size_t workerId) {
  Logger::info(LogCategory::TRANSFER,
               "Worker #" + std::to_string(workerId) + " started");

  while (!shutdown_.load()) {
    TableTask task;
    if (!tasks_.popBlocking(task)) {
      break;
    }

    activeWorkers_++;

    try {
      Logger::info(LogCategory::TRANSFER,
                   "Worker #" + std::to_string(workerId) +
                       " processing table: " + task.table.schema_name + "." +
                       task.table.table_name);

      task.processor(task.table);

      completedTasks_++;

      Logger::info(LogCategory::TRANSFER,
                   "Worker #" + std::to_string(workerId) +
                       " completed table: " + task.table.schema_name + "." +
                       task.table.table_name + " (Total: " +
                       std::to_string(completedTasks_.load()) + ")");

    } catch (const std::exception &e) {
      failedTasks_++;
      Logger::error(LogCategory::TRANSFER,
                    "Worker #" + std::to_string(workerId) +
                        " failed processing table: " + task.table.schema_name +
                        "." + task.table.table_name +
                        " - Error: " + std::string(e.what()));
    }

    activeWorkers_--;
  }

  Logger::info(LogCategory::TRANSFER,
               "Worker #" + std::to_string(workerId) + " stopped");
}

void TableProcessorThreadPool::submitTask(
    const DatabaseToPostgresSync::TableInfo &table,
    std::function<void(const DatabaseToPostgresSync::TableInfo &)> processor) {
  if (shutdown_.load()) {
    Logger::warning(
        LogCategory::TRANSFER, "submitTask",
        "Cannot submit task - thread pool is shutting down: " +
            table.schema_name + "." + table.table_name);
    return;
  }

  tasks_.push(TableTask{table, std::move(processor)});
  totalTasksSubmitted_++;
}

void TableProcessorThreadPool::waitForCompletion() {
  Logger::info(LogCategory::TRANSFER, "TableProcessorThreadPool",
               "Waiting for all tasks to complete...");

  tasks_.finish();

  for (auto &worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }

  Logger::info(LogCategory::TRANSFER, "TableProcessorThreadPool",
               "All tasks completed - Completed: " +
                   std::to_string(completedTasks_.load()) +
                   " | Failed: " + std::to_string(failedTasks_.load()));
}

void TableProcessorThreadPool::shutdown() {
  if (shutdown_.exchange(true)) {
    return;
  }

  Logger::info(LogCategory::TRANSFER, "TableProcessorThreadPool",
               "Shutting down thread pool...");

  monitoringEnabled_ = false;
  if (monitoringThread_.joinable()) {
    monitoringThread_.join();
  }

  tasks_.finish();

  for (auto &worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }

  Logger::info(LogCategory::TRANSFER, "TableProcessorThreadPool",
               "Thread pool shut down successfully");
}

void TableProcessorThreadPool::enableMonitoring(bool enable) {
  if (enable && !monitoringEnabled_.load()) {
    monitoringEnabled_ = true;
    monitoringThread_ =
        std::thread(&TableProcessorThreadPool::monitoringThreadFunc, this);
    Logger::info(LogCategory::TRANSFER, "TableProcessorThreadPool",
                 "Monitoring enabled - reporting every 10 seconds");
  } else if (!enable && monitoringEnabled_.load()) {
    monitoringEnabled_ = false;
    if (monitoringThread_.joinable()) {
      monitoringThread_.join();
    }
    Logger::info(LogCategory::TRANSFER, "TableProcessorThreadPool",
                 "Monitoring disabled");
  }
}

void TableProcessorThreadPool::monitoringThreadFunc() {
  std::this_thread::sleep_for(std::chrono::seconds(5));

  while (monitoringEnabled_.load() && !shutdown_.load()) {
    size_t active = activeWorkers_.load();
    size_t completed = completedTasks_.load();
    size_t failed = failedTasks_.load();
    size_t pending = tasks_.size();
    size_t total = totalWorkers();
    double speed = getTasksPerSecond();

    if (completed > 0 || active > 0) {
      Logger::info(LogCategory::TRANSFER,
                   "═══ ThreadPool Monitor ═══ Active: " +
                       std::to_string(active) + "/" + std::to_string(total) +
                       " | Completed: " + std::to_string(completed) + "/" +
                       std::to_string(totalTasksSubmitted_) +
                       " | Failed: " + std::to_string(failed) +
                       " | Pending: " + std::to_string(pending) +
                       " | Speed: " + std::to_string(static_cast<int>(speed)) +
                       " tbl/s");
    }

    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
}

double TableProcessorThreadPool::getTasksPerSecond() const {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now -
                                                                   startTime_)
                     .count();

  if (elapsed == 0)
    return 0.0;

  return static_cast<double>(completedTasks_.load()) /
         static_cast<double>(elapsed);
}

