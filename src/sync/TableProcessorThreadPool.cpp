#include "sync/TableProcessorThreadPool.h"

TableProcessorThreadPool::TableProcessorThreadPool(size_t numWorkers) {
  if (numWorkers == 0) {
    numWorkers = std::max<size_t>(1, std::thread::hardware_concurrency());
    Logger::warning(LogCategory::TRANSFER, "TableProcessorThreadPool",
                    "numWorkers was 0, using hardware_concurrency: " +
                        std::to_string(numWorkers));
  }

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

  tasks_.finish();

  for (auto &worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }

  Logger::info(LogCategory::TRANSFER, "TableProcessorThreadPool",
               "Thread pool shut down successfully");
}

