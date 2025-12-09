#include "sync/TableProcessorThreadPool.h"

// Constructs a thread pool with the specified number of worker threads. If
// numWorkers is 0, automatically uses the hardware concurrency (number of CPU
// cores). Initializes all atomic counters to 0, creates worker threads, and
// records the start time for performance metrics. The thread pool is ready to
// accept tasks immediately after construction.
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

// Destructor automatically shuts down the thread pool, ensuring all worker
// threads are properly joined and resources are cleaned up.
TableProcessorThreadPool::~TableProcessorThreadPool() { shutdown(); }

// Main worker thread function that processes tasks from the queue. Continuously
// pops tasks from the blocking queue until shutdown is requested. For each
// task, increments activeWorkers counter, executes the processor function,
// increments completedTasks on success or failedTasks on exception, then
// decrements activeWorkers. Logs all processing activity including start,
// completion, and failures. Thread-safe through atomic counters and thread-safe
// queue.
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

// Submits a new task to the thread pool for processing. Takes a TableInfo
// struct and a processor function that will be called with that table. If the
// thread pool is shutting down, logs a warning and returns without adding the
// task. Otherwise, pushes the task to the queue and increments
// totalTasksSubmitted. Thread-safe through atomic shutdown flag and
// thread-safe queue.
void TableProcessorThreadPool::submitTask(
    const DatabaseToPostgresSync::TableInfo &table,
    std::function<void(const DatabaseToPostgresSync::TableInfo &)> processor) {
  if (shutdown_.load()) {
    Logger::warning(LogCategory::TRANSFER, "submitTask",
                    "Cannot submit task - thread pool is shutting down: " +
                        table.schema_name + "." + table.table_name);
    return;
  }

  tasks_.push(TableTask{table, std::move(processor)});
  totalTasksSubmitted_++;
}

// Waits for all submitted tasks to complete. Signals the queue to finish
// accepting new tasks, then joins all worker threads. Blocks until all tasks
// are processed. Logs completion statistics including completed and failed task
// counts. This method should be called before destroying the thread pool to
// ensure clean shutdown.
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

// Shuts down the thread pool gracefully. Sets the shutdown flag atomically,
// disables monitoring if active, joins the monitoring thread, signals the queue
// to finish, and joins all worker threads. Idempotent - can be called multiple
// times safely. Ensures all threads are properly cleaned up before returning.
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

// Enables or disables the monitoring thread that periodically reports thread
// pool statistics. When enabling, creates a new monitoring thread that reports
// every 10 seconds. When disabling, joins the existing monitoring thread if
// it's running. Thread-safe through atomic monitoringEnabled flag. Monitoring
// reports include active workers, completed/failed/pending tasks, and
// processing speed (tables per second).
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

// Monitoring thread function that periodically reports thread pool statistics.
// Waits 5 seconds before first report, then reports every 10 seconds while
// monitoring is enabled and shutdown hasn't been requested. Reports include
// active workers, completed/failed/pending tasks, total submitted tasks, and
// processing speed. Only reports if there are active workers or completed
// tasks to avoid noise. Thread-safe through atomic counters.
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
                       " | Pending: " + std::to_string(pending) + " | Speed: " +
                       std::to_string(static_cast<int>(speed)) + " tbl/s");
    }

    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
}

// Calculates and returns the average processing speed in tasks per second.
// Computes elapsed time since thread pool creation, divides completed tasks by
// elapsed seconds. Returns 0.0 if elapsed time is 0 to avoid division by
// zero. Thread-safe through atomic completedTasks counter and const method
// (read-only access to startTime_).
double TableProcessorThreadPool::getTasksPerSecond() const {
  auto now = std::chrono::steady_clock::now();
  auto elapsed =
      std::chrono::duration_cast<std::chrono::seconds>(now - startTime_)
          .count();

  if (elapsed == 0)
    return 0.0;

  return static_cast<double>(completedTasks_.load()) /
         static_cast<double>(elapsed);
}
