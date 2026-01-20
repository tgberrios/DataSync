#include "sync/TaskQueue.h"
#include "sync/WorkflowExecutor.h"
#include "core/database_config.h"
#include "core/logger.h"

TaskQueue& TaskQueue::getInstance() {
  static TaskQueue instance;
  return instance;
}

TaskQueue::~TaskQueue() {
  stop();
}

void TaskQueue::start(int numWorkers) {
  if (running_.exchange(true)) {
    return;
  }
  
  numWorkers_ = numWorkers;
  workers_.clear();
  workers_.reserve(numWorkers);
  
  for (int i = 0; i < numWorkers; i++) {
    workers_.emplace_back(&TaskQueue::workerLoop, this);
  }
  
  Logger::info(LogCategory::MONITORING, "TaskQueue",
               "Task queue started with " + std::to_string(numWorkers) + " workers");
}

void TaskQueue::stop() {
  if (!running_.exchange(false)) {
    return;
  }
  
  queueCondition_.notify_all();
  
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  
  workers_.clear();
  Logger::info(LogCategory::MONITORING, "TaskQueue",
               "Task queue stopped");
}

bool TaskQueue::isRunning() const {
  return running_.load();
}

void TaskQueue::enqueue(const QueuedTask& task) {
  std::lock_guard<std::mutex> lock(queueMutex_);
  queue_.push(task);
  queueCondition_.notify_one();
  Logger::info(LogCategory::MONITORING, "enqueue",
               "Task queued: " + task.workflow_name + "::" + task.task_name +
               " (priority: " + std::to_string(task.priority) + ")");
}

QueuedTask TaskQueue::dequeue() {
  std::unique_lock<std::mutex> lock(queueMutex_);
  queueCondition_.wait(lock, [this] { return !queue_.empty() || !running_.load(); });
  
  if (queue_.empty()) {
    return QueuedTask{};
  }
  
  QueuedTask task = queue_.top();
  queue_.pop();
  return task;
}

size_t TaskQueue::size() const {
  std::lock_guard<std::mutex> lock(queueMutex_);
  return queue_.size();
}

void TaskQueue::clear() {
  std::lock_guard<std::mutex> lock(queueMutex_);
  while (!queue_.empty()) {
    queue_.pop();
  }
}

void TaskQueue::setWorkerPoolSize(int numWorkers) {
  if (numWorkers != numWorkers_) {
    stop();
    start(numWorkers);
  }
}

int TaskQueue::getWorkerPoolSize() const {
  return numWorkers_;
}

void TaskQueue::workerLoop() {
  while (running_.load()) {
    QueuedTask task = dequeue();
    
    if (task.workflow_name.empty()) {
      continue;
    }
    
    processTask(task);
  }
}

void TaskQueue::processTask(const QueuedTask& task) {
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    WorkflowExecutor executor(connStr);
    
    Logger::info(LogCategory::MONITORING, "processTask",
                 "Processing queued task: " + task.workflow_name + "::" + task.task_name);
    
    executor.executeWorkflowAsync(task.workflow_name, TriggerType::MANUAL);
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "processTask",
                  "Error processing queued task: " + std::string(e.what()));
  }
}
