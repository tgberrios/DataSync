#ifndef TASK_QUEUE_H
#define TASK_QUEUE_H

#include "catalog/workflow_repository.h"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <vector>

class TaskQueue {
public:
  struct QueuedTask {
    std::string workflow_name;
    std::string task_name;
    int priority;
    std::chrono::system_clock::time_point queued_at;
    json task_config;
  };
  
  static TaskQueue& getInstance();
  
  void start(int numWorkers = 4);
  void stop();
  bool isRunning() const;
  
  void enqueue(const QueuedTask& task);
  QueuedTask dequeue();
  size_t size() const;
  void clear();
  
  void setWorkerPoolSize(int numWorkers);
  int getWorkerPoolSize() const;
  
private:
  TaskQueue() = default;
  ~TaskQueue();
  TaskQueue(const TaskQueue&) = delete;
  TaskQueue& operator=(const TaskQueue&) = delete;
  
  struct TaskComparator {
    bool operator()(const QueuedTask& a, const QueuedTask& b) const {
      if (a.priority != b.priority) {
        return a.priority < b.priority;
      }
      return a.queued_at > b.queued_at;
    }
  };
  
  std::atomic<bool> running_;
  std::priority_queue<QueuedTask, std::vector<QueuedTask>, TaskComparator> queue_;
  mutable std::mutex queueMutex_;
  std::condition_variable queueCondition_;
  std::vector<std::thread> workers_;
  int numWorkers_;
  
  void workerLoop();
  void processTask(const QueuedTask& task);
};

#endif
