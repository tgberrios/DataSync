#ifndef PARALLELPROCESSING_H
#define PARALLELPROCESSING_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

// Thread-safe queue template
template <typename T> class ThreadSafeQueue {
private:
  mutable std::mutex mtx;
  std::queue<T> queue;
  std::condition_variable cv;
  std::atomic<bool> shutdown{false};

public:
  void push(T item) {
    std::lock_guard<std::mutex> lock(mtx);
    queue.push(std::move(item));
    cv.notify_one();
  }

  bool pop(T &item,
           std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) {
    std::unique_lock<std::mutex> lock(mtx);
    if (cv.wait_for(lock, timeout,
                    [this] { return !queue.empty() || shutdown; })) {
      if (!queue.empty()) {
        item = std::move(queue.front());
        queue.pop();
        return true;
      }
    }
    return false;
  }

  void shutdown_queue() {
    shutdown = true;
    cv.notify_all();
  }

  void finish() { shutdown_queue(); }

  void reset_queue() { shutdown = false; }

  void clear() {
    std::lock_guard<std::mutex> lock(mtx);
    std::queue<T> empty;
    queue.swap(empty);
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mtx);
    return queue.size();
  }

  bool empty() const {
    std::lock_guard<std::mutex> lock(mtx);
    return queue.empty();
  }

  bool popBlocking(T &item) {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this] { return !queue.empty() || shutdown; });

    if (queue.empty()) {
      return false;
    }

    item = std::move(queue.front());
    queue.pop();
    return true;
  }
};

// Common data structures for parallel processing
namespace ParallelProcessing {
struct DataChunk {
  std::vector<std::vector<std::string>> rawData;
  size_t chunkNumber;
  std::string schemaName;
  std::string tableName;
  bool isLastChunk = false;
};

struct PreparedBatch {
  std::string batchQuery;
  size_t batchSize;
  size_t chunkNumber;
  std::string schemaName;
  std::string tableName;
};

struct ProcessedResult {
  size_t chunkNumber;
  std::string schemaName;
  std::string tableName;
  size_t rowsProcessed;
  bool success;
  std::string errorMessage;
};
} // namespace ParallelProcessing

#endif // PARALLELPROCESSING_H
