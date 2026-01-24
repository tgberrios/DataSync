#ifndef MEMORY_MANAGER_H
#define MEMORY_MANAGER_H

#include "core/logger.h"
#include <cstddef>
#include <memory>
#include <mutex>
#include <chrono>
#include <functional>
#include <string>
#include <map>
#include <atomic>

// MemoryManager: Gestión avanzada de memoria con monitoreo, límites y spill to disk
class MemoryManager {
public:
  struct MemoryStats {
    size_t currentUsage{0};
    size_t peakUsage{0};
    size_t totalAllocated{0};
    size_t totalFreed{0};
    size_t allocationCount{0};
    size_t freeCount{0};
    size_t spillCount{0};
    size_t spillBytes{0};
    double averageAllocationSize{0.0};
  };

  struct MemoryLimit {
    size_t maxMemory;
    size_t warningThreshold;  // Porcentaje (0-100)
    size_t criticalThreshold;  // Porcentaje (0-100)
    bool enableSpill;
    std::string spillDirectory;
    
    MemoryLimit() : maxMemory(0), warningThreshold(0), criticalThreshold(0), enableSpill(false), spillDirectory("/tmp") {}
  };

  explicit MemoryManager(const MemoryLimit& limit = MemoryLimit());
  ~MemoryManager();

  // Allocate memory con tracking
  void* allocate(size_t size, const std::string& context = "");

  // Free memory con tracking
  void deallocate(void* ptr, size_t size);

  // Verificar si hay memoria disponible
  bool hasAvailableMemory(size_t requiredSize) const;

  // Obtener uso actual de memoria
  size_t getCurrentUsage() const;

  // Obtener estadísticas
  MemoryStats getStats() const;

  // Configurar límites
  void setLimit(const MemoryLimit& limit);

  // Spill data to disk cuando se excede memoria
  std::string spillToDisk(const void* data, size_t size, const std::string& prefix = "spill");

  // Load data from disk
  std::unique_ptr<char[]> loadFromDisk(const std::string& filePath, size_t& size);

  // Limpiar archivos de spill
  void cleanupSpillFiles();

  // Memory pool para reducir allocaciones
  class MemoryPool {
  public:
    explicit MemoryPool(size_t blockSize, size_t initialBlocks = 10);
    ~MemoryPool();

    void* acquire();
    void release(void* ptr);
    size_t getBlockSize() const { return blockSize_; }
    size_t getAvailableBlocks() const { return availableBlocks_; }

  private:
    size_t blockSize_;
    std::vector<void*> pool_;
    std::mutex mutex_;
    size_t availableBlocks_{0};
  };

  // Obtener o crear memory pool
  std::shared_ptr<MemoryPool> getPool(size_t blockSize);

  // Garbage collection inteligente
  void performGC();

  // Alertas cuando se acerca a límites
  void checkLimitsAndAlert();

private:
  mutable std::mutex mutex_;
  MemoryLimit limit_;
  MemoryStats stats_;

  // Memory pools por tamaño de bloque
  std::map<size_t, std::shared_ptr<MemoryPool>> pools_;

  // Tracking de allocations por contexto
  std::map<std::string, size_t> contextUsage_;

  // Archivos de spill
  std::vector<std::string> spillFiles_;

  // Callbacks para alertas
  std::function<void(size_t, size_t)> warningCallback_;
  std::function<void(size_t, size_t)> criticalCallback_;

  // Helper methods
  void updateStats(size_t allocated, size_t freed);
  size_t calculateWarningThreshold() const;
  size_t calculateCriticalThreshold() const;
  void triggerWarning();
  void triggerCritical();
};

#endif // MEMORY_MANAGER_H
