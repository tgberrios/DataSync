#include "utils/CacheManager.h"
#include <algorithm>
#include <optional>

CacheManager::CacheManager(size_t maxSize, std::chrono::seconds defaultTTL)
    : maxSize_(maxSize), defaultTTL_(defaultTTL) {
  stats_.maxSize = maxSize_;
  Logger::info(LogCategory::SYSTEM, "CacheManager",
               "Initialized with max size: " + std::to_string(maxSize_) + 
               ", default TTL: " + std::to_string(defaultTTL_.count()) + "s");
}

std::optional<json> CacheManager::get(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = cache_.find(key);
  if (it == cache_.end()) {
    updateStats(false);
    return std::nullopt;
  }

  CacheEntry& entry = it->second.entry;
  
  // Verificar si está expirado
  if (isExpired(entry)) {
    // Remover entrada expirada
    accessOrder_.erase(it->second.lruIterator);
    cache_.erase(it);
    updateStats(false);
    return std::nullopt;
  }

  // Actualizar acceso (LRU)
  touch(key);
  updateStats(true);
  
  return entry.value;
}

void CacheManager::put(const std::string& key, const json& value, 
                       std::optional<std::chrono::seconds> ttl) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto now = std::chrono::system_clock::now();
  auto expiresAt = now + (ttl.has_value() ? ttl.value() : defaultTTL_);

  auto it = cache_.find(key);
  if (it != cache_.end()) {
    // Actualizar entrada existente
    it->second.entry.value = value;
    it->second.entry.expiresAt = expiresAt;
    it->second.entry.createdAt = now;
    touch(key);
    return;
  }

  // Verificar si necesitamos evictar
  if (cache_.size() >= maxSize_) {
    evictLRU();
  }

  // Crear nueva entrada
  CacheEntry entry;
  entry.key = key;
  entry.value = value;
  entry.createdAt = now;
  entry.expiresAt = expiresAt;
  entry.lastAccessed = now;

  // Agregar al LRU
  accessOrder_.push_front(key);
  auto lruIt = accessOrder_.begin();

  EntryNode node;
  node.entry = entry;
  node.lruIterator = lruIt;
  cache_[key] = node;

  stats_.currentSize = cache_.size();
}

bool CacheManager::exists(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = cache_.find(key);
  if (it == cache_.end()) {
    return false;
  }

  if (isExpired(it->second.entry)) {
    accessOrder_.erase(it->second.lruIterator);
    cache_.erase(it);
    return false;
  }

  return true;
}

bool CacheManager::remove(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = cache_.find(key);
  if (it == cache_.end()) {
    return false;
  }

  accessOrder_.erase(it->second.lruIterator);
  cache_.erase(it);
  stats_.currentSize = cache_.size();
  return true;
}

void CacheManager::clear() {
  std::lock_guard<std::mutex> lock(mutex_);

  cache_.clear();
  accessOrder_.clear();
  stats_.currentSize = 0;
  stats_.hits = 0;
  stats_.misses = 0;
  stats_.evictions = 0;
  stats_.hitRate = 0.0;

  Logger::info(LogCategory::SYSTEM, "CacheManager", "Cache cleared");
}

CacheManager::CacheStats CacheManager::getStats() const {
  std::lock_guard<std::mutex> lock(mutex_);

  CacheStats stats = stats_;
  stats.currentSize = cache_.size();
  
  size_t total = stats.hits + stats.misses;
  if (total > 0) {
    stats.hitRate = static_cast<double>(stats.hits) / static_cast<double>(total);
  }

  return stats;
}

void CacheManager::setMaxSize(size_t maxSize) {
  std::lock_guard<std::mutex> lock(mutex_);

  maxSize_ = maxSize;
  stats_.maxSize = maxSize;

  // Evictar si es necesario
  while (cache_.size() > maxSize_) {
    evictLRU();
  }

  Logger::info(LogCategory::SYSTEM, "CacheManager",
               "Max size set to: " + std::to_string(maxSize_));
}

void CacheManager::setDefaultTTL(std::chrono::seconds ttl) {
  std::lock_guard<std::mutex> lock(mutex_);
  defaultTTL_ = ttl;
}

size_t CacheManager::cleanupExpired() {
  std::lock_guard<std::mutex> lock(mutex_);

  size_t removed = 0;
  auto it = cache_.begin();
  
  while (it != cache_.end()) {
    if (isExpired(it->second.entry)) {
      accessOrder_.erase(it->second.lruIterator);
      it = cache_.erase(it);
      removed++;
    } else {
      ++it;
    }
  }

  stats_.currentSize = cache_.size();
  
  if (removed > 0) {
    Logger::debug(LogCategory::SYSTEM, "CacheManager",
                  "Cleaned up " + std::to_string(removed) + " expired entries");
  }

  return removed;
}

std::vector<std::string> CacheManager::getKeys() const {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<std::string> keys;
  keys.reserve(cache_.size());

  for (const auto& [key, node] : cache_) {
    if (!isExpired(node.entry)) {
      keys.push_back(key);
    }
  }

  return keys;
}

size_t CacheManager::size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return cache_.size();
}

void CacheManager::touch(const std::string& key) {
  auto it = cache_.find(key);
  if (it == cache_.end()) {
    return;
  }

  // Mover al frente del LRU
  accessOrder_.erase(it->second.lruIterator);
  accessOrder_.push_front(key);
  it->second.lruIterator = accessOrder_.begin();
  
  // Actualizar estadísticas de acceso
  it->second.entry.lastAccessed = std::chrono::system_clock::now();
  it->second.entry.accessCount++;
}

void CacheManager::evictLRU() {
  if (accessOrder_.empty()) {
    return;
  }

  // Remover el menos recientemente usado (último en la lista)
  std::string lruKey = accessOrder_.back();
  accessOrder_.pop_back();
  cache_.erase(lruKey);
  
  stats_.evictions++;
  stats_.currentSize = cache_.size();

  Logger::debug(LogCategory::SYSTEM, "CacheManager",
                "Evicted LRU key: " + lruKey);
}

bool CacheManager::isExpired(const CacheEntry& entry) const {
  auto now = std::chrono::system_clock::now();
  return now > entry.expiresAt;
}

void CacheManager::updateStats(bool hit) {
  if (hit) {
    stats_.hits++;
  } else {
    stats_.misses++;
  }

  size_t total = stats_.hits + stats_.misses;
  if (total > 0) {
    stats_.hitRate = static_cast<double>(stats_.hits) / static_cast<double>(total);
  }
}
