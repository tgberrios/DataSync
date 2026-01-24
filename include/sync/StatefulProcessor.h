#ifndef STATEFUL_PROCESSOR_H
#define STATEFUL_PROCESSOR_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <map>
#include <memory>
#include <mutex>
#include <chrono>

using json = nlohmann::json;

// StatefulProcessor: Mantiene estado entre eventos para procesamiento stateful
class StatefulProcessor {
public:
  struct StateValue {
    json value;
    std::chrono::system_clock::time_point lastUpdated;
    int64_t updateCount{0};
  };

  struct StateSnapshot {
    std::map<std::string, StateValue> states;
    std::chrono::system_clock::time_point snapshotTime;
    int64_t totalKeys{0};
  };

  explicit StatefulProcessor();
  ~StatefulProcessor();

  // Obtener estado por key
  json getState(const std::string& key) const;

  // Obtener estado completo (StateValue)
  StateValue getStateValue(const std::string& key) const;

  // Actualizar estado
  bool updateState(const std::string& key, const json& value);

  // Actualizar estado con función de transformación
  bool updateStateWithFunction(const std::string& key,
                               std::function<json(const json& current, const json& event)> updateFn,
                               const json& event);

  // Limpiar estado de una key
  bool clearState(const std::string& key);

  // Limpiar todos los estados
  void clearAllStates();

  // Obtener snapshot del estado completo
  StateSnapshot getStateSnapshot() const;

  // Obtener todas las keys
  std::vector<std::string> getAllKeys() const;

  // Verificar si key existe
  bool hasKey(const std::string& key) const;

  // Obtener estadísticas
  json getStatistics() const;

  // Limpiar estados antiguos (más de X tiempo sin actualizar)
  void cleanupOldStates(int64_t maxAgeSeconds);

private:
  mutable std::mutex stateMutex_;
  std::map<std::string, StateValue> state_;

  // Contadores (mutable para permitir modificación en funciones const)
  mutable int64_t stateUpdates_{0};
  mutable int64_t stateGets_{0};
  mutable int64_t stateClears_{0};
};

#endif // STATEFUL_PROCESSOR_H
