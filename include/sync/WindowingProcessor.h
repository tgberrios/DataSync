#ifndef WINDOWING_PROCESSOR_H
#define WINDOWING_PROCESSOR_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <chrono>
#include <memory>
#include <mutex>

using json = nlohmann::json;

// WindowingProcessor: Procesa eventos en ventanas de tiempo
class WindowingProcessor {
public:
  enum class WindowType {
    TUMBLING,   // Ventanas fijas sin solapamiento
    SLIDING,    // Ventanas con solapamiento
    SESSION     // Ventanas basadas en actividad
  };

  struct WindowConfig {
    WindowType windowType{WindowType::TUMBLING};
    int64_t windowSizeSeconds{60};        // Tamaño de ventana en segundos
    int64_t slideIntervalSeconds{60};     // Intervalo de deslizamiento (para sliding)
    int64_t sessionTimeoutSeconds{300};   // Timeout de sesión (para session windows)
  };

  struct Window {
    std::string windowId;
    std::chrono::system_clock::time_point startTime;
    std::chrono::system_clock::time_point endTime;
    std::vector<json> events;
    bool isClosed{false};
    json metadata;
  };

  struct WindowResult {
    std::string windowId;
    std::vector<json> events;
    int64_t eventCount{0};
    std::chrono::system_clock::time_point startTime;
    std::chrono::system_clock::time_point endTime;
    json aggregatedData;  // Datos agregados de la ventana
  };

  explicit WindowingProcessor(const WindowConfig& config);
  ~WindowingProcessor();

  // Crear nueva ventana
  std::string createWindow();

  // Agregar evento a ventana
  bool addEvent(const std::string& windowId, const json& event, 
                int64_t eventTimestamp = 0);

  // Obtener eventos de una ventana
  std::vector<json> getWindowedEvents(const std::string& windowId) const;

  // Cerrar ventana y obtener resultado
  WindowResult closeWindow(const std::string& windowId);

  // Obtener ventanas activas
  std::vector<std::string> getActiveWindows() const;

  // Procesar evento (crea/actualiza ventanas automáticamente)
  std::vector<WindowResult> processEvent(const json& event, int64_t eventTimestamp = 0);

  // Limpiar ventanas expiradas
  void cleanupExpiredWindows();

  // Obtener estadísticas
  json getStatistics() const;

private:
  WindowConfig config_;
  mutable std::mutex windowsMutex_;
  std::map<std::string, Window> windows_;
  std::map<std::string, std::string> sessionWindows_;  // key -> windowId para session windows

  // Contadores
  int64_t windowsCreated_{0};
  int64_t windowsClosed_{0};
  int64_t eventsProcessed_{0};

  // Generar ID único para ventana
  std::string generateWindowId() const;

  // Determinar si evento pertenece a ventana existente (session windows)
  std::string findSessionWindow(const json& event, int64_t eventTimestamp);

  // Crear nueva ventana tumbling/sliding
  std::string createNewWindow(int64_t eventTimestamp);

  // Verificar si ventana debe cerrarse
  bool shouldCloseWindow(const Window& window, int64_t currentTime) const;

  // Calcular tiempo de inicio de ventana (para tumbling/sliding)
  std::chrono::system_clock::time_point calculateWindowStart(int64_t eventTimestamp) const;
};

#endif // WINDOWING_PROCESSOR_H
