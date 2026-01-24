#include "sync/CEPProcessor.h"
#include "core/logger.h"
#include <sstream>
#include <random>
#include <algorithm>
#include <iomanip>

CEPProcessor::CEPProcessor() {
  Logger::info(LogCategory::SYSTEM, "CEPProcessor",
               "Initializing CEPProcessor");
}

CEPProcessor::~CEPProcessor() {
  std::lock_guard<std::mutex> rulesLock(rulesMutex_);
  std::lock_guard<std::mutex> matchesLock(matchesMutex_);
  rules_.clear();
  matches_.clear();
  patternStates_.clear();
}

bool CEPProcessor::addRule(const CEPRule& rule) {
  std::lock_guard<std::mutex> lock(rulesMutex_);
  
  if (rule.ruleId.empty()) {
    Logger::error(LogCategory::SYSTEM, "CEPProcessor",
                 "Rule ID cannot be empty");
    return false;
  }

  rules_[rule.ruleId] = rule;
  
  Logger::info(LogCategory::SYSTEM, "CEPProcessor",
               "CEP rule added: " + rule.ruleId + " - " + rule.name);
  return true;
}

bool CEPProcessor::removeRule(const std::string& ruleId) {
  std::lock_guard<std::mutex> lock(rulesMutex_);
  
  auto it = rules_.find(ruleId);
  if (it == rules_.end()) {
    Logger::warning(LogCategory::SYSTEM, "CEPProcessor",
                    "Rule not found: " + ruleId);
    return false;
  }

  rules_.erase(it);
  
  // Limpiar estado del patrón
  patternStates_.erase(ruleId);
  
  Logger::info(LogCategory::SYSTEM, "CEPProcessor",
               "CEP rule removed: " + ruleId);
  return true;
}

CEPProcessor::CEPRule CEPProcessor::getRule(const std::string& ruleId) const {
  std::lock_guard<std::mutex> lock(rulesMutex_);
  
  auto it = rules_.find(ruleId);
  if (it == rules_.end()) {
    return CEPRule{};
  }

  return it->second;
}

std::vector<CEPProcessor::CEPRule> CEPProcessor::getRules() const {
  std::lock_guard<std::mutex> lock(rulesMutex_);
  
  std::vector<CEPRule> result;
  for (const auto& [ruleId, rule] : rules_) {
    result.push_back(rule);
  }
  return result;
}

std::vector<CEPProcessor::PatternMatch> CEPProcessor::processEvent(
    const json& event, int64_t eventTimestamp) {
  
  std::vector<PatternMatch> newMatches;
  eventsProcessed_++;

  if (eventTimestamp == 0) {
    eventTimestamp = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
  }

  std::lock_guard<std::mutex> rulesLock(rulesMutex_);

  for (auto& [ruleId, rule] : rules_) {
    if (!rule.enabled) {
      continue;
    }

    // Verificar condiciones
    if (!rule.conditions.empty() && !evaluateCondition(event, rule.conditions)) {
      continue;
    }

    // Obtener o crear estado del patrón
    auto& state = patternStates_[ruleId];
    if (state.ruleId.empty()) {
      state.ruleId = ruleId;
      state.sequenceStart = std::chrono::system_clock::now();
    }

    // Agregar evento a la secuencia actual
    state.currentSequence.push_back(event);

    // Verificar si la secuencia cumple el patrón
    if (matchPattern(state.currentSequence, rule.pattern)) {
      // Patrón detectado
      PatternMatch match;
      match.matchId = generateMatchId();
      match.ruleId = ruleId;
      match.matchedEvents = state.currentSequence;
      match.matchTime = std::chrono::system_clock::now();
      match.metadata["pattern"] = rule.pattern;
      match.metadata["ruleName"] = rule.name;

      newMatches.push_back(match);

      // Guardar match
      {
        std::lock_guard<std::mutex> matchesLock(matchesMutex_);
        matches_.push_back(match);
      }

      patternsMatched_++;

      // Ejecutar acciones
      executeActions(ruleId, match);

      // Reiniciar estado del patrón
      state.currentSequence.clear();
      state.sequenceStart = std::chrono::system_clock::now();
      state.currentStep = 0;
    } else {
      // Verificar si la secuencia aún puede cumplir el patrón
      // Si no, limpiar y empezar de nuevo
      auto now = std::chrono::system_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
          now - state.sequenceStart).count();
      
      if (elapsed > rule.windowSeconds) {
        // Ventana expirada, reiniciar
        state.currentSequence.clear();
        state.sequenceStart = now;
        state.currentStep = 0;
      }
    }
  }

  return newMatches;
}

std::vector<CEPProcessor::PatternMatch> CEPProcessor::getMatches(
    const std::string& ruleId) const {
  
  std::lock_guard<std::mutex> lock(matchesMutex_);
  
  std::vector<PatternMatch> result;
  for (const auto& match : matches_) {
    if (ruleId.empty() || match.ruleId == ruleId) {
      result.push_back(match);
    }
  }
  return result;
}

void CEPProcessor::cleanupOldMatches(int64_t maxAgeSeconds) {
  std::lock_guard<std::mutex> lock(matchesMutex_);
  
  auto now = std::chrono::system_clock::now();
  auto it = matches_.begin();
  
  while (it != matches_.end()) {
    auto age = std::chrono::duration_cast<std::chrono::seconds>(
        now - it->matchTime).count();
    if (age > maxAgeSeconds) {
      it = matches_.erase(it);
    } else {
      ++it;
    }
  }
}

json CEPProcessor::getStatistics() const {
  std::lock_guard<std::mutex> rulesLock(rulesMutex_);
  std::lock_guard<std::mutex> matchesLock(matchesMutex_);
  
  json stats;
  stats["rulesCount"] = rules_.size();
  stats["eventsProcessed"] = eventsProcessed_;
  stats["patternsMatched"] = patternsMatched_;
  stats["matchesCount"] = matches_.size();
  stats["activePatternStates"] = patternStates_.size();
  
  return stats;
}

bool CEPProcessor::evaluateCondition(const json& event, const json& condition) const {
  // Evaluación simple de condiciones
  // Formato esperado: {"field": "value"} o {"field": {"op": ">", "value": 100}}
  
  if (!condition.is_object()) {
    return false;
  }

  for (const auto& [field, conditionValue] : condition.items()) {
    if (!event.contains(field)) {
      return false;
    }

    const auto& eventValue = event[field];

    if (conditionValue.is_object() && conditionValue.contains("op")) {
      // Operador de comparación
      std::string op = conditionValue["op"];
      const auto& compareValue = conditionValue["value"];

      if (op == "==") {
        if (eventValue != compareValue) return false;
      } else if (op == "!=") {
        if (eventValue == compareValue) return false;
      } else if (op == ">") {
        if (eventValue.is_number() && compareValue.is_number()) {
          if (eventValue.get<double>() <= compareValue.get<double>()) return false;
        } else {
          return false;
        }
      } else if (op == "<") {
        if (eventValue.is_number() && compareValue.is_number()) {
          if (eventValue.get<double>() >= compareValue.get<double>()) return false;
        } else {
          return false;
        }
      } else if (op == ">=") {
        if (eventValue.is_number() && compareValue.is_number()) {
          if (eventValue.get<double>() < compareValue.get<double>()) return false;
        } else {
          return false;
        }
      } else if (op == "<=") {
        if (eventValue.is_number() && compareValue.is_number()) {
          if (eventValue.get<double>() > compareValue.get<double>()) return false;
        } else {
          return false;
        }
      }
    } else {
      // Comparación directa
      if (eventValue != conditionValue) {
        return false;
      }
    }
  }

  return true;
}

bool CEPProcessor::matchPattern(const std::vector<json>& events,
                                const json& pattern) const {
  // Detección simple de patrones secuenciales
  // Formato esperado: {"sequence": [{"type": "A"}, {"type": "B"}, {"type": "C"}]}
  
  if (!pattern.is_object() || !pattern.contains("sequence")) {
    return false;
  }

  const auto& sequence = pattern["sequence"];
  if (!sequence.is_array()) {
    return false;
  }

  if (events.size() < sequence.size()) {
    return false;
  }

  // Verificar si los últimos N eventos coinciden con el patrón
  size_t patternSize = sequence.size();
  if (events.size() < patternSize) {
    return false;
  }

  // Comparar los últimos eventos con el patrón
  size_t startIdx = events.size() - patternSize;
  for (size_t i = 0; i < patternSize; i++) {
    const auto& patternEvent = sequence[i];
    const auto& actualEvent = events[startIdx + i];

    // Verificar si el evento actual cumple el patrón
    if (!evaluateCondition(actualEvent, patternEvent)) {
      return false;
    }
  }

  return true;
}

void CEPProcessor::executeActions(const std::string& ruleId, const PatternMatch& match) {
  std::lock_guard<std::mutex> rulesLock(rulesMutex_);
  
  auto it = rules_.find(ruleId);
  if (it == rules_.end()) {
    return;
  }

  const auto& actions = it->second.actions;
  
  // Ejecutar acciones (por ahora solo logging)
  // TODO: Implementar acciones más complejas (webhooks, notificaciones, etc.)
  if (actions.is_object() && actions.contains("log")) {
    Logger::info(LogCategory::SYSTEM, "CEPProcessor",
                 "Pattern matched for rule: " + ruleId + 
                 " - " + actions["log"].get<std::string>());
  }

  if (actions.is_object() && actions.contains("alert")) {
    Logger::warning(LogCategory::SYSTEM, "CEPProcessor",
                    "Alert triggered for rule: " + ruleId);
  }
}

std::string CEPProcessor::generateMatchId() const {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()).count();
  
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 9999);
  
  std::stringstream ss;
  ss << "match_" << time << "_" << std::setfill('0') << std::setw(4) << dis(gen);
  return ss.str();
}
