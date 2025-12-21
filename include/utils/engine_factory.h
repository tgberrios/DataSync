#ifndef ENGINE_FACTORY_H
#define ENGINE_FACTORY_H

#include "engines/database_engine.h"
#include <memory>
#include <string>

namespace EngineFactory {
// Creates a database engine instance based on the engine name and connection
// string. Returns nullptr if the engine type is unsupported or if creation
// fails. This factory function centralizes engine creation logic and eliminates
// code duplication across the codebase.
std::unique_ptr<IDatabaseEngine>
createEngine(const std::string &dbEngine, const std::string &connectionString);
} // namespace EngineFactory

#endif
