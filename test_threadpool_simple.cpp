#include "sync/TableProcessorThreadPool.h"
#include "core/Config.h"
#include "core/logger.h"
#include <chrono>
#include <iostream>
#include <thread>

using namespace std::chrono;

int main() {
  DatabaseConfig::loadFromFile("config.json");
  Logger::initialize();

  std::cout << "\n╔═══════════════════════════════════════════════════════════╗\n";
  std::cout << "║        TESTING TableProcessorThreadPool                  ║\n";
  std::cout << "╚═══════════════════════════════════════════════════════════╝\n\n";

  auto startTime = high_resolution_clock::now();

  {
    TableProcessorThreadPool pool(4);

    std::cout << "✅ Created thread pool with 4 workers\n\n";

    for (int i = 1; i <= 12; i++) {
      DatabaseToPostgresSync::TableInfo table;
      table.schema_name = "test_schema";
      table.table_name = "table_" + std::to_string(i);
      table.db_engine = "TestEngine";

      int sleepMs = (i % 3 == 0) ? 2000 : 500;

      pool.submitTask(
          table,
          [sleepMs](const DatabaseToPostgresSync::TableInfo &t) {
            std::cout << "  📦 Processing " << t.table_name
                      << " (will take " << sleepMs << "ms)\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
            std::cout << "  ✓  Completed " << t.table_name << "\n";
          });
    }

    std::cout << "\n📊 Submitted 12 tasks\n";
    std::cout << "   - 4 tasks slow (2000ms each)\n";
    std::cout << "   - 8 tasks fast (500ms each)\n\n";
    std::cout << "⏳ Waiting for completion...\n\n";

    pool.waitForCompletion();

    std::cout << "\n📈 RESULTS:\n";
    std::cout << "   - Completed: " << pool.completedTasks() << "/12\n";
    std::cout << "   - Failed:    " << pool.failedTasks() << "/12\n";
  }

  auto endTime = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(endTime - startTime);

  std::cout << "\n⏱️  Total time: " << duration.count() << "ms\n";
  std::cout << "\n╔═══════════════════════════════════════════════════════════╗\n";
  std::cout << "║  EXPECTED (with thread pool):                            ║\n";
  std::cout << "║  • ~4.5 seconds (optimal parallelization)                 ║\n";
  std::cout << "║                                                           ║\n";
  std::cout << "║  WITHOUT thread pool (sequential):                       ║\n";
  std::cout << "║  • ~12 seconds (4×2000ms + 8×500ms)                       ║\n";
  std::cout << "╚═══════════════════════════════════════════════════════════╝\n";

  if (duration.count() < 6000) {
    std::cout << "\n✅ TEST PASSED - Thread pool is working optimally!\n\n";
    return 0;
  } else {
    std::cout << "\n⚠️  TEST WARNING - Took longer than expected\n\n";
    return 1;
  }
}

