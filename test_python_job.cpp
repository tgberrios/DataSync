#include "core/database_config.h"
#include "sync/CustomJobExecutor.h"
#include <iostream>

int main() {
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    CustomJobExecutor executor(connStr);

    std::cout << "Executing test_python_simple job..." << std::endl;
    executor.executeJob("test_python_simple");
    std::cout << "Job executed successfully!" << std::endl;

    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
}
