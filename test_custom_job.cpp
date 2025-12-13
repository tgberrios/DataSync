#include "catalog/custom_jobs_repository.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "sync/CustomJobExecutor.h"
#include <iostream>

int main() {
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();

    CustomJobExecutor executor(connStr);

    std::cout << "Executing test job: test_job_postgres" << std::endl;
    executor.executeJob("test_job_postgres");

    std::cout << "Job executed successfully!" << std::endl;
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
}
