#include "sync/CustomJobExecutor.h"
#include "core/database_config.h"
#include <iostream>

int main() {
    try {
        std::string connStr = DatabaseConfig::getPostgresConnectionString();
        CustomJobExecutor executor(connStr);
        std::cout << "Executing job: test_job_postgres" << std::endl;
        executor.executeJob("test_job_postgres");
        std::cout << "Job completed successfully!" << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
