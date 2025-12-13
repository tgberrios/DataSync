#include "core/database_config.h"
#include "core/logger.h"
#include "sync/APIToDatabaseSync.h"
#include <iostream>

int main() {
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    APIToDatabaseSync apiSync(connStr);

    std::cout << "Testing API sync with jsonplaceholder_users..." << std::endl;
    apiSync.syncAPIToDatabase("jsonplaceholder_users");
    std::cout << "API sync completed!" << std::endl;

    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
}
