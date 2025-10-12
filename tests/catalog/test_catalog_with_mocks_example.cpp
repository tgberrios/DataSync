#include "catalog/catalog_manager.h"
#include <iostream>

class MockMetadataRepository : public IMetadataRepository {
public:
  std::vector<std::string>
  getConnectionStrings(const std::string &dbEngine) override {
    std::cout << "MockMetadataRepository::getConnectionStrings called with: "
              << dbEngine << std::endl;
    return {"mock_connection_string_1", "mock_connection_string_2"};
  }

  std::vector<CatalogEntry>
  getCatalogEntries(const std::string &dbEngine,
                    const std::string &connectionString) override {
    std::cout << "MockMetadataRepository::getCatalogEntries called"
              << std::endl;
    return {};
  }

  void insertOrUpdateTable(const CatalogTableInfo &tableInfo,
                           const std::string &timeColumn,
                           const std::vector<std::string> &pkColumns,
                           bool hasPK, int64_t tableSize,
                           const std::string &dbEngine) override {
    std::cout << "MockMetadataRepository::insertOrUpdateTable called for: "
              << tableInfo.schema << "." << tableInfo.table << std::endl;
  }

  void updateClusterName(const std::string &clusterName,
                         const std::string &connectionString,
                         const std::string &dbEngine) override {
    std::cout << "MockMetadataRepository::updateClusterName called"
              << std::endl;
  }

  void deleteTable(const std::string &schema, const std::string &table,
                   const std::string &dbEngine,
                   const std::string &connectionString = "") override {
    std::cout << "MockMetadataRepository::deleteTable called for: " << schema
              << "." << table << std::endl;
  }

  int deactivateNoDataTables() override {
    std::cout << "MockMetadataRepository::deactivateNoDataTables called"
              << std::endl;
    return 5;
  }

  int markInactiveTablesAsSkip() override {
    std::cout << "MockMetadataRepository::markInactiveTablesAsSkip called"
              << std::endl;
    return 3;
  }

  int resetTable(const std::string &schema, const std::string &table,
                 const std::string &dbEngine) override {
    std::cout << "MockMetadataRepository::resetTable called" << std::endl;
    return 1;
  }

  int cleanInvalidOffsets() override {
    std::cout << "MockMetadataRepository::cleanInvalidOffsets called"
              << std::endl;
    return 2;
  }

  std::unordered_map<std::string, int64_t> getTableSizesBatch() override {
    std::cout << "MockMetadataRepository::getTableSizesBatch called"
              << std::endl;
    return {{"schema1|table1", 1000}, {"schema2|table2", 2000}};
  }
};

class MockCatalogCleaner : public ICatalogCleaner {
public:
  void cleanNonExistentPostgresTables() override {
    std::cout << "MockCatalogCleaner::cleanNonExistentPostgresTables called"
              << std::endl;
  }

  void cleanNonExistentMariaDBTables() override {
    std::cout << "MockCatalogCleaner::cleanNonExistentMariaDBTables called"
              << std::endl;
  }

  void cleanNonExistentMSSQLTables() override {
    std::cout << "MockCatalogCleaner::cleanNonExistentMSSQLTables called"
              << std::endl;
  }

  void cleanOrphanedTables() override {
    std::cout << "MockCatalogCleaner::cleanOrphanedTables called" << std::endl;
  }

  void cleanOldLogs(int retentionHours) override {
    std::cout << "MockCatalogCleaner::cleanOldLogs called with: "
              << retentionHours << " hours" << std::endl;
  }
};

int main() {
  std::cout << "=== Testing CatalogManager with Mock Dependencies ==="
            << std::endl;
  std::cout << std::endl;

  auto mockRepo = std::make_unique<MockMetadataRepository>();
  auto mockCleaner = std::make_unique<MockCatalogCleaner>();

  CatalogManager manager("mock_connection_string", std::move(mockRepo),
                         std::move(mockCleaner));

  std::cout << "\n--- Testing cleanCatalog() ---" << std::endl;
  manager.cleanCatalog();

  std::cout << "\n--- Testing deactivateNoDataTables() ---" << std::endl;
  manager.deactivateNoDataTables();

  std::cout << "\n=== All Tests Completed Successfully ===" << std::endl;

  return 0;
}
