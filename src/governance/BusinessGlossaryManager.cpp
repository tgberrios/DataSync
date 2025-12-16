#include "governance/BusinessGlossaryManager.h"
#include "core/logger.h"
#include <algorithm>
#include <pqxx/pqxx>
#include <sstream>

BusinessGlossaryManager::BusinessGlossaryManager(
    const std::string &connectionString)
    : connectionString_(connectionString) {}

std::vector<std::string>
BusinessGlossaryManager::parseRelatedTables(const std::string &relatedTables) {
  std::vector<std::string> tables;
  if (relatedTables.empty()) {
    return tables;
  }

  std::istringstream ss(relatedTables);
  std::string table;
  while (std::getline(ss, table, ',')) {
    table.erase(0, table.find_first_not_of(" \t"));
    table.erase(table.find_last_not_of(" \t") + 1);
    if (!table.empty()) {
      tables.push_back(table);
    }
  }
  return tables;
}

std::vector<std::string>
BusinessGlossaryManager::parseTags(const std::string &tags) {
  return parseRelatedTables(tags);
}

bool BusinessGlossaryManager::addTerm(const GlossaryTerm &term) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.business_glossary (
        term, definition, category, business_domain, owner, steward,
        related_tables, tags
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (term) DO UPDATE SET
        definition = EXCLUDED.definition,
        category = EXCLUDED.category,
        business_domain = EXCLUDED.business_domain,
        owner = EXCLUDED.owner,
        steward = EXCLUDED.steward,
        related_tables = EXCLUDED.related_tables,
        tags = EXCLUDED.tags,
        updated_at = NOW()
    )";

    txn.exec_params(query, term.term, term.definition, term.category,
                    term.business_domain, term.owner, term.steward,
                    term.related_tables, term.tags);

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                 "Added/updated glossary term: " + term.term);

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error adding term: " + std::string(e.what()));
    return false;
  }
}

bool BusinessGlossaryManager::updateTerm(int termId, const GlossaryTerm &term) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.business_glossary
      SET term = $1,
          definition = $2,
          category = $3,
          business_domain = $4,
          owner = $5,
          steward = $6,
          related_tables = $7,
          tags = $8,
          updated_at = NOW()
      WHERE id = $9
    )";

    txn.exec_params(query, term.term, term.definition, term.category,
                    term.business_domain, term.owner, term.steward,
                    term.related_tables, term.tags, std::to_string(termId));

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                 "Updated glossary term ID: " + std::to_string(termId));

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error updating term: " + std::string(e.what()));
    return false;
  }
}

bool BusinessGlossaryManager::deleteTerm(int termId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = "DELETE FROM metadata.business_glossary WHERE id = $1";
    txn.exec_params(query, std::to_string(termId));

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                 "Deleted glossary term ID: " + std::to_string(termId));

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error deleting term: " + std::string(e.what()));
    return false;
  }
}

GlossaryTerm BusinessGlossaryManager::getTerm(const std::string &termName) {
  GlossaryTerm term;
  term.id = -1;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, term, definition, category, business_domain, owner, steward,
             related_tables, tags, created_at, updated_at
      FROM metadata.business_glossary
      WHERE term = $1
    )";

    auto result = txn.exec_params(query, termName);

    if (!result.empty()) {
      term.id = result[0][0].as<int>();
      term.term = result[0][1].as<std::string>();
      term.definition = result[0][2].as<std::string>();
      term.category =
          result[0][3].is_null() ? "" : result[0][3].as<std::string>();
      term.business_domain =
          result[0][4].is_null() ? "" : result[0][4].as<std::string>();
      term.owner = result[0][5].is_null() ? "" : result[0][5].as<std::string>();
      term.steward =
          result[0][6].is_null() ? "" : result[0][6].as<std::string>();
      term.related_tables =
          result[0][7].is_null() ? "" : result[0][7].as<std::string>();
      term.tags = result[0][8].is_null() ? "" : result[0][8].as<std::string>();
      term.created_at =
          result[0][9].is_null() ? "" : result[0][9].as<std::string>();
      term.updated_at =
          result[0][10].is_null() ? "" : result[0][10].as<std::string>();
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error getting term: " + std::string(e.what()));
  }

  return term;
}

std::vector<GlossaryTerm> BusinessGlossaryManager::getAllTerms() {
  std::vector<GlossaryTerm> terms;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, term, definition, category, business_domain, owner, steward,
             related_tables, tags, created_at, updated_at
      FROM metadata.business_glossary
      ORDER BY term ASC
    )";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      GlossaryTerm term;
      term.id = row[0].as<int>();
      term.term = row[1].as<std::string>();
      term.definition = row[2].as<std::string>();
      term.category = row[3].is_null() ? "" : row[3].as<std::string>();
      term.business_domain = row[4].is_null() ? "" : row[4].as<std::string>();
      term.owner = row[5].is_null() ? "" : row[5].as<std::string>();
      term.steward = row[6].is_null() ? "" : row[6].as<std::string>();
      term.related_tables = row[7].is_null() ? "" : row[7].as<std::string>();
      term.tags = row[8].is_null() ? "" : row[8].as<std::string>();
      term.created_at = row[9].is_null() ? "" : row[9].as<std::string>();
      term.updated_at = row[10].is_null() ? "" : row[10].as<std::string>();

      terms.push_back(term);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error getting all terms: " + std::string(e.what()));
  }

  return terms;
}

std::vector<GlossaryTerm>
BusinessGlossaryManager::getTermsByDomain(const std::string &domain) {
  std::vector<GlossaryTerm> terms;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, term, definition, category, business_domain, owner, steward,
             related_tables, tags, created_at, updated_at
      FROM metadata.business_glossary
      WHERE business_domain = $1
      ORDER BY term ASC
    )";

    auto result = txn.exec_params(query, domain);

    for (const auto &row : result) {
      GlossaryTerm term;
      term.id = row[0].as<int>();
      term.term = row[1].as<std::string>();
      term.definition = row[2].as<std::string>();
      term.category = row[3].is_null() ? "" : row[3].as<std::string>();
      term.business_domain = row[4].is_null() ? "" : row[4].as<std::string>();
      term.owner = row[5].is_null() ? "" : row[5].as<std::string>();
      term.steward = row[6].is_null() ? "" : row[6].as<std::string>();
      term.related_tables = row[7].is_null() ? "" : row[7].as<std::string>();
      term.tags = row[8].is_null() ? "" : row[8].as<std::string>();
      term.created_at = row[9].is_null() ? "" : row[9].as<std::string>();
      term.updated_at = row[10].is_null() ? "" : row[10].as<std::string>();

      terms.push_back(term);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error getting terms by domain: " + std::string(e.what()));
  }

  return terms;
}

std::vector<GlossaryTerm>
BusinessGlossaryManager::searchTerms(const std::string &searchQuery) {
  std::vector<GlossaryTerm> terms;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, term, definition, category, business_domain, owner, steward,
             related_tables, tags, created_at, updated_at
      FROM metadata.business_glossary
      WHERE term ILIKE $1 OR definition ILIKE $1 OR tags ILIKE $1
      ORDER BY term ASC
    )";

    std::string searchPattern = "%" + searchQuery + "%";
    auto result = txn.exec_params(query, searchPattern);

    for (const auto &row : result) {
      GlossaryTerm term;
      term.id = row[0].as<int>();
      term.term = row[1].as<std::string>();
      term.definition = row[2].as<std::string>();
      term.category = row[3].is_null() ? "" : row[3].as<std::string>();
      term.business_domain = row[4].is_null() ? "" : row[4].as<std::string>();
      term.owner = row[5].is_null() ? "" : row[5].as<std::string>();
      term.steward = row[6].is_null() ? "" : row[6].as<std::string>();
      term.related_tables = row[7].is_null() ? "" : row[7].as<std::string>();
      term.tags = row[8].is_null() ? "" : row[8].as<std::string>();
      term.created_at = row[9].is_null() ? "" : row[9].as<std::string>();
      term.updated_at = row[10].is_null() ? "" : row[10].as<std::string>();

      terms.push_back(term);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error searching terms: " + std::string(e.what()));
  }

  return terms;
}

bool BusinessGlossaryManager::addDictionaryEntry(
    const DataDictionaryEntry &entry) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.data_dictionary (
        schema_name, table_name, column_name, business_description,
        business_name, data_type_business, business_rules, examples,
        glossary_term, owner, steward
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      ON CONFLICT (schema_name, table_name, column_name) DO UPDATE SET
        business_description = EXCLUDED.business_description,
        business_name = EXCLUDED.business_name,
        data_type_business = EXCLUDED.data_type_business,
        business_rules = EXCLUDED.business_rules,
        examples = EXCLUDED.examples,
        glossary_term = EXCLUDED.glossary_term,
        owner = EXCLUDED.owner,
        steward = EXCLUDED.steward,
        updated_at = NOW()
    )";

    txn.exec_params(query, entry.schema_name, entry.table_name,
                    entry.column_name, entry.business_description,
                    entry.business_name, entry.data_type_business,
                    entry.business_rules, entry.examples, entry.glossary_term,
                    entry.owner, entry.steward);

    txn.commit();

    std::string updateQuery = R"(
      UPDATE metadata.data_governance_catalog
      SET business_glossary_term = $1,
          data_dictionary_description = $2
      WHERE schema_name = $3 AND table_name = $4
    )";

    pqxx::work updateTxn(conn);
    updateTxn.exec_params(updateQuery, entry.glossary_term,
                          entry.business_description, entry.schema_name,
                          entry.table_name);
    updateTxn.commit();

    Logger::info(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                 "Added dictionary entry for " + entry.schema_name + "." +
                     entry.table_name + "." + entry.column_name);

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error adding dictionary entry: " + std::string(e.what()));
    return false;
  }
}

bool BusinessGlossaryManager::updateDictionaryEntry(
    const std::string &schemaName, const std::string &tableName,
    const std::string &columnName, const DataDictionaryEntry &entry) {
  return addDictionaryEntry(entry);
}

DataDictionaryEntry
BusinessGlossaryManager::getDictionaryEntry(const std::string &schemaName,
                                            const std::string &tableName,
                                            const std::string &columnName) {
  DataDictionaryEntry entry;
  entry.schema_name = schemaName;
  entry.table_name = tableName;
  entry.column_name = columnName;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT business_description, business_name, data_type_business,
             business_rules, examples, glossary_term, owner, steward
      FROM metadata.data_dictionary
      WHERE schema_name = $1 AND table_name = $2 AND column_name = $3
    )";

    auto result = txn.exec_params(query, schemaName, tableName, columnName);

    if (!result.empty()) {
      entry.business_description =
          result[0][0].is_null() ? "" : result[0][0].as<std::string>();
      entry.business_name =
          result[0][1].is_null() ? "" : result[0][1].as<std::string>();
      entry.data_type_business =
          result[0][2].is_null() ? "" : result[0][2].as<std::string>();
      entry.business_rules =
          result[0][3].is_null() ? "" : result[0][3].as<std::string>();
      entry.examples =
          result[0][4].is_null() ? "" : result[0][4].as<std::string>();
      entry.glossary_term =
          result[0][5].is_null() ? "" : result[0][5].as<std::string>();
      entry.owner =
          result[0][6].is_null() ? "" : result[0][6].as<std::string>();
      entry.steward =
          result[0][7].is_null() ? "" : result[0][7].as<std::string>();
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error getting dictionary entry: " + std::string(e.what()));
  }

  return entry;
}

std::vector<DataDictionaryEntry>
BusinessGlossaryManager::getDictionaryForTable(const std::string &schemaName,
                                               const std::string &tableName) {
  std::vector<DataDictionaryEntry> entries;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT column_name, business_description, business_name, data_type_business,
             business_rules, examples, glossary_term, owner, steward
      FROM metadata.data_dictionary
      WHERE schema_name = $1 AND table_name = $2
      ORDER BY column_name ASC
    )";

    auto result = txn.exec_params(query, schemaName, tableName);

    for (const auto &row : result) {
      DataDictionaryEntry entry;
      entry.schema_name = schemaName;
      entry.table_name = tableName;
      entry.column_name = row[0].as<std::string>();
      entry.business_description =
          row[1].is_null() ? "" : row[1].as<std::string>();
      entry.business_name = row[2].is_null() ? "" : row[2].as<std::string>();
      entry.data_type_business =
          row[3].is_null() ? "" : row[3].as<std::string>();
      entry.business_rules = row[4].is_null() ? "" : row[4].as<std::string>();
      entry.examples = row[5].is_null() ? "" : row[5].as<std::string>();
      entry.glossary_term = row[6].is_null() ? "" : row[6].as<std::string>();
      entry.owner = row[7].is_null() ? "" : row[7].as<std::string>();
      entry.steward = row[8].is_null() ? "" : row[8].as<std::string>();

      entries.push_back(entry);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error getting dictionary for table: " +
                      std::string(e.what()));
  }

  return entries;
}

std::vector<DataDictionaryEntry>
BusinessGlossaryManager::searchDictionary(const std::string &searchQuery) {
  std::vector<DataDictionaryEntry> entries;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT schema_name, table_name, column_name, business_description,
             business_name, data_type_business, business_rules, examples,
             glossary_term, owner, steward
      FROM metadata.data_dictionary
      WHERE business_description ILIKE $1 OR business_name ILIKE $1 OR
            business_rules ILIKE $1 OR glossary_term ILIKE $1
      ORDER BY schema_name, table_name, column_name
    )";

    std::string searchPattern = "%" + searchQuery + "%";
    auto result = txn.exec_params(query, searchPattern);

    for (const auto &row : result) {
      DataDictionaryEntry entry;
      entry.schema_name = row[0].as<std::string>();
      entry.table_name = row[1].as<std::string>();
      entry.column_name = row[2].as<std::string>();
      entry.business_description =
          row[3].is_null() ? "" : row[3].as<std::string>();
      entry.business_name = row[4].is_null() ? "" : row[4].as<std::string>();
      entry.data_type_business =
          row[5].is_null() ? "" : row[5].as<std::string>();
      entry.business_rules = row[6].is_null() ? "" : row[6].as<std::string>();
      entry.examples = row[7].is_null() ? "" : row[7].as<std::string>();
      entry.glossary_term = row[8].is_null() ? "" : row[8].as<std::string>();
      entry.owner = row[9].is_null() ? "" : row[9].as<std::string>();
      entry.steward = row[10].is_null() ? "" : row[10].as<std::string>();

      entries.push_back(entry);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error searching dictionary: " + std::string(e.what()));
  }

  return entries;
}

bool BusinessGlossaryManager::linkTermToTable(const std::string &termName,
                                              const std::string &schemaName,
                                              const std::string &tableName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string selectQuery = R"(
      SELECT related_tables
      FROM metadata.business_glossary
      WHERE term = $1
    )";

    auto result = txn.exec_params(selectQuery, termName);
    std::string relatedTables = "";

    if (!result.empty() && !result[0][0].is_null()) {
      relatedTables = result[0][0].as<std::string>();
    }

    std::string tableRef = schemaName + "." + tableName;
    if (relatedTables.find(tableRef) == std::string::npos) {
      if (!relatedTables.empty()) {
        relatedTables += "," + tableRef;
      } else {
        relatedTables = tableRef;
      }

      std::string updateQuery = R"(
        UPDATE metadata.business_glossary
        SET related_tables = $1, updated_at = NOW()
        WHERE term = $2
      )";

      txn.exec_params(updateQuery, relatedTables, termName);
    }

    std::string updateCatalogQuery = R"(
      UPDATE metadata.data_governance_catalog
      SET business_glossary_term = $1
      WHERE schema_name = $2 AND table_name = $3
    )";

    txn.exec_params(updateCatalogQuery, termName, schemaName, tableName);

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                 "Linked term " + termName + " to table " + schemaName + "." +
                     tableName);

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error linking term to table: " + std::string(e.what()));
    return false;
  }
}

std::vector<std::string>
BusinessGlossaryManager::getTablesForTerm(const std::string &termName) {
  std::vector<std::string> tables;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT related_tables
      FROM metadata.business_glossary
      WHERE term = $1
    )";

    auto result = txn.exec_params(query, termName);

    if (!result.empty() && !result[0][0].is_null()) {
      std::string relatedTables = result[0][0].as<std::string>();
      tables = parseRelatedTables(relatedTables);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error getting tables for term: " + std::string(e.what()));
  }

  return tables;
}

std::vector<std::string>
BusinessGlossaryManager::getTermsForTable(const std::string &schemaName,
                                          const std::string &tableName) {
  std::vector<std::string> terms;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT business_glossary_term
      FROM metadata.data_governance_catalog
      WHERE schema_name = $1 AND table_name = $2
        AND business_glossary_term IS NOT NULL
    )";

    auto result = txn.exec_params(query, schemaName, tableName);

    for (const auto &row : result) {
      if (!row[0].is_null()) {
        std::string term = row[0].as<std::string>();
        if (!term.empty()) {
          terms.push_back(term);
        }
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "BusinessGlossaryManager",
                  "Error getting terms for table: " + std::string(e.what()));
  }

  return terms;
}
