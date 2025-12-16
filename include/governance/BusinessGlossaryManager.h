#ifndef BUSINESS_GLOSSARY_MANAGER_H
#define BUSINESS_GLOSSARY_MANAGER_H

#include <pqxx/pqxx>
#include <string>
#include <vector>

struct GlossaryTerm {
  int id;
  std::string term;
  std::string definition;
  std::string category;
  std::string business_domain;
  std::string owner;
  std::string steward;
  std::string related_tables;
  std::string tags;
  std::string created_at;
  std::string updated_at;
};

struct DataDictionaryEntry {
  std::string schema_name;
  std::string table_name;
  std::string column_name;
  std::string business_description;
  std::string business_name;
  std::string data_type_business;
  std::string business_rules;
  std::string examples;
  std::string glossary_term;
  std::string owner;
  std::string steward;
};

class BusinessGlossaryManager {
private:
  std::string connectionString_;

  std::vector<std::string> parseRelatedTables(const std::string &relatedTables);
  std::vector<std::string> parseTags(const std::string &tags);

public:
  explicit BusinessGlossaryManager(const std::string &connectionString);
  ~BusinessGlossaryManager() = default;

  bool addTerm(const GlossaryTerm &term);
  bool updateTerm(int termId, const GlossaryTerm &term);
  bool deleteTerm(int termId);
  GlossaryTerm getTerm(const std::string &termName);
  std::vector<GlossaryTerm> getAllTerms();
  std::vector<GlossaryTerm> getTermsByDomain(const std::string &domain);
  std::vector<GlossaryTerm> searchTerms(const std::string &searchQuery);

  bool addDictionaryEntry(const DataDictionaryEntry &entry);
  bool updateDictionaryEntry(const std::string &schemaName,
                             const std::string &tableName,
                             const std::string &columnName,
                             const DataDictionaryEntry &entry);
  DataDictionaryEntry getDictionaryEntry(const std::string &schemaName,
                                         const std::string &tableName,
                                         const std::string &columnName);
  std::vector<DataDictionaryEntry>
  getDictionaryForTable(const std::string &schemaName,
                        const std::string &tableName);
  std::vector<DataDictionaryEntry>
  searchDictionary(const std::string &searchQuery);

  bool linkTermToTable(const std::string &termName,
                       const std::string &schemaName,
                       const std::string &tableName);
  std::vector<std::string> getTablesForTerm(const std::string &termName);
  std::vector<std::string> getTermsForTable(const std::string &schemaName,
                                            const std::string &tableName);
};

#endif
