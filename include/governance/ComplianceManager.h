#ifndef COMPLIANCE_MANAGER_H
#define COMPLIANCE_MANAGER_H

#include <pqxx/pqxx>
#include <string>
#include <vector>

struct DataSubjectRequest {
  std::string request_id;
  std::string request_type;
  std::string data_subject_email;
  std::string data_subject_name;
  std::string request_status;
  std::string requested_data;
  std::string response_data;
  std::string processed_by;
  std::string compliance_requirement;
};

struct ConsentRecord {
  std::string schema_name;
  std::string table_name;
  std::string data_subject_id;
  std::string consent_type;
  std::string consent_status;
  std::string legal_basis;
  std::string purpose;
  std::string retention_period;
};

class ComplianceManager {
private:
  std::string connectionString_;

  std::string generateRequestId();
  bool validateRequest(const DataSubjectRequest &request);
  std::vector<std::string> findDataLocations(const std::string &email,
                                             const std::string &name);
  std::string exportDataForSubject(const std::string &email,
                                   const std::string &name);
  bool deleteDataForSubject(const std::string &email, const std::string &name);

public:
  explicit ComplianceManager(const std::string &connectionString);
  ~ComplianceManager() = default;

  std::string createDataSubjectRequest(const DataSubjectRequest &request);
  bool processRightToBeForgotten(const std::string &requestId);
  bool processDataPortability(const std::string &requestId);
  bool processAccessRequest(const std::string &requestId);
  std::vector<DataSubjectRequest> getPendingRequests();
  bool updateRequestStatus(const std::string &requestId,
                           const std::string &status,
                           const std::string &processedBy);

  bool recordConsent(const ConsentRecord &consent);
  bool withdrawConsent(const std::string &dataSubjectId,
                       const std::string &tableName);
  std::vector<ConsentRecord>
  getConsentsForSubject(const std::string &dataSubjectId);

  bool checkBreachNotification(const std::string &schemaName,
                               const std::string &tableName);
  void logBreachCheck(const std::string &schemaName,
                      const std::string &tableName);
};

#endif
