#ifndef DATABASE_DEFAULTS_H
#define DATABASE_DEFAULTS_H

namespace DatabaseDefaults {
constexpr int DEFAULT_MYSQL_PORT = 3306;
constexpr int MARIADB_TIMEOUT_SECONDS = 600;
constexpr int BUFFER_SIZE = 1024;
constexpr int DEFAULT_LOG_RETENTION_HOURS = 24;

constexpr const char *TIME_COLUMN_CANDIDATES[] = {
    "updated_at", "modified_at",  "last_modified", "updated_time",
    "created_at", "created_time", "timestamp"};
constexpr size_t TIME_COLUMN_COUNT = 7;

constexpr const char *ENV_PATTERN_PROD[] = {"prod", "production"};
constexpr const char *ENV_PATTERN_STAGING[] = {"staging", "stage"};
constexpr const char *ENV_PATTERN_DEV[] = {"dev", "development"};
constexpr const char *ENV_PATTERN_TEST[] = {"test", "testing"};
constexpr const char *ENV_PATTERN_LOCAL[] = {"local", "localhost"};
constexpr const char *ENV_PATTERN_UAT[] = {"uat"};
constexpr const char *ENV_PATTERN_QA[] = {"qa"};
constexpr const char *CLUSTER_PATTERN_CLUSTER[] = {"cluster"};
constexpr const char *CLUSTER_PATTERN_DB[] = {"db-"};
} // namespace DatabaseDefaults

#endif
