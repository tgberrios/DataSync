#include "utils/HostnamePatternMatcher.h"
#include "utils/string_utils.h"

std::string
HostnamePatternMatcher::deriveClusterName(const std::string &hostname) {
  if (hostname.empty())
    return "";

  std::string lower = StringUtils::toLower(hostname);

  static constexpr const char *PROD_PATTERNS[] = {"prod", "production"};
  static constexpr const char *STAGING_PATTERNS[] = {"staging", "stage"};
  static constexpr const char *DEV_PATTERNS[] = {"dev", "development"};
  static constexpr const char *TEST_PATTERNS[] = {"test", "testing"};
  static constexpr const char *LOCAL_PATTERNS[] = {"local", "localhost"};

  if (matchesPattern(lower, PROD_PATTERNS, 2))
    return "PRODUCTION";
  if (matchesPattern(lower, STAGING_PATTERNS, 2))
    return "STAGING";
  if (matchesPattern(lower, DEV_PATTERNS, 2))
    return "DEVELOPMENT";
  if (matchesPattern(lower, TEST_PATTERNS, 2))
    return "TESTING";
  if (matchesPattern(lower, LOCAL_PATTERNS, 2))
    return "LOCAL";

  if (lower.find("uat") != std::string::npos)
    return "UAT";
  if (lower.find("qa") != std::string::npos)
    return "QA";

  size_t pos = lower.find("cluster");
  if (pos != std::string::npos) {
    return StringUtils::toUpper(lower.substr(pos));
  }

  pos = lower.find("db-");
  if (pos != std::string::npos) {
    return StringUtils::toUpper(lower.substr(pos));
  }

  return StringUtils::toUpper(hostname);
}

bool HostnamePatternMatcher::matchesPattern(const std::string &hostname,
                                            const char *const *patterns,
                                            size_t count) {
  for (size_t i = 0; i < count; ++i) {
    if (hostname.find(patterns[i]) != std::string::npos)
      return true;
  }
  return false;
}
