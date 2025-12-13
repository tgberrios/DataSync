#ifndef HOSTNAME_PATTERN_MATCHER_H
#define HOSTNAME_PATTERN_MATCHER_H

#include <string>

class HostnamePatternMatcher {
public:
  static std::string deriveClusterName(const std::string &hostname);

private:
  static bool matchesPattern(const std::string &hostname,
                             const char *const *patterns, size_t count);
};

#endif
