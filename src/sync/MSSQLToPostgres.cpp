#include "sync/MSSQLToPostgres.h"
#include "engines/database_engine.h"
#include <algorithm>
#include <cctype>

std::unordered_map<std::string, std::string> MSSQLToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"bit", "BOOLEAN"},
    {"decimal", "NUMERIC"},
    {"numeric", "NUMERIC"},
    {"float", "REAL"},
    {"real", "REAL"},
    {"money", "NUMERIC(19,4)"},
    {"smallmoney", "NUMERIC(10,4)"},
    {"varchar", "VARCHAR"},
    {"nvarchar", "VARCHAR"},
    {"char", "CHAR"},
    {"nchar", "CHAR"},
    {"text", "TEXT"},
    {"ntext", "TEXT"},
    {"datetime", "TIMESTAMP"},
    {"datetime2", "TIMESTAMP"},
    {"smalldatetime", "TIMESTAMP"},
    {"date", "DATE"},
    {"time", "TIME"},
    {"datetimeoffset", "TIMESTAMP WITH TIME ZONE"},
    {"uniqueidentifier", "UUID"},
    {"varbinary", "BYTEA"},
    {"image", "BYTEA"},
    {"binary", "BYTEA"},
    {"xml", "TEXT"},
    {"sql_variant", "TEXT"}};

std::unordered_map<std::string, std::string> MSSQLToPostgres::collationMap = {
    {"SQL_Latin1_General_CP1_CI_AS", "en_US.utf8"},
    {"Latin1_General_CI_AS", "en_US.utf8"},
    {"SQL_Latin1_General_CP1_CS_AS", "C"},
    {"Latin1_General_CS_AS", "C"}};

std::string
MSSQLToPostgres::cleanValueForPostgres(const std::string &value,
                                       const std::string &columnType) {
  std::string cleanValue = value;
  std::string upperType = columnType;
  std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                 ::toupper);

  bool isNull =
      (cleanValue.empty() || cleanValue == "NULL" || cleanValue == "null" ||
       cleanValue == "\\N" || cleanValue == "\\0" || cleanValue == "0" ||
       cleanValue.find("0000-") != std::string::npos ||
       cleanValue.find("1900-01-01") != std::string::npos ||
       cleanValue.find("1970-01-01") != std::string::npos);

  for (char &c : cleanValue) {
    if (static_cast<unsigned char>(c) > 127 || c < 32) {
      isNull = true;
      break;
    }
  }

  if (upperType.find("TIMESTAMP") != std::string::npos ||
      upperType.find("DATETIME") != std::string::npos ||
      upperType.find("DATE") != std::string::npos) {
    if (cleanValue.length() < 10 ||
        cleanValue.find("-") == std::string::npos ||
        cleanValue.find("0000") != std::string::npos) {
      isNull = true;
    } else {
      if (cleanValue.find("-00") != std::string::npos ||
          cleanValue.find("-00 ") != std::string::npos ||
          cleanValue.find(" 00:00:00") != std::string::npos) {
        Logger::warning(
            LogCategory::TRANSFER, "cleanValueForPostgres",
            "Invalid date detected (contains -00), converting to NULL: " +
                cleanValue);
        isNull = true;
      }
    }
  }

  if (isNull) {
    if (upperType.find("INTEGER") != std::string::npos ||
        upperType.find("BIGINT") != std::string::npos ||
        upperType.find("SMALLINT") != std::string::npos) {
      return "0";
    } else if (upperType.find("REAL") != std::string::npos ||
               upperType.find("FLOAT") != std::string::npos ||
               upperType.find("DOUBLE") != std::string::npos ||
               upperType.find("NUMERIC") != std::string::npos) {
      return "0.0";
    } else if (upperType.find("VARCHAR") != std::string::npos ||
               upperType.find("TEXT") != std::string::npos ||
               upperType.find("CHAR") != std::string::npos) {
      return "DEFAULT";
    } else if (upperType.find("TIMESTAMP") != std::string::npos ||
               upperType.find("DATETIME") != std::string::npos) {
      return "1970-01-01 00:00:00";
    } else if (upperType.find("DATE") != std::string::npos) {
      return "1970-01-01";
    } else if (upperType.find("TIME") != std::string::npos) {
      return "00:00:00";
    } else if (upperType.find("BOOLEAN") != std::string::npos ||
               upperType.find("BOOL") != std::string::npos) {
      return "false";
    } else {
      return "DEFAULT";
    }
  }

  cleanValue.erase(std::remove_if(cleanValue.begin(), cleanValue.end(),
                                  [](unsigned char c) {
                                    return c < 32 && c != 9 && c != 10 &&
                                           c != 13;
                                  }),
                   cleanValue.end());

  if (upperType.find("BOOLEAN") != std::string::npos ||
      upperType.find("BOOL") != std::string::npos) {
    if (cleanValue == "N" || cleanValue == "0" || cleanValue == "false" ||
        cleanValue == "FALSE") {
      cleanValue = "false";
    } else if (cleanValue == "Y" || cleanValue == "1" ||
               cleanValue == "true" || cleanValue == "TRUE") {
      cleanValue = "true";
    }
  } else if (upperType.find("BIT") != std::string::npos) {
    if (cleanValue == "0" || cleanValue == "false" || cleanValue == "FALSE") {
      cleanValue = "false";
    } else if (cleanValue == "1" || cleanValue == "true" ||
               cleanValue == "TRUE") {
      cleanValue = "true";
    }
  }

  return cleanValue;
}

