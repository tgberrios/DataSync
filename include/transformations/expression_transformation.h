#ifndef EXPRESSION_TRANSFORMATION_H
#define EXPRESSION_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>
#include <map>

// Expression transformation: Evaluate expressions on columns
class ExpressionTransformation : public Transformation {
public:
  ExpressionTransformation();
  ~ExpressionTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "expression"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Evaluate a single expression on a row
  json evaluateExpression(const json& row, const json& expressionConfig);
  
  // Evaluate mathematical expression
  double evaluateMathExpression(const std::string& expression, const json& row);
  
  // Evaluate string expression
  std::string evaluateStringExpression(const std::string& expression, const json& row);
  
  // Evaluate date expression
  std::string evaluateDateExpression(const std::string& expression, const json& row);
  
  // Get column value from row
  json getColumnValue(const json& row, const std::string& columnPath);
  
  // Replace column references in expression
  std::string replaceColumnReferences(const std::string& expression, const json& row);
  
  // Simple expression parser (supports basic math, string, date functions)
  double evaluateSimpleMath(const std::string& expr, const std::map<std::string, double>& variables);
  
  // String functions
  std::string upperCase(const std::string& str);
  std::string lowerCase(const std::string& str);
  std::string trim(const std::string& str);
  std::string substring(const std::string& str, int start, int length);
  std::string concat(const std::vector<std::string>& strings, const std::string& separator);
  
  // Date functions
  std::string dateAdd(const std::string& dateStr, int days);
  int dateDiff(const std::string& date1, const std::string& date2);
  int datePart(const std::string& dateStr, const std::string& part);
};

#endif // EXPRESSION_TRANSFORMATION_H
