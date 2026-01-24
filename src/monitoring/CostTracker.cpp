#include "monitoring/CostTracker.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <sstream>
#include <ctime>

CostTracker::CostTracker(const std::string& connectionString)
    : connectionString_(connectionString) {
  resourceTracker_ = std::make_unique<ResourceTracker>(connectionString);

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.cost_tracking ("
        "id SERIAL PRIMARY KEY,"
        "cost_id VARCHAR(255) UNIQUE NOT NULL,"
        "workflow_id VARCHAR(255),"
        "operation_name VARCHAR(255),"
        "compute_cost DECIMAL(10,4),"
        "storage_cost DECIMAL(10,4),"
        "network_cost DECIMAL(10,4),"
        "total_cost DECIMAL(10,4),"
        "currency VARCHAR(10) DEFAULT 'USD',"
        "breakdown JSONB DEFAULT '{}'::jsonb,"
        "timestamp TIMESTAMP DEFAULT NOW()"
        ")");

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.cost_budgets ("
        "id SERIAL PRIMARY KEY,"
        "budget_id VARCHAR(255) UNIQUE NOT NULL,"
        "name VARCHAR(255) NOT NULL,"
        "scope VARCHAR(50) NOT NULL,"
        "scope_id VARCHAR(255),"
        "amount DECIMAL(10,2) NOT NULL,"
        "period VARCHAR(20) NOT NULL,"
        "current_spend DECIMAL(10,2) DEFAULT 0,"
        "period_start TIMESTAMP DEFAULT NOW(),"
        "alert_on_exceed BOOLEAN DEFAULT true,"
        "alert_threshold DECIMAL(5,2) DEFAULT 80.0"
        ")");

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.cost_estimates ("
        "id SERIAL PRIMARY KEY,"
        "resource_type VARCHAR(50) NOT NULL,"
        "estimated_cost DECIMAL(10,4),"
        "currency VARCHAR(10) DEFAULT 'USD',"
        "period VARCHAR(20),"
        "details JSONB DEFAULT '{}'::jsonb,"
        "estimated_at TIMESTAMP DEFAULT NOW()"
        ")");

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "CostTracker",
                  "Error creating tables: " + std::string(e.what()));
  }
}

CostTracker::CostRecord CostTracker::calculateOperationCost(
    const std::string& workflowId, const std::string& operationName,
    const ResourceTracker::ResourceMetrics& metrics) {
  CostRecord cost;
  cost.id = "cost_" + std::to_string(std::time(nullptr)) + "_" + workflowId;
  cost.workflowId = workflowId;
  cost.operationName = operationName;
  cost.computeCost = calculateComputeCost(metrics);
  cost.storageCost = calculateStorageCost(metrics);
  cost.networkCost = calculateNetworkCost(metrics);
  cost.totalCost = cost.computeCost + cost.storageCost + cost.networkCost;
  cost.currency = "USD";
  cost.timestamp = std::chrono::system_clock::now();

  cost.breakdown = json{{"compute", cost.computeCost},
                         {"storage", cost.storageCost},
                         {"network", cost.networkCost}};

  return cost;
}

double CostTracker::calculateComputeCost(const ResourceTracker::ResourceMetrics& metrics) {
  // Simplified: $0.10 per CPU-hour
  double cpuHours = (metrics.cpuPercent / 100.0) * (1.0 / 3600.0); // Approximate
  return cpuHours * 0.10;
}

double CostTracker::calculateStorageCost(const ResourceTracker::ResourceMetrics& metrics) {
  // Simplified: $0.023 per GB-month
  double gbMonths = (metrics.memoryUsedMB / 1024.0) * (1.0 / (30.0 * 24.0 * 3600.0));
  return gbMonths * 0.023;
}

double CostTracker::calculateNetworkCost(const ResourceTracker::ResourceMetrics& metrics) {
  // Simplified: $0.09 per GB
  double gb = (metrics.networkBytesIn + metrics.networkBytesOut) / (1024.0 * 1024.0 * 1024.0);
  return gb * 0.09;
}

bool CostTracker::saveCost(const CostRecord& cost) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.cost_tracking "
        "(cost_id, workflow_id, operation_name, compute_cost, storage_cost, network_cost, "
        "total_cost, currency, breakdown) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        cost.id, cost.workflowId.empty() ? nullptr : cost.workflowId,
        cost.operationName.empty() ? nullptr : cost.operationName, cost.computeCost,
        cost.storageCost, cost.networkCost, cost.totalCost, cost.currency, cost.breakdown.dump());

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "CostTracker",
                  "Error saving cost: " + std::string(e.what()));
    return false;
  }
}

json CostTracker::getCostSummary(const std::string& workflowId, int days) {
  json summary;
  // TODO: Implement
  return summary;
}

std::vector<CostTracker::CostRecord> CostTracker::getCostsByWorkflow(const std::string& workflowId,
                                                                       int days) {
  std::vector<CostRecord> costs;
  // TODO: Implement
  return costs;
}

bool CostTracker::setBudget(const Budget& budget) {
  return saveBudgetToDatabase(budget);
}

bool CostTracker::saveBudgetToDatabase(const Budget& budget) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.cost_budgets "
        "(budget_id, name, scope, scope_id, amount, period, current_spend, alert_on_exceed, "
        "alert_threshold) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
        "ON CONFLICT (budget_id) DO UPDATE SET "
        "name = EXCLUDED.name, amount = EXCLUDED.amount, period = EXCLUDED.period, "
        "alert_on_exceed = EXCLUDED.alert_on_exceed, alert_threshold = EXCLUDED.alert_threshold",
        budget.id, budget.name, budget.scope,
        budget.scopeId.empty() ? nullptr : budget.scopeId, budget.amount, budget.period,
        budget.currentSpend, budget.alertOnExceed, budget.alertThreshold);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "CostTracker",
                  "Error saving budget: " + std::string(e.what()));
    return false;
  }
}

std::vector<CostTracker::Budget> CostTracker::getBudgets() {
  std::vector<Budget> budgets;
  // TODO: Implement
  return budgets;
}

bool CostTracker::exceedsBudget(const std::string& budgetId) {
  // TODO: Implement
  return false;
}

std::vector<CostTracker::CostEstimate> CostTracker::estimateCloudCosts() {
  std::vector<CostEstimate> estimates;
  // TODO: Implement
  return estimates;
}

bool CostTracker::saveEstimateToDatabase(const CostEstimate& estimate) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.cost_estimates (resource_type, estimated_cost, currency, period, details) "
        "VALUES ($1, $2, $3, $4, $5)",
        estimate.resourceType, estimate.estimatedCost, estimate.currency, estimate.period,
        estimate.details.dump());

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "CostTracker",
                  "Error saving estimate: " + std::string(e.what()));
    return false;
  }
}
